use crate::core::common::types::{DimId, DimOffset, ElementOffsetType, Weight};
use crate::core::common::{madvise, StorageVersion};
use crate::core::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::core::inverted_index::{InvertedIndex, INDEX_FILE_NAME};
use crate::core::sparse_vector::RemappedSparseVector;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::common::file_operations::{atomic_save_json, read_json};
use crate::core::common::mmap_ops::{create_and_ensure_length, open_read_mmap, transmute_from_u8_to_slice, transmute_to_u8, transmute_to_u8_slice};
use crate::core::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use crate::core::posting_list::{CompressedPostingChunk, CompressedPostingListIterator, CompressedPostingListView, GenericPostingElement};

const INDEX_CONFIG_FILE_NAME: &str = "inverted_index_config.json";

pub struct Version;

impl StorageVersion for Version {
    fn current_raw() -> &'static str {
        "0.2.0"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InvertedIndexFileHeader {
    pub posting_count: usize, // number of posting
    pub vector_count: usize, // all unique sparse vectors
}

#[derive(Debug, Default, Clone)]
#[repr(C)] // 确保结构体在内存中的布局和 C 兼容
struct PostingListFileHeader<W: Weight> {
    pub ids_start: u64,
    pub last_id: u32,
    pub ids_len: u32,
    pub chunks_count: u32,
    pub quantization_params: W::QuantizationParams,
}

#[derive(Debug)]
pub struct InvertedIndexCompressedMmap<W> {
    path: PathBuf,
    mmap: Arc<Mmap>,
    pub file_header: InvertedIndexFileHeader,
    _phantom: PhantomData<W>,  // 零大小类型，用于在范型上下文中标记类型 W 的存在，不占用实际的内存空间，主要用于编译时的类型检查与约束
}



impl<W: Weight> InvertedIndex for InvertedIndexCompressedMmap<W> {
    type Iter<'a> = CompressedPostingListIterator<'a, W>;
    type Version = Version;

    fn open(path: &Path) -> std::io::Result<Self> {
        Self::load(path)
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        debug_assert_eq!(path, self.path);
        // If Self instance exists, it's either constructed by using `open()` (which reads core
        // files), or using `from_ram_index()` (which writes them). Both assume that the files
        // exist. If any of the files are missing, then something went wrong.
        for file in Self::files(path) {
            debug_assert!(file.exists());
        }

        Ok(())
    }

    fn get(&self, id: &DimOffset) -> Option<Self::Iter<'_>> {
        // fn get<'a>(&'a self, id: &DimOffset) -> Option<CompressedPostingListIterator<'a,W>> {
        // 返回值中的迭代器依赖于 self.get 的数据，通常编译器会使得自动推断出来的生命周期保持和 self 一致
        self.get(id).map(|posting_list| posting_list.iter())
    }

    fn len(&self) -> usize {
        self.file_header.posting_count
    }

    fn posting_list_len(&self, id: &DimOffset) -> Option<usize> {
        self.get(id).map(|posting_list| posting_list.len())
    }

    fn files(path: &Path) -> Vec<PathBuf> {
        vec![
            Self::index_file_path(path),
            Self::index_config_file_path(path),
        ]
    }

    fn remove(&mut self, id: ElementOffsetType, old_vector: RemappedSparseVector) {
        panic!("Cannot remove from a read-only mmap inverted core")
    }

    fn upsert(
        &mut self,
        id: ElementOffsetType,
        vector: RemappedSparseVector,
        old_vector: Option<RemappedSparseVector>,
    ) {
        panic!("Cannot upsert into a read-only mmap inverted core")
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        path: P,
    ) -> std::io::Result<Self> {
        let index = InvertedIndexCompressedImmutableRam::from_ram_index(ram_index, &path)?;
        Self::convert_and_save(&index, path)
    }

    fn vector_count(&self) -> usize {
        self.file_header.vector_count
    }

    fn max_index(&self) -> Option<DimOffset> {
        match self.file_header.posting_count {
            0 => None,
            len => Some(len as DimId - 1)
        }
    }
}

impl<W: Weight> InvertedIndexCompressedMmap<W> {
    pub fn index_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_FILE_NAME)
    }

    pub fn index_config_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_CONFIG_FILE_NAME)
    }

    /// 从索引中将维度 id 对应的 Posting List 给取出来
    // pub fn get<'a>(&'a self, id: &DimId) -> Option<CompressedPostingListView<'a, W>> {
    pub fn get(&self, id: &DimId) -> Option<CompressedPostingListView<W>> {
        // check that the id is not out of bounds (posting_count includes the empty zeroth entry)
        if *id >= self.file_header.posting_count as DimId {
            return None;
        }

        // 获得 PostingList 头信息
        let header: PostingListFileHeader<W> = self.slice_part::<PostingListFileHeader<W>>(
            *id as u64 * size_of::<PostingListFileHeader<W>>() as u64,
            1u32,
        )[0]
            .clone();

        // remainders 的起始位置紧跟着 ids 和 chunks 之后
        let remainders_start = header.ids_start
            + header.ids_len as u64
            + header.chunks_count as u64 * size_of::<CompressedPostingChunk<W>>() as u64;

        // 计算 remainders 的结束位置
        // 如果当前 id 不是最后一个 PostingList，那么 remainders_end 的长度就是下一个 PostingList 的 ids_start
        // 否则，remainders_end 就是整个 mmap 的长度
        let remainders_end = if *id + 1 < self.file_header.posting_count as DimId {
            self.slice_part::<PostingListFileHeader<W>>(
                (*id + 1) as u64 * size_of::<PostingListFileHeader<W>>() as u64,
                1u32,
            )[0]
                .ids_start
        } else {
            self.mmap.len() as u64
        };

        // 检查 remainders end-start 的长度是否能够被 GenericPostingElement 整除, 如果不能就返回 None
        if remainders_end
            .checked_sub(remainders_start)
            .map_or(false, |len| {
                len % size_of::<GenericPostingElement<W>>() as u64 != 0
            })
        {
            return None;
        }

        Some(CompressedPostingListView::new(
            self.slice_part(header.ids_start, header.ids_len),
            self.slice_part(
                header.ids_start + header.ids_len as u64,
                header.chunks_count,
            ),
            transmute_from_u8_to_slice(
                &self.mmap[remainders_start as usize..remainders_end as usize],
            ),
            header.last_id.checked_sub(1),
            header.quantization_params,
        ))
    }

    fn slice_part<T>(&self, start: impl Into<u64>, count: impl Into<u64>) -> &[T] {
        let start = start.into() as usize;
        let end = start + count.into() as usize * size_of::<T>();
        transmute_from_u8_to_slice(&self.mmap[start..end])
    }

    pub fn convert_and_save<P: AsRef<Path>>(
        index: &InvertedIndexCompressedImmutableRam<W>,
        path: P,
    ) -> std::io::Result<Self> {
        // 每个 postingList 都会对应一个 PostingListFileHeader
        let total_posting_headers_size =
            index.postings.as_slice().len() * size_of::<PostingListFileHeader<W>>();

        // 计算整个索引文件的长度
        let file_length = total_posting_headers_size
            + index
            .postings
            .as_slice()
            .iter()
            .map(|p| p.view().store_size().total)
            .sum::<usize>();
        let file_path = Self::index_file_path(path.as_ref());
        let file = create_and_ensure_length(file_path.as_ref(), file_length)?;

        let mut buf = BufWriter::new(file);

        // Save posting headers
        let mut offset: usize = total_posting_headers_size;
        for posting in index.postings.as_slice() {
            let store_size = posting.view().store_size();
            let posting_header = PostingListFileHeader::<W> {
                ids_start: offset as u64, // 首个 PostingList 的偏移量应该是所有的 header 长度
                ids_len: store_size.id_data_bytes as u32,
                chunks_count: store_size.chunks_count as u32,
                last_id: posting.view().last_id().map_or(0, |id| id + 1),
                quantization_params: posting.view().multiplier(),
            };
            buf.write_all(transmute_to_u8(&posting_header))?;
            offset += store_size.total;
        }

        // Save posting elements
        for posting in index.postings.as_slice() {
            let posting_view = posting.view();
            let (id_data, chunks, remainders) = posting_view.parts();
            buf.write_all(id_data)?;
            buf.write_all(transmute_to_u8_slice(chunks))?;
            buf.write_all(transmute_to_u8_slice(remainders))?;
        }

        buf.flush()?;
        drop(buf);

        // save header properties
        let file_header = InvertedIndexFileHeader {
            posting_count: index.postings.as_slice().len(),
            vector_count: index.vector_count,
        };
        atomic_save_json(&Self::index_config_file_path(path.as_ref()), &file_header)?;

        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(open_read_mmap(file_path.as_ref())?),
            file_header,
            _phantom: PhantomData,
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        // read core config file
        let config_file_path = Self::index_config_file_path(path.as_ref());
        // if the file header does not exist, the core is malformed
        let file_header: InvertedIndexFileHeader = read_json(&config_file_path)?;
        // read core data into mmap
        let file_path = Self::index_file_path(path.as_ref());
        let mmap = open_read_mmap(file_path.as_ref())?;
        madvise::madvise(&mmap, madvise::Advice::Normal)?;
        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(mmap),
            file_header,
            _phantom: PhantomData,
        })
    }
}