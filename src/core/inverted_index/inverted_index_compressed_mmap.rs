use crate::core::common::ops::*;
use crate::core::common::types::{DimId, DimOffset, ElementOffsetType, Weight};
use crate::core::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use crate::core::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::core::inverted_index::{InvertedIndex};
use crate::core::posting_list::{
    CompressedPostingChunk, CompressedPostingListIterator, CompressedPostingListView,
    GenericPostingElement,
};
use crate::core::sparse_vector::SparseVector;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{InvertedIndexConfig, StorageVersion};


pub struct Version;

impl StorageVersion for Version {
    fn current_raw() -> &'static str {
        "0.2.0"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InvertedIndexFileHeader {
    pub posting_count: usize, // number of posting
    pub vector_count: usize,  // all unique sparse vectors
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
    _phantom: PhantomData<W>, // 零大小类型，用于在范型上下文中标记类型 W 的存在，不占用实际的内存空间，主要用于编译时的类型检查与约束
}

impl<W: Weight> InvertedIndex for InvertedIndexCompressedMmap<W> {
    type Iter<'a> = CompressedPostingListIterator<'a, W>;
    type Version = Version;


    fn open_with_config(path: &Path, config: InvertedIndexConfig) -> std::io::Result<Self> {
        Self::load_with_config(path, config)
    }
    fn open(path: &Path) -> std::io::Result<Self> {
        Self::load(path)
    }

    fn save_with_config(&self, path: &Path, config: InvertedIndexConfig) -> std::io::Result<()> {
        debug_assert_eq!(path, self.path);
        // mmap 不需要通过该函数保存
        for file in Self::files(path, config) {
            debug_assert!(file.exists());
        }
        Ok(())
    }
    fn save(&self, path: &Path) -> std::io::Result<()> {
        Self::save_with_config(&self, path, InvertedIndexConfig::default())
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

    fn files(path: &Path, config: InvertedIndexConfig) -> Vec<PathBuf> {
        vec![
            path.join(config.data_file_name()),
            path.join(config.meta_file_name()),
        ]
    }

    fn remove(&mut self, id: ElementOffsetType, old_vector: SparseVector) {
        panic!("Cannot remove from a read-only mmap inverted core")
    }

    fn upsert(
        &mut self,
        id: ElementOffsetType,
        vector: SparseVector,
        old_vector: Option<SparseVector>,
    ) {
        panic!("Cannot upsert into a read-only mmap inverted core")
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        path: P,
        config: Option<InvertedIndexConfig>
    ) -> std::io::Result<Self> {
        let index = InvertedIndexCompressedImmutableRam::from_ram_index(ram_index, &path, config.clone())?;
        Self::convert_and_save(&index, path, config.unwrap_or_default())
    }

    fn vector_count(&self) -> usize {
        self.file_header.vector_count
    }

    fn max_index(&self) -> Option<DimOffset> {
        match self.file_header.posting_count {
            0 => None,
            len => Some(len as DimId - 1),
        }
    }
    

}


impl<W: Weight> InvertedIndexCompressedMmap<W> {

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
        config: InvertedIndexConfig
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
        let file_path = path.as_ref().join(config.data_file_name());
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
        let meta_path = path.as_ref().join(config.meta_file_name());
        atomic_save_json(&meta_path, &file_header)?;

        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(open_read_mmap(file_path.as_ref())?),
            file_header,
            _phantom: PhantomData,
        })
    }



    pub fn load_with_config<P: AsRef<Path>>(path: P, config: InvertedIndexConfig) -> std::io::Result<Self> {
        // read meta data file
        let meta_path = path.as_ref().join(config.meta_file_name());
        let file_header: InvertedIndexFileHeader = read_json(&meta_path)?;

        // read index data file in mmap);
        let file_path = path.as_ref().join(config.data_file_name());
        let mmap = open_read_mmap(file_path.as_ref())?;
        madvise::madvise(&mmap, madvise::Advice::Normal)?;

        Ok(Self {
            path: path.as_ref().to_owned(),
            mmap: Arc::new(mmap),
            file_header,
            _phantom: PhantomData,
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        return Self::load_with_config(path, InvertedIndexConfig::default());
    }
}
