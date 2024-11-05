use crate::core::common::ops::*;
use crate::core::common::types::{DimId, DimOffset};
use crate::core::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::core::inverted_index::InvertedIndex;
use crate::core::posting_list::{PostingElementEx, PostingListIterator};
use crate::core::{
    INVERTED_INDEX_META_FILE_SUFFIX, INVERTED_INDEX_OFFSETS_SUFFIX, INVERTED_INDEX_POSTINGS_SUFFIX,
};
use crate::RowId;
use log::warn;
use memmap2::{Mmap, MmapMut};
use std::borrow::Cow;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{InvertedIndexMeta, Revision, Version, INVERTED_INDEX_FILE_NAME};

pub const POSTING_OFFSET_SIZE: usize = size_of::<PostingListOffset>();

// 在 mmap 文件里面找到一段 posting List 所在的 offsets
#[derive(Debug, Default, Clone)]
pub struct PostingListOffset {
    pub start_offset: usize,
    pub end_offset: usize,
}

// InvertedIndexMmap 索引格式下相关的文件
pub struct InvertedIndexMmapFileConfig;

impl InvertedIndexMmapFileConfig {
    pub fn get_posting_offset_file_name(segment_id: Option<&str>) -> String {
        format!(
            "{}{}",
            segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME),
            INVERTED_INDEX_OFFSETS_SUFFIX
        )
    }
    pub fn get_posting_data_file_name(segment_id: Option<&str>) -> String {
        format!(
            "{}{}",
            segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME),
            INVERTED_INDEX_POSTINGS_SUFFIX
        )
    }
    pub fn get_inverted_meta_file_name(segment_id: Option<&str>) -> String {
        format!(
            "{}{}",
            segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME),
            INVERTED_INDEX_META_FILE_SUFFIX
        )
    }
    pub fn get_all_files(segment_id: Option<&str>) -> Vec<String> {
        vec![
            Self::get_posting_offset_file_name(segment_id),
            Self::get_posting_data_file_name(segment_id),
            Self::get_inverted_meta_file_name(segment_id),
        ]
    }
}

/// Inverted flatten core from dimension id to posting list
#[derive(Debug, Clone)]
pub struct InvertedIndexMmap {
    pub path: PathBuf,
    pub offsets_mmap: Arc<Mmap>,
    pub postings_mmap: Arc<Mmap>,
    pub meta: InvertedIndexMeta,
}

impl InvertedIndex for InvertedIndexMmap {
    type Iter<'a> = PostingListIterator<'a>;

    fn open(path: &Path, segment_id: Option<&str>) -> std::io::Result<Self> {
        Self::load_under_segment(path, segment_id)
    }

    /// 实际上没有执行任何存储的逻辑，仅检查了文件路径是否存在
    fn save(&self, path: &Path, segment_id: Option<&str>) -> std::io::Result<()> {
        debug_assert_eq!(path, self.path);
        for file in self.files(segment_id) {
            debug_assert!(file.exists());
        }
        Ok(())
    }

    fn iter(&self, id: &DimOffset) -> Option<Self::Iter<'_>> {
        // map 后面接的是一个 function，将 &[PostingElementEx] 类型转换为了 PostingListIterator
        self.get(id).map(PostingListIterator::new)
    }

    fn size(&self) -> usize {
        self.meta.posting_count()
    }

    fn vector_count(&self) -> usize {
        self.meta.vector_count()
    }

    fn min_dim_id(&self) -> DimId {
        self.meta.min_dim_id()
    }

    fn max_dim_id(&self) -> DimId {
        self.meta.max_dim_id()
    }

    fn posting_size(&self, dim_id: &DimId) -> Option<usize> {
        self.get(dim_id).map(|posting_list| posting_list.len())
    }

    fn files(&self, segment_id: Option<&str>) -> Vec<PathBuf> {
        // 仅仅获得相对路径
        let get_all_files = InvertedIndexMmapFileConfig::get_all_files(segment_id);
        get_all_files.iter().map(|p| PathBuf::from(p)).collect()
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        path: P,
        segment_id: Option<&str>,
    ) -> std::io::Result<Self> {
        Self::convert_and_save(&ram_index, path, segment_id)
    }

    fn remove(&mut self, _row_id: RowId) {
        panic!("Cannot remove from a read-only mmap inverted core")
    }

    fn insert(&mut self, _row_id: RowId, _sparse_vector: crate::core::SparseVector) {
        panic!("Cannot insert from a read-only mmap inverted core")
    }

    fn update(
        &mut self,
        _row_id: RowId,
        _new_vector: crate::core::SparseVector,
        _old_vector: crate::core::SparseVector,
    ) {
        panic!("Cannot update from a read-only mmap inverted core")
    }
}

impl InvertedIndexMmap {
    /// 获得索引中最小的 row id
    pub fn min_row_id(&self) -> RowId {
        self.meta.min_row_id()
    }

    /// 获得索引中最大的 row_id
    pub fn max_row_id(&self) -> RowId {
        self.meta.max_row_id()
    }

    /// 根据 dim-id 拿到对应的 PostingList
    pub fn get(&self, dim_id: &DimId) -> Option<&[PostingElementEx]> {
        // check that the id is not out of bounds (posting_count includes the empty zeroth entry)
        if *dim_id >= self.meta.posting_count() as DimId {
            warn!(
                "dim_id is overflow, dim_id should smaller than {}",
                self.meta.posting_count()
            );
            return None;
        }
        // 加载 posting 对应的 offset obj
        let offset_left = *dim_id as usize * POSTING_OFFSET_SIZE;
        let offset_obj = transmute_from_u8::<PostingListOffset>(
            &self.offsets_mmap[offset_left..(offset_left + POSTING_OFFSET_SIZE)],
        )
        .clone(); // TODO 将 clone 删除掉会提升性能吗？

        // 根据 offset 去 postings 文件中查找到对应的 posting 数据
        let elements_bytes =
            &self.postings_mmap[offset_obj.start_offset as usize..offset_obj.end_offset as usize];
        Some(transmute_from_u8_to_slice(elements_bytes))
    }

    /// 将 ram 中的 inverted core 存储至 mmap
    pub fn convert_and_save<P: AsRef<Path>>(
        inverted_index_ram: &InvertedIndexRam,
        directory: P,
        segment_id: Option<&str>,
    ) -> std::io::Result<Self> {
        // compute posting_offsets and elements size.
        let total_postings_offsets_size: usize = inverted_index_ram.size() * POSTING_OFFSET_SIZE;
        let total_posting_elements_size: usize = inverted_index_ram
            .postings()
            .iter()
            .map(|posting| posting.len() * size_of::<PostingElementEx>())
            .sum();

        // 初始化 3 个文件路径.
        let meta_file_path =
            directory
                .as_ref()
                .join(InvertedIndexMmapFileConfig::get_inverted_meta_file_name(
                    segment_id,
                ));
        let offsets_mmap_file_path =
            directory
                .as_ref()
                .join(InvertedIndexMmapFileConfig::get_posting_offset_file_name(
                    segment_id,
                ));
        let postings_mmap_file_path =
            directory
                .as_ref()
                .join(InvertedIndexMmapFileConfig::get_posting_data_file_name(
                    segment_id,
                ));

        // 创建 2 个 mmap 文件.
        create_and_ensure_length(
            offsets_mmap_file_path.as_ref(),
            total_postings_offsets_size as u64,
        )?;
        let mut offsets_mmap: MmapMut = open_write_mmap(offsets_mmap_file_path.as_ref())?;
        madvise::madvise(&offsets_mmap, madvise::Advice::Normal)?;

        create_and_ensure_length(
            postings_mmap_file_path.as_ref(),
            total_posting_elements_size as u64,
        )?;
        let mut postings_mmap: MmapMut = open_write_mmap(postings_mmap_file_path.as_ref())?;
        madvise::madvise(&postings_mmap, madvise::Advice::Normal)?;

        // file core data
        Self::save_postings_offsets(&mut offsets_mmap, inverted_index_ram);

        Self::save_postings_elements(&mut postings_mmap, inverted_index_ram);

        if total_postings_offsets_size > 0 {
            offsets_mmap.flush()?;
        }
        if total_posting_elements_size > 0 {
            postings_mmap.flush()?;
        }

        // save header properties 实际上就是 meta data
        let meta = InvertedIndexMeta::new(
            inverted_index_ram.size(),
            inverted_index_ram.vector_count(),
            inverted_index_ram.min_row_id(),
            inverted_index_ram.max_row_id(),
            inverted_index_ram.min_dim_id(),
            inverted_index_ram.max_dim_id(),
            total_posting_elements_size,
            total_posting_elements_size,
            Version::memory(Revision::V1),
        );

        atomic_save_json(&meta_file_path, &meta)?;

        Ok(Self {
            path: directory.as_ref().to_owned(),
            offsets_mmap: Arc::new(offsets_mmap.make_read_only()?),
            postings_mmap: Arc::new(postings_mmap.make_read_only()?),
            meta,
        })
    }

    /// 加载指定目录下的 mmap 索引文件
    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Self::load_under_segment(path, None)
    }

    /// 给定具体配置, 并加载指定目录下的 mmap 索引文件
    pub fn load_under_segment<P: AsRef<Path>>(
        path: P,
        segment_id: Option<&str>,
    ) -> std::io::Result<Self> {
        // read meta file data.
        let meta_file_path =
            path.as_ref()
                .join(InvertedIndexMmapFileConfig::get_inverted_meta_file_name(
                    segment_id,
                ));
        let meta_data: InvertedIndexMeta = read_json(&meta_file_path)?;

        // read inverted index data.
        let offsets_mmap_file_path =
            path.as_ref()
                .join(InvertedIndexMmapFileConfig::get_posting_offset_file_name(
                    segment_id,
                ));
        let postings_mmap_file_path =
            path.as_ref()
                .join(InvertedIndexMmapFileConfig::get_posting_data_file_name(
                    segment_id,
                ));

        let offsets_mmap = open_read_mmap(offsets_mmap_file_path.as_ref())?;
        let postings_mmap = open_read_mmap(postings_mmap_file_path.as_ref())?;

        // TODO 使用顺序读取，观察 QPS 有什么变化
        madvise::madvise(&offsets_mmap, madvise::Advice::Normal)?;
        madvise::madvise(&postings_mmap, madvise::Advice::Normal)?;

        Ok(Self {
            path: path.as_ref().to_owned(),
            offsets_mmap: Arc::new(offsets_mmap),
            postings_mmap: Arc::new(postings_mmap),
            meta: meta_data,
        })
    }

    /// 将 posting offsets 存储到 mmap
    fn save_postings_offsets(mmap: &mut MmapMut, inverted_index_ram: &InvertedIndexRam) {
        let mut current_element_offset: usize = 0;

        for (id, posting) in inverted_index_ram.postings().iter().enumerate() {
            // generate an offset object.
            let offset_obj = PostingListOffset {
                start_offset: current_element_offset,
                end_offset: current_element_offset
                    + (posting.len() * size_of::<PostingElementEx>()),
            };

            // update current element offset.
            current_element_offset = offset_obj.end_offset;

            // save the offset object to mmap.
            let posting_header_bytes = transmute_to_u8(&offset_obj);
            let start_posting_offset = id * POSTING_OFFSET_SIZE;
            let end_posting_offset = (id + 1) * POSTING_OFFSET_SIZE;
            mmap[start_posting_offset..end_posting_offset].copy_from_slice(posting_header_bytes);
        }
    }

    /// 将 postings 内部的 elements 存储到 mmap
    fn save_postings_elements(mmap: &mut MmapMut, inverted_index_ram: &InvertedIndexRam) {
        let mut current_element_offset = 0;
        for posting in inverted_index_ram.postings() {
            // save posting element
            let posting_elements_bytes = transmute_to_u8_slice(&posting.elements);
            mmap[current_element_offset..(current_element_offset + posting_elements_bytes.len())]
                .copy_from_slice(posting_elements_bytes);
            current_element_offset += posting_elements_bytes.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
    use tempfile::Builder;

    fn compare_indexes(
        inverted_index_ram: &InvertedIndexRam,
        inverted_index_mmap: &InvertedIndexMmap,
    ) {
        for id in 0..inverted_index_ram.size() as DimId {
            let posting_list_ram = inverted_index_ram.get(&id).unwrap().elements.as_slice();
            let posting_list_mmap = inverted_index_mmap.get(&id).unwrap();
            assert_eq!(posting_list_ram.len(), posting_list_mmap.len());
            for i in 0..posting_list_ram.len() {
                assert_eq!(posting_list_ram[i], posting_list_mmap[i]);
            }
        }
    }

    #[test]
    fn test_inverted_index_mmap() {
        // skip 4th dimension
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0), (5, 10.0)].into());
        builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0), (5, 20.0)].into());
        builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
        builder.add(4, [(1, 1.0), (2, 1.0)].into());
        builder.add(5, [(1, 2.0)].into());
        builder.add(6, [(1, 3.0)].into());
        builder.add(7, [(1, 4.0)].into());
        builder.add(8, [(1, 5.0)].into());
        builder.add(9, [(1, 6.0)].into());
        let inverted_index_ram = builder.build();

        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();

        {
            let inverted_index_mmap =
                InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path, None)
                    .unwrap();

            compare_indexes(&inverted_index_ram, &inverted_index_mmap);
        }
        let inverted_index_mmap = InvertedIndexMmap::load(&tmp_dir_path).unwrap();
        // posting_count: 0th entry is always empty + 1st + 2nd + 3rd + 4th empty + 5th
        assert_eq!(inverted_index_mmap.size(), 6);
        assert_eq!(inverted_index_mmap.vector_count(), 9);

        compare_indexes(&inverted_index_ram, &inverted_index_mmap);

        assert!(inverted_index_mmap.get(&0).unwrap().is_empty()); // the first entry is always empty as dimension ids start at 1
        assert_eq!(inverted_index_mmap.get(&1).unwrap().len(), 9);
        assert_eq!(inverted_index_mmap.get(&2).unwrap().len(), 4);
        assert_eq!(inverted_index_mmap.get(&3).unwrap().len(), 3);
        assert!(inverted_index_mmap.get(&4).unwrap().is_empty()); // return empty posting list info for intermediary empty ids
        assert_eq!(inverted_index_mmap.get(&5).unwrap().len(), 2);
        // index after the last values are None
        assert!(inverted_index_mmap.get(&6).is_none());
        assert!(inverted_index_mmap.get(&7).is_none());
        assert!(inverted_index_mmap.get(&100).is_none());
    }
}
