use crossbeam_channel::select_biased;

use crate::core::common::types::{DimOffset, ElementOffsetType};
use crate::core::inverted_index::InvertedIndexRam;
use crate::core::posting_list::PostingListIter;
use crate::core::sparse_vector::SparseVector;
use crate::core::DimId;
use crate::RowId;
use std::borrow::Cow;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

pub trait InvertedIndex: Sized + Debug {
    // 联合类型 Iter 会持有对 InvertedIndex 实例对象中属性字段的引用
    type Iter<'a>: PostingListIter + Clone
    where
        Self: 'a;

    /// 打开一个 inverted index 文件
    fn open(path: &Path, segment_id: Option<&str>) -> std::io::Result<Self>;

    /// 将 inverted index 存储到文件
    fn save(&self, path: &Path, segment_id: Option<&str>) -> std::io::Result<()>;

    /// Get posting list for dimension id
    fn iter(&self, dim_id: &DimId) -> Option<Self::Iter<'_>>;

    /// Get number of posting lists
    fn size(&self) -> usize;

    /// Check if the core is empty
    fn empty(&self) -> bool {
        self.size() == 0
    }

    /// Get number of posting lists for dimension id
    fn posting_size(&self, dim_id: &DimId) -> Option<usize>;

    /// Number of indexed vectors
    fn vector_count(&self) -> usize;

    fn min_dim_id(&self) -> DimId;

    fn max_dim_id(&self) -> DimId;

    // Get max existed core
    fn max_dim(&self) -> Option<DimId> {
        match self.size() {
            0 => None,
            len => Some(len as DimId - 1),
        }
    }

    /// Files used by this core
    fn files(&self, segment_id: Option<&str>) -> Vec<PathBuf>;

    fn remove(&mut self, row_id: RowId);

    fn insert(&mut self, row_id: RowId, sparse_vector: SparseVector);

    fn update(&mut self, row_id: RowId, new_vector: SparseVector, old_vector: SparseVector);

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        path: P,
        segment_id: Option<&str>,
    ) -> std::io::Result<Self>;
}
