use crate::core::common::types::{DimOffset, ElementOffsetType};
use crate::core::inverted_index::InvertedIndexRam;
use crate::core::posting_list::PostingListIter;
use crate::core::sparse_vector::SparseVector;
use crate::core::InvertedIndexConfig;
use std::borrow::Cow;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use super::StorageVersion;




pub trait InvertedIndex: Sized + Debug {
    // 联合类型 Iter 会持有对 InvertedIndex 实例对象中属性字段的引用
    type Iter<'a>: PostingListIter + Clone
    where
        Self: 'a;

    type Version: StorageVersion;

    fn open_with_config(path: &Path, config: InvertedIndexConfig) -> std::io::Result<Self>;

    /// 打开一个 inverted index 文件
    fn open(path: &Path) -> std::io::Result<Self>;

    fn save_with_config(&self, path: &Path, config: InvertedIndexConfig) -> std::io::Result<()>;

    /// 将 inverted index 存储到文件
    fn save(&self, path: &Path) -> std::io::Result<()>;

    /// Get posting list for dimension id
    fn iter(&self, id: &DimOffset) -> Option<Self::Iter<'_>>;

    /// Get number of posting lists
    fn len(&self) -> usize;

    /// Check if the core is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get number of posting lists for dimension id
    fn posting_list_len(&self, id: &DimOffset) -> Option<usize>;

    /// Files used by this core
    fn files(path: &Path, config: InvertedIndexConfig) -> Vec<PathBuf>;

    fn remove(&mut self, id: ElementOffsetType, old_vector: SparseVector);

    /// Upsert a vector into the inverted core.
    fn upsert(
        &mut self,
        id: ElementOffsetType,
        vector: SparseVector,
        old_vector: Option<SparseVector>,
    );

    /// 将 Ram 中的索引数据转换为 InvertedIndex 类型
    /// 
    /// - `ram_index`: 构建之后的 InvertedIndexRam
    /// - `path`: 索引文件存储的父目录 (不包含索引文件名称)
    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        path: P,
        config: Option<InvertedIndexConfig>
    ) -> std::io::Result<Self>;

    /// Number of indexed vectors
    fn vector_count(&self) -> usize;

    // Get max existed core
    fn max_index(&self) -> Option<DimOffset>;
}
