use log::error;

use crate::core::common::types::{DimId, ElementOffsetType};
use crate::core::inverted_index::InvertedIndex;
use crate::core::posting_list::{self, PostingElementEx, PostingList, PostingListIterator};
use crate::core::sparse_vector::SparseVector;
use crate::RowId;
use std::borrow::Cow;
use std::io::Error;
use std::path::{Path, PathBuf};

/// Inverted flatten core from dimension id to posting list
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexRam {
    /// index 中所有的 postings, dim-id 即数组的下标
    pub(super) postings: Vec<PostingList>,

    /// 记录 index 中 unique vector 的数量, 对估算内存占用比较有用
    pub(super) vector_count: usize,

    /// index 中最小的 row id
    pub(super) min_row_id: RowId,

    /// index 中最大的 row id
    pub(super) max_row_id: RowId,

    /// index 中最小的 dim id
    pub(super) min_dim_id: DimId,

    /// index 中最大的 dim id
    pub(super) max_dim_id: DimId,
}

/// metrics
impl InvertedIndexRam {
    pub fn min_row_id(&self) -> RowId {
        self.min_row_id
    }

    pub fn max_row_id(&self) -> RowId {
        self.max_row_id
    }

    pub fn min_dim_id(&self) -> DimId {
        self.min_dim_id
    }

    pub fn max_dim_id(&self) -> DimId {
        self.max_dim_id
    }

    pub fn postings(&self) -> &Vec<PostingList> {
        &self.postings
    }
}

impl InvertedIndexRam {
    /// New empty inverted core
    pub fn new() -> InvertedIndexRam {
        InvertedIndexRam {
            postings: Vec::new(),
            vector_count: 0,
            min_row_id: RowId::MAX,
            max_row_id: RowId::MIN,
            min_dim_id: 0,
            max_dim_id: DimId::MIN,
        }
    }

    /// Get posting list for dim-id
    pub fn get(&self, dim_id: &DimId) -> Option<&PostingList> {
        self.postings.get((*dim_id) as usize)
    }

    /// remove one row from index.
    pub fn remove(&mut self, row_id: RowId) {
        for posting in self.postings.iter_mut() {
            posting.delete(row_id);
        }
        self.vector_count = self.vector_count.saturating_sub(1);
    }

    pub fn insert(&mut self, row_id: RowId, sparse_vector: SparseVector) {
        for (dim_id, weight) in sparse_vector
            .indices
            .into_iter()
            .zip(sparse_vector.values.into_iter())
        {
            let dim_id = dim_id as usize;
            match self.postings.get_mut(dim_id) {
                Some(posting) => {
                    // update existing posting list
                    let posting_element = PostingElementEx::new(row_id, weight);
                    posting.upsert(posting_element);
                }
                None => {
                    // resize postings vector (fill gaps with empty posting lists)
                    self.postings.resize_with(dim_id + 1, PostingList::default);
                    // initialize new posting for dimension
                    self.postings[dim_id] = PostingList::new_one(row_id, weight);
                }
            }
        }

        self.vector_count = self.vector_count.saturating_add(1);
    }

    /// Upsert a vector into the inverted core.
    pub fn update(&mut self, row_id: RowId, new_vector: SparseVector, old_vector: SparseVector) {
        // Find elements of the old vector that are not in the new vector
        let elements_to_delete = old_vector
            .indices
            .iter()
            .filter(|&dim_id| !new_vector.indices.contains(dim_id))
            .map(|&dim_id| dim_id as usize);
        for dim_id in elements_to_delete {
            if let Some(posting) = self.postings.get_mut(dim_id) {
                posting.delete(row_id);
            }
        }

        self.insert(row_id, new_vector);
    }

    /// call propagate for all postings.
    pub fn commit(&mut self) {
        for posting in self.postings.iter_mut() {
            if posting.len() > 1 {
                posting.refine();
            }
        }
    }
}

impl InvertedIndex for InvertedIndexRam {
    type Iter<'a> = PostingListIterator<'a>;

    fn open(_path: &Path, _segment_id: Option<&str>) -> std::io::Result<Self> {
        let error_msg: &str = "InvertedIndexRam doesn't support call open.";
        error!("{}", error_msg);
        Err(Error::new(std::io::ErrorKind::Other, error_msg))
    }

    fn save(&self, _path: &Path, _segment_id: Option<&str>) -> std::io::Result<()> {
        let error_msg: &str = "InvertedIndexRam doesn't support call save.";
        error!("{}", error_msg);
        Err(Error::new(std::io::ErrorKind::Other, error_msg))
    }

    fn iter(&self, id: &DimId) -> Option<PostingListIterator> {
        self.get(id).map(|posting_list| posting_list.iter())
    }

    fn size(&self) -> usize {
        self.postings.len()
    }

    fn vector_count(&self) -> usize {
        self.vector_count
    }

    fn min_dim_id(&self) -> DimId {
        self.min_dim_id()
    }

    fn max_dim_id(&self) -> DimId {
        self.max_dim_id()
    }

    fn posting_size(&self, id: &DimId) -> Option<usize> {
        self.get(id).map(|posting_list| posting_list.len())
    }

    fn files(&self, _segment_id: Option<&str>) -> Vec<PathBuf> {
        Vec::new()
    }

    fn remove(&mut self, row_id: ElementOffsetType) {
        self.remove(row_id);
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
        _segment_id: Option<&str>,
    ) -> std::io::Result<Self> {
        Ok(ram_index.into_owned())
    }

    fn insert(&mut self, row_id: RowId, sparse_vector: SparseVector) {
        self.insert(row_id, sparse_vector);
    }

    fn update(&mut self, row_id: RowId, new_vector: SparseVector, old_vector: SparseVector) {
        self.update(row_id, new_vector, old_vector);
    }
}
