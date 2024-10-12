use super::{InvertedIndexConfig, StorageVersion};
use crate::core::common::types::{DimId, ElementOffsetType};
use crate::core::inverted_index::InvertedIndex;
use crate::core::posting_list::{PostingElementEx, PostingList, PostingListIterator};
use crate::core::sparse_vector::SparseVector;
use crate::RowId;
use std::borrow::Cow;
use std::path::{Path, PathBuf};

pub struct Version;

impl StorageVersion for Version {
    fn current_raw() -> &'static str {
        panic!("InvertedIndexRam is not supposed to be versioned");
    }
}

/// Inverted flatten core from dimension id to posting list
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexRam {
    /// Posting lists for each dimension flattened (dimension id -> posting list)
    /// Gaps are filled with empty posting lists
    pub postings: Vec<PostingList>,
    /// Number of unique indexed vectors
    /// pre-computed on build and upsert to avoid having to traverse the posting lists.
    /// 存储 unique vector 的数量，在构建和更新的时候计算, 避免遍历 posting list
    pub vector_count: usize,

    pub min_row_id: RowId,
    pub max_row_id: RowId,
}

impl InvertedIndex for InvertedIndexRam {
    type Iter<'a> = PostingListIterator<'a>;

    type Version = Version;

        
    fn open_with_config(_path: &Path, _config: InvertedIndexConfig) -> std::io::Result<Self> {
        panic!("InvertedIndexRam is not supposed to be loaded");
    }
    
    fn save_with_config(&self, _path: &Path, _config: InvertedIndexConfig) -> std::io::Result<()> {
        panic!("InvertedIndexRam is not supposed to be saved");
    }

    fn open(path: &Path) -> std::io::Result<Self> {
        Self::open_with_config(path, InvertedIndexConfig::default())
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        self.save_with_config(path, InvertedIndexConfig::default())
    }

    fn iter(&self, id: &DimId) -> Option<PostingListIterator> {
        self.get(id).map(|posting_list| posting_list.iter())
    }

    fn len(&self) -> usize {
        self.postings.len()
    }

    fn posting_list_len(&self, id: &DimId) -> Option<usize> {
        self.get(id).map(|posting_list| posting_list.elements.len())
    }

    fn files(_path: &Path, _config: InvertedIndexConfig) -> Vec<PathBuf> {
        Vec::new()
    }

    // TODO: 针对 CK 的功能不需要，只要在数据库的层面进行 remove 即可
    fn remove(&mut self, id: ElementOffsetType, old_vector: SparseVector) {
        for dim_id in old_vector.indices {
            self.postings[dim_id as usize].delete(id);
        }

        self.vector_count = self.vector_count.saturating_sub(1);
    }

    fn upsert(
        &mut self,
        id: ElementOffsetType,
        vector: SparseVector,
        old_vector: Option<SparseVector>,
    ) {
        self.upsert(id, vector, old_vector);
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
        _config: Option<InvertedIndexConfig>
    ) -> std::io::Result<Self> {
        Ok(ram_index.into_owned())
    }

    fn vector_count(&self) -> usize {
        self.vector_count
    }

    fn max_index(&self) -> Option<DimId> {
        match self.postings.len() {
            0 => None,
            len => Some(len as DimId - 1),
        }
    }

}

impl InvertedIndexRam {
    /// New empty inverted core
    pub fn empty() -> InvertedIndexRam {
        InvertedIndexRam {
            postings: Vec::new(),
            vector_count: 0,
            min_row_id: RowId::MAX,
            max_row_id: RowId::MIN,
        }
    }

    fn min_row_id(&self) -> RowId {
        self.min_row_id
    }

    fn max_row_id(&self) -> RowId {
        self.max_row_id
    }

    /// Get posting list for dimension id
    pub fn get(&self, id: &DimId) -> Option<&PostingList> {
        self.postings.get((*id) as usize)
    }

    /// Upsert a vector into the inverted core.
    pub fn upsert(
        &mut self,
        id: ElementOffsetType,
        vector: SparseVector,
        old_vector: Option<SparseVector>,
    ) {
        // Find elements of the old vector that are not in the new vector
        // TODO: 在 CK 集成并不需要考虑 old vector
        if let Some(old_vector) = &old_vector {
            let elements_to_delete = old_vector
                .indices
                .iter()
                .filter(|&dim_id| !vector.indices.contains(dim_id))
                .map(|&dim_id| dim_id as usize);
            for dim_id in elements_to_delete {
                if let Some(posting) = self.postings.get_mut(dim_id) {
                    posting.delete(id);
                }
            }
        }

        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            match self.postings.get_mut(dim_id) {
                Some(posting) => {
                    // update existing posting list
                    let posting_element = PostingElementEx::new(id, weight);
                    posting.upsert(posting_element);
                }
                None => {
                    // resize postings vector (fill gaps with empty posting lists)
                    self.postings.resize_with(dim_id + 1, PostingList::default);
                    // initialize new posting for dimension
                    self.postings[dim_id] = PostingList::new_one(id, weight);
                }
            }
        }
        if old_vector.is_none() {
            self.vector_count += 1;
        }
    }
}
