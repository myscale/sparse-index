use crate::core::common::types::DimId;
use crate::core::posting_list::PostingList;
use crate::core::{InvertedIndexMetrics, InvertedIndexRamAccess, QuantizedParam, QuantizedWeight};

/// Inverted flatten core from dimension id to posting list
/// TODO 希望这个 inverted index ram 内部的很多 function 都不需要在别的地方被调用吧
/// TODO 创建这个 inverted index ram 的时候应该都是直接使用的 inverted index ram builder, 而不是直接创建这个 inverted index ram
/// TODO 建议写几个新的 trait 去规范一些这些只读的 ram 行为
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexRam<TW: QuantizedWeight> {
    pub(super) postings: Vec<PostingList<TW>>,
    pub(super) quantized_params: Vec<Option<QuantizedParam>>,
    pub(super) metrics: InvertedIndexMetrics,
}

impl<TW: QuantizedWeight> InvertedIndexRam<TW> {
    // New empty inverted core
    // pub fn new() -> InvertedIndexRam<TW> {
    //     InvertedIndexRam {
    //         postings: Vec::new(),
    //         quantized_params: Vec::new(),
    //         metrics: InvertedIndexMetrics::default(),
    //     }
    // }

    pub fn postings(&self) -> &Vec<PostingList<TW>> {
        &self.postings
    }

    pub fn quantized_params(&self) -> &Vec<Option<QuantizedParam>> {
        &self.quantized_params
    }

    /// Get posting list for dim-id
    pub fn get(&self, dim_id: &DimId) -> Option<&PostingList<TW>> {
        self.postings.get(*dim_id as usize)
    }

    // fn inner_insert(&mut self, row_id: RowId, sparse_vector: SparseVector, from_update: bool) {
    //     let mut is_insert = false;
    //     for (dim_id, weight) in sparse_vector
    //         .indices
    //         .into_iter()
    //         .zip(sparse_vector.values.into_iter())
    //     {
    //         let dim_id = dim_id as usize;
    //         match self.postings.get_mut(dim_id) {
    //             Some(posting) => {
    //                 // update existing posting list
    //                 let posting_element: PostingElementEx<OW> = PostingElementEx::new(row_id, OW::from_f32(weight));
    //                 let (_upsert_idx, inserted) = posting.upsert(posting_element);
    //                 is_insert &= inserted;
    //             }
    //             None => {
    //                 // resize postings vector (fill gaps with empty posting lists)
    //                 self.postings.resize_with(dim_id + 1, PostingList::<OW>::new);
    //                 // initialize new posting for dimension
    //                 self.postings[dim_id] = PostingList::new_one(row_id, OW::from_f32(weight));
    //                 is_insert = true;
    //             }
    //         }
    //     }
    //     if is_insert && !from_update {
    //         self.vector_count = self.vector_count.saturating_add(1);
    //     }
    // }
}

impl<TW: QuantizedWeight> InvertedIndexRamAccess for InvertedIndexRam<TW> {
    // type Iter<'a> = PostingListIterator<'a, W>;

    fn size(&self) -> usize {
        self.postings.len()
    }

    // fn posting_len(&self, id: &DimId) -> Option<usize> {
    //     self.get(id).map(|posting_list| posting_list.len())
    // }

    // fn posting_with_param(&self, dim_id: &DimId) -> Option<(&PostingList<TW>, Option<QuantizedParam>)> {
    //     let dim_id = *dim_id as usize;

    //     if dim_id>=self.postings.len() {
    //         return None;
    //     }
    //     let posting: &PostingList<TW> = self.postings.get(dim_id).unwrap();
    //     let param: &Option<QuantizedParam> = self.quantized_params.get(dim_id).unwrap();
    //     return Some((posting, param.clone()));
    // }

    fn metrics(&self) -> InvertedIndexMetrics {
        self.metrics
    }

    // fn remove(&mut self, row_id: ElementOffsetType) {
    //     let mut exists = false;
    //     for posting in self.postings.iter_mut() {
    //         let (_remove_idx, removed) = posting.delete(row_id);
    //         exists |= removed;
    //     }
    //     if exists {
    //         self.vector_count = self.vector_count.saturating_sub(1);
    //     }
    // }

    // fn insert(&mut self, row_id: RowId, sparse_vector: SparseVector) {
    //     self.inner_insert(row_id, sparse_vector, false)

    // }

    // fn update(&mut self, row_id: RowId, new_vector: SparseVector, old_vector: SparseVector) {
    //     // Find elements of the old vector that are not in the new vector
    //     let elements_to_delete = old_vector
    //         .indices
    //         .iter()
    //         .filter(|&dim_id| !new_vector.indices.contains(dim_id))
    //         .map(|&dim_id| dim_id as usize);

    //     for dim_id in elements_to_delete {
    //         if let Some(posting) = self.postings.get_mut(dim_id) {
    //             let (_deleted_idx, _deleted) = posting.delete(row_id);
    //         }
    //     }

    //     self.inner_insert(row_id, new_vector, true)
    // }

    // fn iter(&self, id: &DimId) -> Option<PostingListIterator<W>> {
    //     self.get(id).map(|posting_list| posting_list.iter())
    // }
}
