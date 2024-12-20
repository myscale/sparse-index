use crate::core::common::types::DimId;
use crate::core::inverted_index::common::InvertedIndexMetrics;
use crate::core::posting_list::PostingList;
use crate::core::{ElementType, InvertedIndexRamAccess, QuantizedParam, QuantizedWeight};

#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexRam<TW: QuantizedWeight> {
    pub(super) postings: Vec<PostingList<TW>>,
    pub(super) element_type: ElementType,
    pub(super) quantized_params: Vec<Option<QuantizedParam>>,
    pub need_quantized: bool,
    pub(super) metrics: InvertedIndexMetrics,
}

impl<TW: QuantizedWeight> InvertedIndexRam<TW> {
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
}

impl<TW: QuantizedWeight> InvertedIndexRamAccess for InvertedIndexRam<TW> {
    fn size(&self) -> usize {
        self.postings.len()
    }

    fn metrics(&self) -> InvertedIndexMetrics {
        self.metrics
    }

    fn element_type(&self) -> ElementType {
        self.element_type
    }
}
