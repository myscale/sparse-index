use crate::{
    core::{ElementType, InvertedIndexError, InvertedIndexRam, QuantizedWeight, SparseVector},
    RowId,
};

pub trait InvertedIndexRamBuilderTrait<TW: QuantizedWeight> {
    fn new(element_type: ElementType) -> Self;
    fn add(&mut self, row_id: RowId, vector: SparseVector) -> Result<bool, InvertedIndexError>;
    fn build(self) -> Result<InvertedIndexRam<TW>, InvertedIndexError>;
    fn memory_usage(&self) -> Result<usize, InvertedIndexError>;
}
