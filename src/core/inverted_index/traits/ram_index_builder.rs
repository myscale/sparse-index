use crate::{
    core::{ElementType, InvertedIndexRam, QuantizedWeight, SparseVector},
    RowId,
};

pub trait InvertedIndexRamBuilderTrait<TW: QuantizedWeight> {
    fn new(element_type: ElementType) -> Self;
    fn add(&mut self, row_id: RowId, vector: SparseVector) -> bool;
    fn build(self) -> InvertedIndexRam<TW>;
    fn memory_usage(&self) -> usize;
}
