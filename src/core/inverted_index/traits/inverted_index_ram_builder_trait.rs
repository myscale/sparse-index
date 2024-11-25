use crate::{
    core::{InvertedIndexRam, QuantizedWeight, SparseVector},
    RowId,
};

pub trait InvertedIndexRamBuilderTrait<TW: QuantizedWeight> {
    fn add(&mut self, row_id: RowId, vector: SparseVector) -> bool;
    fn build(self) -> InvertedIndexRam<TW>;
    fn memory_usage(&self) -> usize;
}
