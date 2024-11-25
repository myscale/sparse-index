use crate::{
    core::{QuantizedParam, QuantizedWeight},
    RowId,
};

use super::{CompressedPostingBlock, CompressedPostingListView};

#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingList<TW: QuantizedWeight> {
    /// Compressed row_ids data, each block will has it's own offset in it.
    pub row_ids_compressed: Vec<u8>,

    /// Fixed-size chunks.
    pub blocks: Vec<CompressedPostingBlock<TW>>,

    /// Quantization parameters.
    pub quantization_params: Option<QuantizedParam>,

    /// Total row ids count.
    pub row_ids_count: RowId,

    /// Max row id
    pub max_row_id: Option<RowId>,
}

impl<TW: QuantizedWeight> CompressedPostingList<TW> {
    pub fn len(&self) -> usize {
        self.row_ids_count as usize
    }
    pub fn view(&self) -> CompressedPostingListView<TW> {
        CompressedPostingListView {
            row_ids_compressed: &self.row_ids_compressed,
            blocks: &self.blocks,
            quantization_params: self.quantization_params,
            row_ids_count: self.row_ids_count,
            max_row_id: self.max_row_id,
        }
    }
}