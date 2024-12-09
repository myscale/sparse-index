use crate::{
    core::{QuantizedParam, QuantizedWeight},
    RowId,
};

use super::{CompressedBlockType, GenericCompressedPostingBlock, CompressedPostingListView, ExtendedCompressedPostingBlock, SimpleCompressedPostingBlock};

#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingList<TW: QuantizedWeight> {
    /// Compressed row_ids data, each block will has it's own offset in it.
    pub row_ids_compressed: Vec<u8>,

    /// Fixed-size chunks.
    /// 序列化的时候需要考虑到 size 的 overhead，不能够单独的以整个 Enum 去序列化
    pub generic_blocks: Vec<GenericCompressedPostingBlock<TW>>,

    /// `compressed_block_type` in blocks.
    pub compressed_block_type: CompressedBlockType,

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
            generic_blocks: &self.generic_blocks,
            compressed_block_type: self.compressed_block_type,
            quantization_params: self.quantization_params,
            row_ids_count: self.row_ids_count,
            max_row_id: self.max_row_id,
        }
    }
}