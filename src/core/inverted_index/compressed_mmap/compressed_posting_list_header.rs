use crate::{
    core::{CompressedBlockType, QuantizedParam},
    RowId,
};

#[derive(Debug, Default, Clone)]
pub struct CompressedPostingListHeader {
    // offset for row_ids
    pub compressed_row_ids_start: usize,
    pub compressed_row_ids_end: usize,

    // offset for blocks
    pub compressed_blocks_start: usize,
    pub compressed_blocks_end: usize,

    // Fix sized: header for compressed posting
    pub quantized_params: Option<QuantizedParam>,

    pub compressed_block_type: CompressedBlockType,

    pub row_ids_count: RowId,
    pub max_row_id: Option<RowId>,
}

pub const COMPRESSED_POSTING_HEADER_SIZE: usize = std::mem::size_of::<CompressedPostingListHeader>();
