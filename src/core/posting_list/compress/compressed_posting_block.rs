use crate::{
    core::{QuantizedWeight, COMPRESSION_BLOCK_SIZE},
    RowId,
};

#[derive(Debug, Clone, PartialEq)]
pub struct CompressedPostingBlock<TW: QuantizedWeight> {
    /// Current block's first `row_id`.
    pub row_id_start: RowId,

    /// Block's storage offset within the whole `Posting`.
    pub block_offset: u64,

    /// Current block's `row_ids_compressed`(type is [u8]) data size.
    pub row_ids_compressed_size: u16,

    /// How many row_ids does current block stored. (We ensure this value smaller than [`COMPRESSION_BLOCK_SIZE`])
    pub row_ids_count: u8,

    /// It's necessary for uncompress operation.
    pub num_bits: u8,

    /// payload storage.
    pub weights: [TW; COMPRESSION_BLOCK_SIZE],

    /// payload storage.
    pub max_next_weights: [TW; COMPRESSION_BLOCK_SIZE],
}
