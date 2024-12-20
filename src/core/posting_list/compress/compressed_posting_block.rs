use crate::{
    core::{ElementType, QuantizedWeight, COMPRESSION_BLOCK_SIZE},
    RowId,
};

#[derive(Default, Copy, Debug, Clone, PartialEq)]
pub enum CompressedBlockType {
    #[default]
    Simple,
    Extended,
}

impl From<ElementType> for CompressedBlockType {
    fn from(element_type: ElementType) -> Self {
        match element_type {
            ElementType::SIMPLE => CompressedBlockType::Simple,
            ElementType::EXTENDED => CompressedBlockType::Extended,
        }
    }
}

impl Into<ElementType> for CompressedBlockType {
    fn into(self) -> ElementType {
        match self {
            CompressedBlockType::Simple => ElementType::SIMPLE,
            CompressedBlockType::Extended => ElementType::EXTENDED,
        }
    }
}

pub trait CompressedPostingBlock<W: QuantizedWeight> {
    fn compressed_block_type(&self) -> CompressedBlockType;
}

#[derive(Debug, Clone, Copy)]
pub struct SimpleCompressedPostingBlock<TW>
where
    TW: QuantizedWeight,
{
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
}

#[derive(Debug, Clone, Copy)]
pub struct ExtendedCompressedPostingBlock<TW>
where
    TW: QuantizedWeight,
{
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

impl<TW: QuantizedWeight> CompressedPostingBlock<TW> for SimpleCompressedPostingBlock<TW> {
    fn compressed_block_type(&self) -> CompressedBlockType {
        CompressedBlockType::Simple
    }
}

impl<TW: QuantizedWeight> CompressedPostingBlock<TW> for ExtendedCompressedPostingBlock<TW> {
    fn compressed_block_type(&self) -> CompressedBlockType {
        CompressedBlockType::Extended
    }
}
