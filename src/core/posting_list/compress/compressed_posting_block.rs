use enum_dispatch::enum_dispatch;

use crate::{
    core::{QuantizedWeight, COMPRESSION_BLOCK_SIZE},
    RowId,
};

#[enum_dispatch]
pub trait CompressedPostingBlockTrait<TW: QuantizedWeight> {
    fn row_id_start(&self) -> RowId;
    fn block_offset(&self) -> u64;
    fn row_ids_compressed_size(&self) -> u16;
    fn row_ids_count(&self) -> u8;
    fn num_bits(&self) -> u8;
    fn weights(&self) -> [TW; COMPRESSION_BLOCK_SIZE];
    fn max_next_weights(&self) -> [TW; COMPRESSION_BLOCK_SIZE];
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimpleCompressedPostingBlock<TW: QuantizedWeight> {
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

impl<TW: QuantizedWeight> CompressedPostingBlockTrait<TW> for SimpleCompressedPostingBlock<TW> {
    fn row_id_start(&self) -> RowId {
        self.row_id_start
    }

    fn block_offset(&self) -> u64 {
        self.block_offset
    }

    fn row_ids_compressed_size(&self) -> u16 {
        self.row_ids_compressed_size
    }

    fn row_ids_count(&self) -> u8 {
        self.row_ids_count
    }

    fn num_bits(&self) -> u8 {
        self.num_bits
    }

    fn weights(&self) -> [TW; COMPRESSION_BLOCK_SIZE] {
        self.weights
    }

    fn max_next_weights(&self) -> [TW; COMPRESSION_BLOCK_SIZE] {
        panic!("Not supported!")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExtendedCompressedPostingBlock<TW: QuantizedWeight> {
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

impl<TW: QuantizedWeight> CompressedPostingBlockTrait<TW> for ExtendedCompressedPostingBlock<TW> {
    fn row_id_start(&self) -> RowId {
        self.row_id_start
    }

    fn block_offset(&self) -> u64 {
        self.block_offset
    }

    fn row_ids_compressed_size(&self) -> u16 {
        self.row_ids_compressed_size
    }

    fn row_ids_count(&self) -> u8 {
        self.row_ids_count
    }

    fn num_bits(&self) -> u8 {
        self.num_bits
    }

    fn weights(&self) -> [TW; COMPRESSION_BLOCK_SIZE] {
        self.weights
    }

    fn max_next_weights(&self) -> [TW; COMPRESSION_BLOCK_SIZE] {
        self.max_next_weights
    }
}


#[derive(Debug, Clone, PartialEq)]
pub enum CompressedBlockType {
    Simple,
    Extended,
}


#[derive(Debug, Clone, PartialEq)]
#[enum_dispatch(CompressedPostingBlockTrait<TW>)]
pub enum GenericCompressedPostingBlock<TW: QuantizedWeight> {
    Simple(SimpleCompressedPostingBlock<TW>),
    Extended(ExtendedCompressedPostingBlock<TW>),
}

impl<TW: QuantizedWeight> GenericCompressedPostingBlock<TW> {
    pub fn block_type(&self) -> CompressedBlockType {
        match self {
            GenericCompressedPostingBlock::Simple(_) => CompressedBlockType::Simple,
            GenericCompressedPostingBlock::Extended(_) => CompressedBlockType::Extended,
        }
    }
}