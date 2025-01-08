use crate::{
    core::{ElementType, QuantizedParam, QuantizedWeight, WeightType, COMPRESSION_BLOCK_SIZE},
    RowId,
};

#[derive(Default, Copy, Debug, Clone, PartialEq, Eq)]
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
    #[allow(unused)]
    fn compressed_block_type(&self) -> CompressedBlockType;

    #[allow(unused)]
    fn approximately_eq(&self, other: &Self, quantized_param: Option<QuantizedParam>) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

    fn approximately_eq(&self, other: &Self, quantized_param: Option<QuantizedParam>) -> bool {
        if quantized_param.is_none() {
            return self == other;
        } else {
            assert_eq!(TW::weight_type(), WeightType::WeightU8)
        }
        let left = Self { weights: [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE], ..self.clone() };
        let right = Self { weights: [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE], ..other.clone() };

        if left != right {
            return false;
        }

        let param = quantized_param.unwrap();
        for (&w1, &w2) in self.weights.iter().zip(&other.weights) {
            if param.approximately_eq(w1, w2) {
                continue;
            } else {
                return false;
            }
        }

        return true;
    }
}

impl<TW: QuantizedWeight> CompressedPostingBlock<TW> for ExtendedCompressedPostingBlock<TW> {
    fn compressed_block_type(&self) -> CompressedBlockType {
        CompressedBlockType::Extended
    }

    fn approximately_eq(&self, other: &Self, quantized_param: Option<QuantizedParam>) -> bool {
        if quantized_param.is_none() {
            return self == other;
        } else {
            assert_eq!(TW::weight_type(), WeightType::WeightU8)
        }
        let left = Self { weights: [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE], ..self.clone() };
        let right = Self { weights: [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE], ..other.clone() };

        if left != right {
            return false;
        }

        let param = quantized_param.unwrap();
        // for `weights` and `max_next_weights`.
        for idx in 0..COMPRESSION_BLOCK_SIZE {
            let w1 = TW::to_u8(self.weights[idx]);
            let w2 = TW::to_u8(other.weights[idx]);
            let mw1 = TW::to_u8(self.max_next_weights[idx]);
            let mw2 = TW::to_u8(other.max_next_weights[idx]);

            if TW::unquantize_with_param(w1, param.clone()) == TW::unquantize_with_param(w2, param.clone())
                && TW::unquantize_with_param(mw1, param.clone()) == TW::unquantize_with_param(mw2, param.clone())
            {
                continue;
            } else {
                return false;
            }
        }

        return true;
    }
}
