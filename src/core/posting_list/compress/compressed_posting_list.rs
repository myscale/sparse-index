use crate::{
    core::{QuantizedParam, QuantizedWeight},
    RowId,
};

use super::{CompressedBlockType, CompressedPostingListView, ExtendedCompressedPostingBlock, SimpleCompressedPostingBlock};

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct CompressedPostingList<TW>
where
    TW: QuantizedWeight,
{
    /// Compressed row_ids data, each block will has it's own offset in it.
    pub row_ids_compressed: Vec<u8>,

    /// Fixed-size blocks.
    pub simple_blocks: Vec<SimpleCompressedPostingBlock<TW>>,
    pub extended_blocks: Vec<ExtendedCompressedPostingBlock<TW>>,

    /// `compressed_block_type` in blocks.
    pub compressed_block_type: CompressedBlockType,

    /// Quantization parameters.
    pub quantization_params: Option<QuantizedParam>,

    /// Total row ids count.
    pub row_ids_count: RowId,

    /// Max row id
    pub max_row_id: Option<RowId>,
}

impl<TW> CompressedPostingList<TW>
where
    TW: QuantizedWeight,
{
    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.row_ids_count as usize
    }

    pub fn view(&self) -> CompressedPostingListView<TW> {
        CompressedPostingListView::new(
            &self.row_ids_compressed,
            &self.simple_blocks,
            &self.extended_blocks,
            self.compressed_block_type,
            self.quantization_params,
            self.row_ids_count,
            self.max_row_id,
        )
    }

    #[cfg(test)]
    pub fn approximately_eq(&self, other: &Self) -> bool {
        use super::CompressedPostingBlock;

        let left = Self { simple_blocks: vec![], extended_blocks: vec![], ..self.clone() };
        let right = Self { simple_blocks: vec![], extended_blocks: vec![], ..self.clone() };

        // compare fields without blocks.
        if left != right {
            return false;
        }

        // compare blocks.
        match self.compressed_block_type {
            CompressedBlockType::Simple => {
                let cur_blocks: &Vec<SimpleCompressedPostingBlock<TW>> = &self.simple_blocks;
                let other_blocks: &Vec<SimpleCompressedPostingBlock<TW>> = &other.simple_blocks;
                cur_blocks.iter().zip(other_blocks).all(|(left, right)| left.approximately_eq(right, self.quantization_params.clone()))
            }
            CompressedBlockType::Extended => {
                let cur_blocks: &Vec<ExtendedCompressedPostingBlock<TW>> = &self.extended_blocks;
                let other_blocks: &Vec<ExtendedCompressedPostingBlock<TW>> = &other.extended_blocks;
                cur_blocks.iter().zip(other_blocks).all(|(left, right)| left.approximately_eq(right, self.quantization_params.clone()))
            }
        }
    }
}
