use std::mem::size_of;

use log::error;

use crate::{
    core::{posting_list::encoder::VIntDecoder, BlockDecoder, PostingListError, QuantizedParam, QuantizedWeight, COMPRESSION_BLOCK_SIZE},
    RowId,
};

use super::{CompressedBlockType, CompressedPostingList, ExtendedCompressedPostingBlock, SimpleCompressedPostingBlock};

#[derive(Default, Debug, Clone)]
pub struct CompressedPostingListView<'a, TW>
where
    TW: QuantizedWeight,
{
    pub row_ids_compressed: &'a [u8],
    pub simple_blocks: &'a [SimpleCompressedPostingBlock<TW>],
    pub extended_blocks: &'a [ExtendedCompressedPostingBlock<TW>],
    pub compressed_block_type: CompressedBlockType,
    pub quantization_params: Option<QuantizedParam>,
    pub row_ids_count: RowId,
    pub max_row_id: Option<RowId>,
}

#[allow(unused)]
impl<'a, TW> CompressedPostingListView<'a, TW>
where
    TW: QuantizedWeight,
{
    pub fn new(
        row_ids_compressed: &'a [u8],
        simple_blocks: &'a [SimpleCompressedPostingBlock<TW>],
        extended_blocks: &'a [ExtendedCompressedPostingBlock<TW>],
        compressed_block_type: CompressedBlockType,
        quantized_params: Option<QuantizedParam>,
        row_ids_count: RowId,
        max_row_id: Option<RowId>,
    ) -> Self {
        Self { row_ids_compressed, simple_blocks, extended_blocks, compressed_block_type, quantization_params: quantized_params, row_ids_count, max_row_id }
    }

    pub fn last_id(&self) -> Option<RowId> {
        self.max_row_id
    }

    pub fn to_owned(&self) -> CompressedPostingList<TW> {
        CompressedPostingList {
            row_ids_compressed: self.row_ids_compressed.to_vec(),
            simple_blocks: self.simple_blocks.to_vec(),
            extended_blocks: self.extended_blocks.to_vec(),
            compressed_block_type: self.compressed_block_type,
            quantization_params: self.quantization_params,
            row_ids_count: self.row_ids_count,
            max_row_id: self.max_row_id,
        }
    }

    pub fn len(&self) -> usize {
        self.row_ids_count as usize
    }

    fn inner_uncompress_block(
        &self,
        decoder: &mut BlockDecoder,
        row_ids_uncompressed_in_block: &mut Vec<RowId>,
        row_ids_offset_start: usize,  // Current block's left offset in [`self.row_ids_compressed`]
        row_ids_offset_end: usize,    // Current block's right offset in [`self.row_ids_compressed`]
        row_ids_count: u8,            // How many row_ids (elements) in current block.
        row_id_start: RowId,          // The smallest row_id in current block.
        num_bits: u8,                 // Useful for current block uncompress, each block's `num_bits` may not same.
        row_ids_compressed_size: u16, // We record `row_ids` data_size of all blocks
    ) -> Result<(), PostingListError> {
        // We will uncompress bytes in [`row_ids_compressed_in_block`] into [`row_ids_uncompressed_in_block`]
        let row_ids_compressed_in_block: &[u8] = &self.row_ids_compressed[row_ids_offset_start..row_ids_offset_end];

        row_ids_uncompressed_in_block.clear();

        if row_ids_count as usize == COMPRESSION_BLOCK_SIZE {
            let consumed_bytes: usize = decoder.uncompress_block_sorted(row_ids_compressed_in_block, row_id_start.checked_sub(1).unwrap_or(0), num_bits, true);

            if consumed_bytes != row_ids_compressed_size as usize {
                let error_msg =
                    format!("During block uncompressing-block-sorted, `consumed_bytes`:{} not equal with `row_ids_compressed_size`:{}", consumed_bytes, row_ids_compressed_size);
                error!("{}", error_msg);
                return Err(PostingListError::UncompressError(error_msg));
            }
            let res: &[u32; COMPRESSION_BLOCK_SIZE] = decoder.full_output();

            row_ids_uncompressed_in_block.reserve(COMPRESSION_BLOCK_SIZE);
            row_ids_uncompressed_in_block.extend_from_slice(res);
        } else {
            let consumed_bytes: usize = decoder.uncompress_vint_sorted(row_ids_compressed_in_block, row_id_start.checked_sub(1).unwrap_or(0), row_ids_count as usize, RowId::MAX);

            if consumed_bytes != row_ids_compressed_size as usize {
                let error_msg =
                    format!("During block uncompressing-vint-sorted, `consumed_bytes`:{} not equal with `row_ids_compressed_size`:{}", consumed_bytes, row_ids_compressed_size);
                error!("{}", error_msg);
                return Err(PostingListError::UncompressError(error_msg));
            }
            let res: &[u32] = &decoder.output_array()[0..decoder.output_len];

            row_ids_uncompressed_in_block.reserve(res.len());
            row_ids_uncompressed_in_block.extend_from_slice(res);
        }
        Ok(())
    }

    pub fn uncompress_block(
        &self,
        block_type: CompressedBlockType,
        block_idx: usize,
        decoder: &mut BlockDecoder,
        row_ids_uncompressed_in_block: &mut Vec<RowId>,
    ) -> Result<(), PostingListError> {
        match block_type {
            CompressedBlockType::Simple => {
                // Boundary.
                if block_idx >= self.simple_blocks.len() {
                    let error_msg = format!(
                        "Can't uncompress SimpleBlock for `CompressedPostingList`, `block_idx` is overflow, [simple:{}, extended:{}]",
                        self.simple_blocks.len(),
                        self.extended_blocks.len()
                    );
                    error!("{}", error_msg);
                    return Err(PostingListError::UncompressError(error_msg));
                }
                // uncompress simple block
                let simple_block_ref: &SimpleCompressedPostingBlock<TW> = &self.simple_blocks[block_idx];

                let block_offset_start = simple_block_ref.block_offset as usize;
                let block_offset_end = (simple_block_ref.block_offset + simple_block_ref.row_ids_compressed_size as u64) as usize;

                self.inner_uncompress_block(
                    decoder,
                    row_ids_uncompressed_in_block,
                    block_offset_start,
                    block_offset_end,
                    simple_block_ref.row_ids_count,
                    simple_block_ref.row_id_start,
                    simple_block_ref.num_bits,
                    simple_block_ref.row_ids_compressed_size,
                )?;
            }
            CompressedBlockType::Extended => {
                // Boundary.
                if block_idx >= self.extended_blocks.len() {
                    let error_msg = format!(
                        "Can't uncompress ExtendedBlock for `CompressedPostingList`, `block_idx` is overflow, [simple:{}, extended:{}]",
                        self.simple_blocks.len(),
                        self.extended_blocks.len()
                    );
                    error!("{}", error_msg);
                    return Err(PostingListError::UncompressError(error_msg));
                }
                // uncompress extended block
                let extended_block_ref: &ExtendedCompressedPostingBlock<TW> = &self.extended_blocks[block_idx];

                let block_offset_start = extended_block_ref.block_offset as usize;
                let block_offset_end = (extended_block_ref.block_offset + extended_block_ref.row_ids_compressed_size as u64) as usize;

                self.inner_uncompress_block(
                    decoder,
                    row_ids_uncompressed_in_block,
                    block_offset_start,
                    block_offset_end,
                    extended_block_ref.row_ids_count,
                    extended_block_ref.row_id_start,
                    extended_block_ref.num_bits,
                    extended_block_ref.row_ids_compressed_size,
                )?;
            }
        }
        return Ok(());
    }

    fn storage_size<F>(&self, calculator: F) -> usize
    where
        F: FnOnce(&Self) -> usize,
    {
        calculator(self)
    }

    pub fn blocks_storage_size(&self) -> usize {
        self.storage_size(|e| match e.compressed_block_type {
            CompressedBlockType::Simple => e.simple_blocks.len() * size_of::<SimpleCompressedPostingBlock<TW>>(),
            CompressedBlockType::Extended => e.extended_blocks.len() * size_of::<ExtendedCompressedPostingBlock<TW>>(),
        })
    }

    pub fn row_ids_storage_size(&self) -> usize {
        self.storage_size(|e| e.row_ids_compressed.len() * size_of::<u8>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_clone() {
        let row_ids_compressed: &[u8] = &vec![1, 2, 3];
        let simple_blocks: &[SimpleCompressedPostingBlock<f32>] = &[];
        let extended_blocks: &[ExtendedCompressedPostingBlock<f32>] = &[];

        let original: CompressedPostingListView<'_, f32> = CompressedPostingListView {
            row_ids_compressed,
            simple_blocks,
            extended_blocks,
            compressed_block_type: CompressedBlockType::Simple,
            quantization_params: None,
            row_ids_count: 3,
            max_row_id: None,
        };

        let cloned = original.clone();

        // Ensure that both the original and cloned views point to the same memory locations for their data.
        assert!(std::ptr::addr_eq(original.row_ids_compressed as *const _, cloned.row_ids_compressed as *const _));
        assert!(std::ptr::addr_eq(original.simple_blocks as *const _, cloned.simple_blocks as *const _));
        assert!(std::ptr::addr_eq(original.extended_blocks as *const _, cloned.extended_blocks as *const _));
    }

    // TODO 测试 uncompress 逻辑是否正确
}
