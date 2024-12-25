use std::mem::size_of;

use log::error;

use crate::{
    core::{posting_list::encoder::VIntDecoder, BlockDecoder, QuantizedParam, QuantizedWeight, COMPRESSION_BLOCK_SIZE},
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

    // pub fn parts(&self) -> (&'a [u8], &'a [GenericCompressedPostingBlock<TW>]) {
    //     (self.row_ids_compressed, self.blocks)
    // }

    pub fn last_id(&self) -> Option<RowId> {
        self.max_row_id
    }

    // TODO: Figure out the ownership transfer after calling `to_vec()`, also think about `self` ownership
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

    pub fn uncompress_simple_block(&self, block_idx: usize, decoder: &mut BlockDecoder, row_ids_uncompressed_in_block: &mut Vec<RowId>) {
        // Boundary.
        if block_idx >= self.simple_blocks.len() {
            let error_msg = format!(
                "Can't uncompress {:?} block, idx boundary is out of [simple:{}, extended:{}]",
                self.compressed_block_type,
                self.simple_blocks.len(),
                self.extended_blocks.len()
            );
            error!("{}", error_msg);
            panic!("{}", error_msg);
        }

        let simple_block_ref = &self.simple_blocks[block_idx];

        let block_offset_start = simple_block_ref.block_offset as usize;
        let block_offset_end = (simple_block_ref.block_offset + simple_block_ref.row_ids_compressed_size as u64) as usize;
        let row_ids_compressed_in_block: &[u8] = &self.row_ids_compressed[block_offset_start..block_offset_end];

        row_ids_uncompressed_in_block.clear();

        if simple_block_ref.row_ids_count as usize == COMPRESSION_BLOCK_SIZE {
            let consumed_bytes: usize =
                decoder.uncompress_block_sorted(row_ids_compressed_in_block, simple_block_ref.row_id_start.checked_sub(1).unwrap_or(0), simple_block_ref.num_bits, true);
            if consumed_bytes != simple_block_ref.row_ids_compressed_size as usize {
                let error_msg = format!(
                    "During block uncompressing simple block (a complete `COMPRESSION_BLOCK_SIZE`), consumed_bytes:{} not equal with row_ids_compressed_size:{}",
                    consumed_bytes, simple_block_ref.row_ids_compressed_size as usize
                );
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
            let res: &[u32; COMPRESSION_BLOCK_SIZE] = decoder.full_output();

            row_ids_uncompressed_in_block.reserve(COMPRESSION_BLOCK_SIZE);
            row_ids_uncompressed_in_block.extend_from_slice(res);
        } else {
            let consumed_bytes: usize = decoder.uncompress_vint_sorted(
                row_ids_compressed_in_block,
                simple_block_ref.row_id_start.checked_sub(1).unwrap_or(0),
                simple_block_ref.row_ids_count as usize,
                RowId::MAX,
            );
            if consumed_bytes != simple_block_ref.row_ids_compressed_size as usize {
                let error_msg = format!(
                    "During block uncompressing simple block (incomplete COMPRESSION_BLOCK_SIZE``), consumed_bytes:{} not equal with row_ids_compressed_size:{}",
                    consumed_bytes, simple_block_ref.row_ids_compressed_size as usize
                );
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
            let res: &[u32] = &decoder.output_array()[0..decoder.output_len];

            row_ids_uncompressed_in_block.reserve(res.len());
            row_ids_uncompressed_in_block.extend_from_slice(res);
        }
    }

    pub fn uncompress_extended_block(&self, block_idx: usize, decoder: &mut BlockDecoder, row_ids_uncompressed_in_block: &mut Vec<RowId>) {
        // Boundary.
        if block_idx >= self.extended_blocks.len() {
            let error_msg = format!(
                "Can't uncompress {:?} block, idx boundary is out of [simple:{}, extended:{}]",
                self.compressed_block_type,
                self.simple_blocks.len(),
                self.extended_blocks.len()
            );
            error!("{}", error_msg);
            panic!("{}", error_msg);
        }

        let extended_block_ref = &self.extended_blocks[block_idx];

        let block_offset_start = extended_block_ref.block_offset as usize;
        let block_offset_end = (extended_block_ref.block_offset + extended_block_ref.row_ids_compressed_size as u64) as usize;
        let row_ids_compressed_in_block: &[u8] = &self.row_ids_compressed[block_offset_start..block_offset_end];

        row_ids_uncompressed_in_block.clear();

        if extended_block_ref.row_ids_count as usize == COMPRESSION_BLOCK_SIZE {
            let consumed_bytes: usize =
                decoder.uncompress_block_sorted(row_ids_compressed_in_block, extended_block_ref.row_id_start.checked_sub(1).unwrap_or(0), extended_block_ref.num_bits, true);
            if consumed_bytes != extended_block_ref.row_ids_compressed_size as usize {
                let error_msg = format!(
                    "During block uncompressing extended block (a complete `COMPRESSION_BLOCK_SIZE`), consumed_bytes:{} not equal with row_ids_compressed_size:{}",
                    consumed_bytes, extended_block_ref.row_ids_compressed_size as usize
                );
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
            let res: &[u32; COMPRESSION_BLOCK_SIZE] = decoder.full_output();

            row_ids_uncompressed_in_block.reserve(COMPRESSION_BLOCK_SIZE);
            row_ids_uncompressed_in_block.extend_from_slice(res);
        } else {
            let consumed_bytes: usize = decoder.uncompress_vint_sorted(
                row_ids_compressed_in_block,
                extended_block_ref.row_id_start.checked_sub(1).unwrap_or(0),
                extended_block_ref.row_ids_count as usize,
                RowId::MAX,
            );
            if consumed_bytes != extended_block_ref.row_ids_compressed_size as usize {
                let error_msg = format!(
                    "During block uncompressing extended block (incomplete COMPRESSION_BLOCK_SIZE``), consumed_bytes:{} not equal with row_ids_compressed_size:{}",
                    consumed_bytes, extended_block_ref.row_ids_compressed_size as usize
                );
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
            let res: &[u32] = &decoder.output_array()[0..decoder.output_len];

            row_ids_uncompressed_in_block.reserve(res.len());
            row_ids_uncompressed_in_block.extend_from_slice(res);
        }
    }

    // TODO: refine code, add this func into trait.
    // pub fn total_storage_size(&self) -> usize {

    //     let blocks_size = match self.compressed_block_type {
    //         CompressedBlockType::Simple => self.generic_blocks.len() * size_of::<SimpleCompressedPostingBlock<TW>>(),
    //         CompressedBlockType::Extended => self.generic_blocks.len() * size_of::<ExtendedCompressedPostingBlock<TW>>(),
    //     };

    //     let total =
    //         self.row_ids_compressed.len() * size_of::<u8>() +  // row_id_compressed
    //         blocks_size +                                      // total posting blocks
    //         size_of::<CompressedBlockType>() +
    //         size_of::<Option<QuantizedParam>>() +
    //         size_of::<RowId>() +                               // val: row_ids_count
    //         size_of::<RowId>();                                // val: max_row_id
    //     return total;
    // }

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
