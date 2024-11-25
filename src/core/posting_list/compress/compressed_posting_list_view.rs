use std::mem::size_of;

use crate::{
    core::{
        posting_list::encoder::VIntDecoder, BlockDecoder, QuantizedParam, QuantizedWeight,
        COMPRESSION_BLOCK_SIZE,
    },
    RowId,
};

use super::{CompressedPostingBlock, CompressedPostingList};

#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingListView<'a, TW: QuantizedWeight> {
    pub row_ids_compressed: &'a [u8],
    pub blocks: &'a [CompressedPostingBlock<TW>],
    pub quantization_params: Option<QuantizedParam>,
    pub row_ids_count: RowId,
    pub max_row_id: Option<RowId>,
}

impl<'a, TW: QuantizedWeight> CompressedPostingListView<'a, TW> {
    pub fn new(
        row_ids_compressed: &'a [u8],
        blocks: &'a [CompressedPostingBlock<TW>],
        quantized_params: Option<QuantizedParam>,
        row_ids_count: RowId,
        max_row_id: Option<RowId>,
    ) -> Self {
        Self {
            row_ids_compressed,
            blocks,
            quantization_params: quantized_params,
            row_ids_count,
            max_row_id,
        }
    }

    pub fn parts(&self) -> (&'a [u8], &'a [CompressedPostingBlock<TW>]) {
        (self.row_ids_compressed, self.blocks)
    }

    pub fn last_id(&self) -> Option<RowId> {
        self.max_row_id
    }

    // TODO: 这些。内部元素（id_data, chunks）在 to_vec 之后所有权怎么变化？当前 self 自己的所有权如何变化？
    pub fn to_owned(&self) -> CompressedPostingList<TW> {
        CompressedPostingList {
            row_ids_compressed: self.row_ids_compressed.to_vec(),
            blocks: self.blocks.to_vec(),
            quantization_params: self.quantization_params,
            row_ids_count: self.row_ids_count,
            max_row_id: self.max_row_id,
        }
    }

    pub fn len(&self) -> usize {
        self.row_ids_count as usize
    }

    pub fn uncompress_block(
        &self,
        block_idx: usize,
        decoder: &mut BlockDecoder,
        row_ids_uncompressed_in_block: &mut Vec<RowId>,
    ) {
        if block_idx >= self.blocks.len() {
            panic!("block idx is overflow.");
        }

        let block: &CompressedPostingBlock<TW> = &self.blocks[block_idx];

        let block_offset_start = block.block_offset as usize;
        let block_offset_end = (block.block_offset + block.row_ids_compressed_size as u64) as usize;
        let row_ids_compressed_in_block: &[u8] =
            &self.row_ids_compressed[block_offset_start..block_offset_end];

        let offset = block.row_id_start.checked_sub(1).unwrap_or(0);
        row_ids_uncompressed_in_block.clear();

        if block.row_ids_count as usize == COMPRESSION_BLOCK_SIZE {
            let consumed_bytes: usize = decoder.uncompress_block_sorted(
                row_ids_compressed_in_block,
                offset,
                block.num_bits,
                true,
            );
            assert_eq!(consumed_bytes, block.row_ids_compressed_size as usize);
            let res: &[u32; COMPRESSION_BLOCK_SIZE] = decoder.full_output();

            row_ids_uncompressed_in_block.reserve(COMPRESSION_BLOCK_SIZE);
            row_ids_uncompressed_in_block.extend_from_slice(res);
        } else {
            let consumed_bytes: usize = decoder.uncompress_vint_sorted(
                row_ids_compressed_in_block,
                block.row_id_start.checked_sub(1).unwrap_or(0),
                block.row_ids_count as usize,
                RowId::MAX,
            );
            assert_eq!(consumed_bytes, block.row_ids_compressed_size as usize);
            let res: &[u32] = &decoder.output_array()[0..decoder.output_len];

            row_ids_uncompressed_in_block.reserve(res.len());
            row_ids_uncompressed_in_block.extend_from_slice(res);
        }
    }

    // TODO 将 storage_size 写到 PostingListTrait 里面, 要求所有的 Posting 类型都应该实现这个接口，单位 B
    pub fn total_storage_size(&self) -> usize {
        let total = self.row_ids_compressed.len() * size_of::<u8>() +  // row_id_compressed 
            self.blocks.len() * size_of::<CompressedPostingBlock<TW>>() +    // total posting blocks
            size_of::<RowId>() +    // val: row_ids_count
            size_of::<RowId>(); // val: max_row_id
        return total;
    }

    // storage_size 可以通过自定义闭包获取相关属性的字节数
    fn storage_size<F>(&self, calculator: F) -> usize
    where
        F: FnOnce(&Self) -> usize,
    {
        calculator(self)
    }

    pub fn blocks_storage_size(&self) -> usize {
        self.storage_size(|e| e.blocks.len() * size_of::<CompressedPostingBlock<TW>>())
    }

    pub fn row_ids_storage_size(&self) -> usize {
        self.storage_size(|e| e.row_ids_compressed.len() * size_of::<u8>())
    }
}
