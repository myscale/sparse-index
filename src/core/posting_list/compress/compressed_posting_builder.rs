use super::CompressedPostingList;
use crate::{
    core::{
        posting_list::{compress::CompressedPostingBlock, encoder::VIntEncoder},
        BlockEncoder, DimWeight, ExtendedElement, PostingList, QuantizedParam, QuantizedWeight,
        WeightType, COMPRESSION_BLOCK_SIZE, DEFAULT_MAX_NEXT_WEIGHT,
    },
    RowId,
};
use itertools::Itertools;
use log::{error, info, warn};
use std::{cmp::max, marker::PhantomData, mem::size_of};

pub struct CompressedPostingBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    posting: PostingList<OW>,

    propagate_while_upserting: bool,
    finally_sort: bool,
    finally_propagate: bool,

    _ow: PhantomData<OW>,
    _tw: PhantomData<TW>,
}

// TODO: Find some third-party dependency to simplify the builder pattern.
// Builder pattern
impl<OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingBuilder<OW, TW> {
    pub fn new() -> Self {
        Self {
            posting: PostingList::new(),
            propagate_while_upserting: false,
            finally_sort: false,
            finally_propagate: true,
            _ow: PhantomData,
            _tw: PhantomData,
        }
    }

    pub fn with_finally_sort(mut self, sort: bool) -> Self {
        self.finally_sort = sort;
        self
    }

    pub fn with_finally_propagate(mut self, propagate: bool) -> Self {
        self.finally_propagate = propagate;
        self
    }

    pub fn with_propagate_while_upserting(mut self, propagate: bool) -> Self {
        self.propagate_while_upserting = propagate;
        self
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingBuilder<OW, TW> {
    /// ## brief
    /// add a new Element to the posting list.
    /// ## return
    /// bool: `ture` means the `insert` operation, `false` means `update`.
    pub fn add(&mut self, row_id: RowId, weight: DimWeight) -> bool {
        if self.propagate_while_upserting {
            self.posting.upsert_with_propagate(ExtendedElement::new(row_id, weight))
        } else {
            self.posting.upsert(ExtendedElement::new(row_id, weight)).1
        }
    }

    /// ## brief
    /// retrun all elements in the posting storage size.
    pub fn memory_usage(&self) -> usize {
        self.posting.len() * size_of::<ExtendedElement<OW>>()
    }

    pub fn pre_build(
        mut self,
    ) -> (
        Vec<u8>,                         // row_ids_compressed
        Vec<CompressedPostingBlock<TW>>, // quantized_blocks may not been quantized.
        Option<QuantizedParam>,          // quantized_param
        RowId,
        Option<RowId>,
    ) {
        let need_quantized =
            TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;

        // sort by row_id.
        if self.finally_sort {
            self.posting.elements.sort_unstable_by_key(|e| e.row_id);
        }
        #[cfg(debug_assertions)]
        {
            if let Some(res) = self.posting.elements.windows(2).find(|e| e[0].row_id >= e[1].row_id)
            {
                let error_msg = format!("Duplicated row_id, or Posting is not sorted by row_id correctly, left: {:?}, right: {:?}.", res[0], res[1]);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }

        // update max_next_weight and quantization_params
        let mut quantized_param: Option<QuantizedParam> = None;
        if self.finally_propagate {
            let mut max_next_weight = OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT);
            let mut min_weight = OW::from_f32(DimWeight::MAX);
            let mut max_weight = OW::from_f32(DimWeight::MIN);

            for element in self.posting.elements.iter_mut().rev() {
                element.max_next_weight = max_next_weight;
                max_next_weight = OW::max(max_next_weight, element.weight);

                min_weight = OW::min(min_weight, element.weight);
                max_weight = OW::max(max_weight, element.weight);
            }
            if need_quantized {
                quantized_param = Some(OW::gen_quantized_param(min_weight, max_weight));
            }
        } else {
            warn!("Skip propagating the Posting finally, please make sure it has already been propagated.");
            if need_quantized {
                // Only execute iteration when using quantized.
                let elements_iter = self.posting.elements.iter().map(|e| e.weight);
                let (min, max) = match elements_iter.minmax() {
                    itertools::MinMaxResult::NoElements => (OW::MINIMUM(), OW::MINIMUM()),
                    itertools::MinMaxResult::OneElement(e) => (e, e),
                    itertools::MinMaxResult::MinMax(min, max) => (min, max),
                };
                quantized_param = Some(OW::gen_quantized_param(min, max));
            }
        }

        let mut max_row_id: Option<RowId> = None;
        let mut total_row_ids_count: RowId = 0;

        let mut encoder = BlockEncoder::new();

        let mut row_ids_compressed_in_posting: Vec<u8> =
            Vec::with_capacity(self.posting.len() / COMPRESSION_BLOCK_SIZE);

        // Record all blocks data in posting
        let mut target_posting_blocks: Vec<CompressedPostingBlock<TW>> =
            Vec::with_capacity(self.posting.len() / COMPRESSION_BLOCK_SIZE + 1);

        let mut block_offsets: u64 = 0;
        for current_block in self.posting.elements.chunks(COMPRESSION_BLOCK_SIZE) {
            // Get current block's uncompressed u32 type row_ids.
            let row_ids_uncompressed_in_block: Vec<RowId> =
                current_block.iter().map(|e| e.row_id).collect::<Vec<RowId>>();
            let row_id_start_in_block: u32 = row_ids_uncompressed_in_block[0];

            let offset = row_id_start_in_block.checked_sub(1).unwrap_or(0);

            let (num_bits, row_ids_compressed_in_block) =
                if current_block.len() == COMPRESSION_BLOCK_SIZE {
                    // Full block compression
                    encoder.compress_block_sorted(&row_ids_uncompressed_in_block, offset)
                } else {
                    (0, encoder.compress_vint_sorted(&row_ids_uncompressed_in_block, offset))
                };

            row_ids_compressed_in_posting.extend_from_slice(row_ids_compressed_in_block);

            total_row_ids_count = total_row_ids_count.saturating_add(current_block.len() as u32);
            // TODO: Refine code, although code execute here can make sure Posting not empty, but we should make code logic more clearly.
            max_row_id = Some(max(
                max_row_id.unwrap_or(0),
                current_block.last().map(|w| w.row_id).unwrap_or_default(),
            ));

            if need_quantized {
                let quantized_weights: [TW; COMPRESSION_BLOCK_SIZE] =
                    quantized_weights_for_block::<OW, TW, _>(current_block, quantized_param, |w| {
                        w.weight
                    });
                let quantized_max_next_weights: [TW; COMPRESSION_BLOCK_SIZE] =
                    quantized_weights_for_block::<OW, TW, _>(current_block, quantized_param, |w| {
                        w.max_next_weight
                    });

                let block: CompressedPostingBlock<TW> = CompressedPostingBlock {
                    row_id_start: row_id_start_in_block,
                    block_offset: block_offsets,
                    row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed 512.
                    row_ids_count: current_block.len() as u8, // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                    num_bits,
                    weights: quantized_weights,
                    max_next_weights: quantized_max_next_weights,
                };
                target_posting_blocks.push(block);
            } else {
                assert_eq!(OW::weight_type(), TW::weight_type());
                let weights_converted: [TW; COMPRESSION_BLOCK_SIZE] =
                    convert_weights_type_for_block::<OW, TW, _>(current_block, |w| w.weight);
                let max_next_weights_converted: [TW; COMPRESSION_BLOCK_SIZE] =
                    convert_weights_type_for_block::<OW, TW, _>(current_block, |w| {
                        w.max_next_weight
                    });
                let block: CompressedPostingBlock<TW> = CompressedPostingBlock::<TW> {
                    row_id_start: row_id_start_in_block,
                    block_offset: block_offsets,
                    row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed 512.
                    row_ids_count: current_block.len() as u8, // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                    num_bits,
                    weights: weights_converted,
                    max_next_weights: max_next_weights_converted,
                };
                target_posting_blocks.push(block);
            }
            block_offsets += row_ids_compressed_in_block.len() as u64;
        }

        return (
            row_ids_compressed_in_posting,
            target_posting_blocks,
            quantized_param,
            total_row_ids_count,
            max_row_id,
        );
    }

    pub fn build(self) -> CompressedPostingList<TW> {
        let (
            row_ids_compressed_in_posting,
            posting_blocks,
            quantized_param,
            total_row_ids_count,
            max_row_id,
        ) = self.pre_build();

        let compressed_posting: CompressedPostingList<TW> = CompressedPostingList::<TW> {
            row_ids_compressed: row_ids_compressed_in_posting,
            blocks: posting_blocks,
            quantization_params: quantized_param,
            row_ids_count: total_row_ids_count,
            max_row_id,
        };

        return compressed_posting;
    }
}

fn quantized_weights_for_block<
    OW: QuantizedWeight,
    TW: QuantizedWeight,
    F: Fn(&ExtendedElement<OW>) -> OW,
>(
    block: &[ExtendedElement<OW>],
    quantization_params: Option<QuantizedParam>,
    weight_selector: F,
) -> [TW; COMPRESSION_BLOCK_SIZE] {
    let quantized_weights: Vec<TW> = block
        .iter()
        .map(|e: &ExtendedElement<OW>| {
            TW::from_u8(OW::quantize_with_param(weight_selector(e), quantization_params.unwrap()))
        })
        .collect::<Vec<TW>>();
    if quantized_weights.len() > COMPRESSION_BLOCK_SIZE {
        panic!(
            "Expected at most {} elements in a single block, found {}",
            COMPRESSION_BLOCK_SIZE,
            quantized_weights.len()
        );
    }
    let mut quantized_weights_slice: [TW; COMPRESSION_BLOCK_SIZE] =
        [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE];
    quantized_weights_slice[..quantized_weights.len()].copy_from_slice(&quantized_weights);

    quantized_weights_slice
}

fn convert_weights_type_for_block<
    OW: QuantizedWeight,
    TW: QuantizedWeight,
    F: Fn(&ExtendedElement<OW>) -> OW,
>(
    block: &[ExtendedElement<OW>],
    weight_selector: F,
) -> [TW; COMPRESSION_BLOCK_SIZE] {
    let weights: Vec<TW> = block
        .iter()
        .map(|e: &ExtendedElement<OW>| TW::from_f32(OW::to_f32(weight_selector(e))))
        .collect::<Vec<TW>>();
    if weights.len() > COMPRESSION_BLOCK_SIZE {
        panic!(
            "Expected at most {} elements in a single block, found {}",
            COMPRESSION_BLOCK_SIZE,
            weights.len()
        );
    }
    let mut weights_slice: [TW; COMPRESSION_BLOCK_SIZE] = [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE];
    weights_slice[..weights.len()].copy_from_slice(&weights);

    weights_slice
}
