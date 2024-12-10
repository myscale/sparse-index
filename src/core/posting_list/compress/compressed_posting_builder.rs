use super::{CompressedBlockType, CompressedPostingList, ExtendedCompressedPostingBlock, GenericCompressedPostingBlock, SimpleCompressedPostingBlock};
use crate::{
    core::{
        posting_list::encoder::VIntEncoder, BlockEncoder, DimWeight, Element, ElementType, ExtendedElement, GenericElement, PostingList, QuantizedParam, QuantizedWeight, SimpleElement, WeightType, COMPRESSION_BLOCK_SIZE, DEFAULT_MAX_NEXT_WEIGHT
    },
    RowId,
};
use itertools::Itertools;
use log::error;
use typed_builder::TypedBuilder;
use std::{cmp::max, marker::PhantomData, mem::size_of};


#[derive(TypedBuilder)]
pub struct CompressedPostingBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    /// [`CompressedPostingBuilder`] will operate inner [`PostingList`]
    #[builder(default=PostingList::<OW>::new(ElementType::SIMPLE))]
    posting: PostingList<OW>,

    /// Element type in [`PostingList`]
    #[builder(default = ElementType::SIMPLE)]
    element_type: ElementType,

    /// Whether need quantize weight in [`PostingList`]
    #[builder(default = false)]
    need_quantized: bool,

    /// This switch is supported when the element type is [`ElementType::EXTENDED`].
    #[builder(default=false)]
    propagate_while_upserting: bool,

    /// Whether need sort the whole [`PostingList`] when finally build.
    #[builder(default=false)]
    finally_sort: bool,

    /// This switch is supported when the element type is [`ElementType::EXTENDED`].
    /// It is conflict with switcher [`propagate_while_upserting`]
    #[builder(default=false)]
    finally_propagate: bool,

    _phantom_tw: PhantomData<TW>,
}

// TODO: Find some third-party dependency to simplify the builder pattern.
// Builder pattern
impl<OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingBuilder<OW, TW> {
    pub fn new(element_type: ElementType, finally_sort: bool, propagate_while_upserting: bool) -> Self {
        // If we need quantize weight.
        let need_quantized = TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if !need_quantized {
            assert_eq!(TW::weight_type(), OW::weight_type());
        }

        // only simple element support quantized.
        // quantize extended element will lead max_next_weight nonsense.
        if need_quantized && element_type==ElementType::EXTENDED {
            let error_msg = format!("extended element not supported be quantized.");
            error!("{}", error_msg);
            panic!("{}", error_msg);
        }

        Self::builder()
            .posting(PostingList::<OW>::new(element_type))
            .element_type(element_type)
            .need_quantized(need_quantized)
            .propagate_while_upserting(element_type == ElementType::EXTENDED && propagate_while_upserting)
            .finally_sort(finally_sort)
            .finally_propagate(element_type == ElementType::EXTENDED && !propagate_while_upserting)
            ._phantom_tw(PhantomData)
            .build()
    }

    pub fn default() -> Self {
        Self::new(ElementType::SIMPLE, false, false)
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingBuilder<OW, TW> {
    /// ## brief
    /// add a new Element to the posting list.
    /// ## return
    /// bool: `ture` means the `insert` operation, `false` means `update`.
    pub fn add(&mut self, row_id: RowId, weight: DimWeight) -> bool {
        let generic_element: GenericElement<OW> = match self.element_type {
            ElementType::SIMPLE => {
                SimpleElement::<OW>::new(row_id, weight).into()
            }
            ElementType::EXTENDED => {
                ExtendedElement::<OW>::new(row_id, weight).into()
            }
            _ => panic!("Not supported element type, this panic should not happen."),
        };

        if self.propagate_while_upserting {
            self.posting.upsert_with_propagate(generic_element)
        } else {
            self.posting.upsert(generic_element).1
        }
    }

    /// ## brief
    /// retrun all elements in the posting storage size.
    pub fn memory_usage(&self) -> usize {
        let actual_memory_usage = self.posting.len() * size_of::<GenericElement<OW>>();
        let inner_memory_usage = match self.element_type {
            ElementType::SIMPLE => {
                self.posting.len() * size_of::<SimpleElement<OW>>()
            }
            ElementType::EXTENDED => {
                self.posting.len() * size_of::<ExtendedElement<OW>>()
            }
            _ => panic!("Not supported element type, this panic should not happen."),
        };
        (actual_memory_usage, inner_memory_usage)
    }

    fn execute_finally_propagate(&mut self) -> Option<QuantizedParam> {
        // boundary
        assert!(self.element_type==ElementType::EXTENDED);

        if self.posting.elements.len()==0 && self.need_quantized {
            return Some(QuantizedParam::default());
        }

        let mut max_next_weight: OW = OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT);
        let mut min_weight = self.posting.elements.last().unwrap().weight();
        let mut max_weight = min_weight.clone();

        // reverse iter, update max_next_weight for element_ex.
        for element in self.posting.elements.iter_mut().rev() {
            element.update_max_next_weight(max_next_weight);
            max_next_weight = max_next_weight.max(element.weight());

            if self.need_quantized {
                min_weight = OW::min(min_weight, element.weight());
                max_weight = OW::max(max_weight, element.weight());
            }
        }
        if self.need_quantized {Some(OW::gen_quantized_param(min_weight, max_weight))} else {None}
    }

    // TODO 将这个 sort、propagate、quantized 的逻辑抽取出来

    pub fn pre_build(
        mut self,
    ) -> (
        Vec<u8>,                                // row_ids_compressed
        Vec<GenericCompressedPostingBlock<TW>>, // quantized_blocks may not been quantized.
        Option<QuantizedParam>,                 // quantized_param
        RowId,
        Option<RowId>,
    ) {
        // 从这里开始 REFINE
        // sort by row_id.
        if self.finally_sort {
            self.posting.elements.sort_unstable_by_key(|e| e.row_id());
        }
        #[cfg(debug_assertions)]
        {
            if let Some(res) = self.posting.elements.windows(2).find(|e| e[0].row_id() >= e[1].row_id())
            {
                let error_msg = format!("Duplicated row_id, or Posting is not sorted by row_id correctly, left: {:?}, right: {:?}.", res[0], res[1]);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }

        // TODO：从这里开始继续 refine, 并修改 index mmap、compressed index mmap
        // update max_next_weight and quantization_params
        let mut quantized_param: Option<QuantizedParam> = None;
        if self.finally_propagate {
            // We should ensure that only extended type can execute weight propagate.
            assert_eq!(self.element_type, ElementType::EXTENDED);

            quantized_param = self.execute_finally_propagate();

        } else {
            if self.need_quantized {
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
        // 到这里结束：之前的这段代码，可以考虑和 posting list builder 统一起来，使用一份代码

        let mut max_row_id: Option<RowId> = None;
        let mut total_row_ids_count: RowId = 0;

        let mut encoder = BlockEncoder::new();

        // 初始分配一定的 capacity，并不是一个最大上限
        let mut row_ids_compressed_in_posting: Vec<u8> = Vec::with_capacity(self.posting.len() / COMPRESSION_BLOCK_SIZE);

        // Record all blocks data in posting
        let mut target_posting_blocks: Vec<GenericCompressedPostingBlock<TW>> = Vec::with_capacity(self.posting.len() / COMPRESSION_BLOCK_SIZE + 1);

        let mut block_offsets: u64 = 0;
        for current_block in self.posting.elements.chunks(COMPRESSION_BLOCK_SIZE) {
            // Get current block's uncompressed u32 type row_ids.
            let row_ids_uncompressed_in_block: Vec<RowId> =
                current_block.iter().map(|e| e.row_id()).collect::<Vec<RowId>>();
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
                current_block.last().map(|w| w.row_id()).unwrap_or(0)
            ));

            if self.need_quantized {
                match CompressedBlockType::bound_by_element(self.element_type) {
                    CompressedBlockType::Simple => {
                        let block: GenericCompressedPostingBlock<TW> = GenericCompressedPostingBlock::Simple(SimpleCompressedPostingBlock {
                            row_id_start: row_id_start_in_block,
                            block_offset: block_offsets,
                            row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed 512.
                            row_ids_count: current_block.len() as u8, // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                            num_bits,
                            weights:  quantized_weights_for_block::<OW, TW, _>(current_block, quantized_param, |w| {w.weight}),
                        });
                        target_posting_blocks.push(block);
                    },
                    CompressedBlockType::Extended => {
                        let block: GenericCompressedPostingBlock<TW> = GenericCompressedPostingBlock::Extended(ExtendedCompressedPostingBlock {
                            row_id_start: row_id_start_in_block,
                            block_offset: block_offsets,
                            row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed 512.
                            row_ids_count: current_block.len() as u8, // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                            num_bits,
                            weights:  quantized_weights_for_block::<OW, TW, _>(current_block, quantized_param, |w| {w.weight()}),
                            max_next_weights: quantized_weights_for_block::<OW, TW, _>(current_block, quantized_param, |w| {w.max_next_weight()}),
                        });
                        target_posting_blocks.push(block);
                    },
                }
            } else {
                assert_eq!(OW::weight_type(), TW::weight_type());

                match CompressedBlockType::bound_by_element(self.element_type) {
                    CompressedBlockType::Simple => {
                        let block: GenericCompressedPostingBlock<TW> = GenericCompressedPostingBlock::Simple(SimpleCompressedPostingBlock {
                            row_id_start: row_id_start_in_block,
                            block_offset: block_offsets,
                            row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed 512.
                            row_ids_count: current_block.len() as u8, // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                            num_bits,
                            weights:  convert_weights_type_for_block::<OW, TW, _>(current_block, |w| w.weight()),
                        });
                        target_posting_blocks.push(block);
                    },
                    CompressedBlockType::Extended => {
                        let block: GenericCompressedPostingBlock<TW> = GenericCompressedPostingBlock::Extended(ExtendedCompressedPostingBlock {
                            row_id_start: row_id_start_in_block,
                            block_offset: block_offsets,
                            row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed 512.
                            row_ids_count: current_block.len() as u8, // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                            num_bits,
                            weights:  convert_weights_type_for_block::<OW, TW, _>(current_block, |w| w.weight()),
                            max_next_weights: convert_weights_type_for_block::<OW, TW, _>(current_block, |w| w.max_next_weight()),
                        });
                        target_posting_blocks.push(block);
                    },
                }
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
            generic_blocks: posting_blocks,
            compressed_block_type: CompressedBlockType::bound_by_element(self.element_type),
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
    F: Fn(&GenericElement<OW>) -> OW,
>(
    block: &[GenericElement<OW>],
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
    F: Fn(&GenericElement<OW>) -> OW,
>(
    block: &[GenericElement<OW>],
    weight_selector: F,
) -> [TW; COMPRESSION_BLOCK_SIZE] {
    let weights: Vec<TW> = block
        .iter()
        .map(|e: &GenericElement<OW>| {
            TW::from_f32(OW::to_f32(weight_selector(e)))
        })
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
