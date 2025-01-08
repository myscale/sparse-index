use super::{CompressedBlockType, CompressedPostingList, ExtendedCompressedPostingBlock, SimpleCompressedPostingBlock};
use crate::{
    core::{
        posting_list::encoder::VIntEncoder, BlockEncoder, DimWeight, ElementRead, ElementType, ElementWrite, ExtendedElement, GenericElement, PostingList, PostingListError,
        QuantizedParam, QuantizedWeight, SimpleElement, WeightType, COMPRESSION_BLOCK_SIZE, DEFAULT_MAX_NEXT_WEIGHT,
    },
    RowId,
};
use itertools::Itertools;
use log::error;
use std::{cmp::max, marker::PhantomData, mem::size_of};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct CompressedPostingBuilder<OW, TW>
where
    OW: QuantizedWeight,
    TW: QuantizedWeight,
{
    /// [`CompressedPostingBuilder`] will operate inner [`PostingList`]
    #[builder(default=PostingList::<OW>::new(ElementType::SIMPLE))]
    pub(super) posting: PostingList<OW>,

    /// Element type in [`PostingList`]
    #[builder(default = ElementType::SIMPLE)]
    pub(super) element_type: ElementType,

    /// Whether need quantize weight in [`PostingList`]
    #[builder(default = false)]
    pub(super) need_quantized: bool,

    /// This switch is supported when the element type is [`ElementType::EXTENDED`].
    #[builder(default = false)]
    pub(super) propagate_while_upserting: bool,

    /// This switch is supported when the element type is [`ElementType::EXTENDED`].
    /// It is conflict with switcher [`propagate_while_upserting`]
    #[builder(default = false)]
    pub(super) finally_propagate: bool,

    _phantom_tw: PhantomData<TW>,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingBuilder<OW, TW> {
    pub fn new(element_type: ElementType, finally_propagate: bool, propagate_while_upserting: bool) -> Result<Self, PostingListError> {
        // If we need quantize weight.
        let need_quantized = TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if !need_quantized && TW::weight_type() != OW::weight_type() {
            let error_msg = "[CompressedPostingBuilder] WeightType should keep same, while quantized is disabled.";
            error!("{}", error_msg);
            return Err(PostingListError::InvalidParameter(error_msg.to_string()));
        }

        // Quantize ExtendedElement will lead `max_next_weight` nonsense.
        if need_quantized && element_type == ElementType::EXTENDED {
            let error_msg = "[CompressedPostingBuilder] ExtendedElement doesn't support to be quantized.";
            error!("{}", error_msg);
            return Err(PostingListError::InvalidParameter(error_msg.to_string()));
        }

        Ok(Self::builder()
            .posting(PostingList::<OW>::new(element_type))
            .element_type(element_type)
            .need_quantized(need_quantized)
            .propagate_while_upserting(element_type == ElementType::EXTENDED && propagate_while_upserting)
            .finally_propagate(element_type == ElementType::EXTENDED && !propagate_while_upserting && finally_propagate)
            ._phantom_tw(PhantomData)
            .build())
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingBuilder<OW, TW> {
    /// ## brief
    /// add a new Element to the posting list.
    /// ## return
    /// bool: `ture` means the `insert` operation, `false` means `update`.
    pub fn add(&mut self, row_id: RowId, weight: DimWeight) -> bool {
        let generic_element: GenericElement<OW> = match self.element_type {
            ElementType::SIMPLE => SimpleElement::<OW>::new(row_id, weight).into(),
            ElementType::EXTENDED => ExtendedElement::<OW>::new(row_id, weight).into(),
        };

        if self.propagate_while_upserting {
            self.posting.upsert_with_propagate(generic_element)
        } else {
            self.posting.upsert(generic_element).1
        }
    }

    /// ## brief
    /// retrun all elements in the posting storage size.
    #[allow(unused)]
    pub fn memory_usage(&self) -> (usize, usize) {
        let actual_memory_usage = self.posting.len() * size_of::<GenericElement<OW>>();
        let inner_memory_usage = match self.element_type {
            ElementType::SIMPLE => self.posting.len() * size_of::<SimpleElement<OW>>(),
            ElementType::EXTENDED => self.posting.len() * size_of::<ExtendedElement<OW>>(),
        };
        (actual_memory_usage, inner_memory_usage)
    }

    fn execute_finally_propagate(&mut self) -> Result<Option<QuantizedParam>, PostingListError> {
        // Boundary
        if self.element_type != ElementType::EXTENDED {
            let error_msg = "[CompressedPostingBuilder] Only `ExtendedElement` support propagate `max_next_weight`.";
            error!("{}", error_msg);
            return Err(PostingListError::InvalidParameter(error_msg.to_string()));
        }

        // Do nothing for
        if self.posting.elements.len() == 0 {
            if self.need_quantized {
                return Ok(Some(QuantizedParam::default()));
            } else {
                return Ok(None);
            }
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
        if self.need_quantized {
            Ok(Some(OW::gen_quantized_param(min_weight, max_weight)))
        } else {
            Ok(None)
        }
    }

    fn propagate_and_quantize(&mut self) -> Result<Option<QuantizedParam>, PostingListError> {
        #[cfg(debug_assertions)]
        {
            if let Some(res) = self.posting.elements.windows(2).find(|e| e[0].row_id() >= e[1].row_id()) {
                let error_msg = format!("Duplicated row_id, or Posting is not sorted by row_id correctly, left: {:?}, right: {:?}.", res[0], res[1]);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }

        let quantized_param: Option<QuantizedParam> = match self.finally_propagate {
            true => {
                // Boundary
                if self.element_type != ElementType::EXTENDED {
                    let error_msg = format!("Only `ExtendedElement` support propagate `max_next_weight`, but got {:?}", self.element_type);
                    error!("{}", error_msg);
                    return Err(PostingListError::InvalidParameter(error_msg));
                }

                self.execute_finally_propagate()?
            }
            false => {
                match self.need_quantized {
                    true => {
                        // Boundary
                        if self.element_type != ElementType::SIMPLE {
                            let error_msg = format!("Only `SimpleElement` support quantized, but got {:?}", self.element_type);
                            error!("{}", error_msg);
                            return Err(PostingListError::InvalidParameter(error_msg));
                        }
                        // Only execute iteration when using quantized.
                        let elements_iter = self.posting.elements.iter().map(|e| e.weight());
                        let (min, max) = match elements_iter.minmax() {
                            itertools::MinMaxResult::NoElements => (OW::MINIMUM(), OW::MINIMUM()),
                            itertools::MinMaxResult::OneElement(e) => (e, e),
                            itertools::MinMaxResult::MinMax(min, max) => (min, max),
                        };
                        Some(OW::gen_quantized_param(min, max))
                    }
                    false => None,
                }
            }
        };
        Ok(quantized_param)
    }

    fn compress_blocks(
        self,
        quantized_param: Option<QuantizedParam>,
    ) -> Result<
        (
            Vec<u8>,                                 // row_ids_compressed
            Vec<SimpleCompressedPostingBlock<TW>>,   // quantized_blocks may not been quantized.
            Vec<ExtendedCompressedPostingBlock<TW>>, // quantized_blocks may not been quantized.
            RowId,
            Option<RowId>,
        ),
        PostingListError,
    > {
        // Init mertics for posting list.
        let mut max_row_id: Option<RowId> = None;
        let mut total_row_ids_count: RowId = 0;

        // Init encoder for compressing row_ids.
        let mut encoder: BlockEncoder = BlockEncoder::new();

        // Init output `row_ids` compressed data for posting list.
        let mut output_row_ids_compressed_in_posting: Vec<u8> = Vec::with_capacity(self.posting.len() / COMPRESSION_BLOCK_SIZE);

        // Init output posting blocks for posting list.
        let mut output_simple_posting_blocks: Vec<SimpleCompressedPostingBlock<TW>> = match self.element_type {
            ElementType::SIMPLE => Vec::with_capacity(self.posting.len() / COMPRESSION_BLOCK_SIZE + 1),
            ElementType::EXTENDED => vec![],
        };
        let mut output_extended_posting_blocks: Vec<ExtendedCompressedPostingBlock<TW>> = match self.element_type {
            ElementType::SIMPLE => vec![],
            ElementType::EXTENDED => Vec::with_capacity(self.posting.len() / COMPRESSION_BLOCK_SIZE + 1),
        };

        // Init `block_offsets` in compressed row_ids for each 128-Block.
        // (While generating posting blocks, we can calculate the block offset in compressed row_ids.)
        let mut block_offsets: u64 = 0;

        // Chunk elements in posting list by 128(`COMPRESSION_BLOCK_SIZE`)
        for current_block in self.posting.elements.chunks(COMPRESSION_BLOCK_SIZE) {
            // Get current block's uncompressed u32 type row_ids.
            let row_ids_uncompressed_in_block: Vec<RowId> = current_block.iter().map(|e| e.row_id()).collect::<Vec<RowId>>();
            let row_id_start_in_block: u32 = row_ids_uncompressed_in_block[0];
            let offset = row_id_start_in_block.checked_sub(1).unwrap_or(0);

            // Compress current block's row_ids.
            let (num_bits, row_ids_compressed_in_block) = if current_block.len() == COMPRESSION_BLOCK_SIZE {
                // Full block compression
                encoder.compress_block_sorted(&row_ids_uncompressed_in_block, offset)
            } else {
                // Partial block compression (last block)
                (0, encoder.compress_vint_sorted(&row_ids_uncompressed_in_block, offset))
            };

            // Save compressed row_ids to output.
            output_row_ids_compressed_in_posting.extend_from_slice(row_ids_compressed_in_block);

            // Update metrics
            total_row_ids_count = total_row_ids_count.saturating_add(current_block.len() as u32);
            max_row_id = Some(max(max_row_id.unwrap_or(0), current_block.last().map(|w| w.row_id()).unwrap_or(0)));

            // Save compressed posting block to output.
            match self.need_quantized {
                true => {
                    match CompressedBlockType::from(self.element_type) {
                        CompressedBlockType::Simple => {
                            let block: SimpleCompressedPostingBlock<TW> = SimpleCompressedPostingBlock {
                                row_id_start: row_id_start_in_block,
                                block_offset: block_offsets,
                                row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed u16::max -> 65535.
                                row_ids_count: current_block.len() as u8,                          // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                                num_bits,
                                weights: quantized_weights_for_block::<OW, TW, _>(current_block, quantized_param, |w| w.weight())?,
                            };
                            output_simple_posting_blocks.push(block);
                        }
                        CompressedBlockType::Extended => {
                            let error_msg = "`ExtendedElement` shouldn't be quantized! This error happended during generate compressed blocks.";
                            error!("{}", error_msg);
                            return Err(PostingListError::InvalidParameter(error_msg.to_string()));
                        }
                    }
                }
                false => {
                    // Boundary.
                    if OW::weight_type() != TW::weight_type() {
                        let error_msg = format!(
                            "WeightType should keep same, while quantized is disabled. OW: {:?}, TW: {:?}.
                             This Error happended during generate compressed blocks.",
                            OW::weight_type(),
                            TW::weight_type()
                        );
                        error!("{}", error_msg);
                        return Err(PostingListError::InvalidParameter(error_msg.to_string()));
                    }

                    match CompressedBlockType::from(self.element_type) {
                        CompressedBlockType::Simple => {
                            let block: SimpleCompressedPostingBlock<TW> = SimpleCompressedPostingBlock {
                                row_id_start: row_id_start_in_block,
                                block_offset: block_offsets,
                                row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed u16::max -> 65535.
                                row_ids_count: current_block.len() as u8,                          // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                                num_bits,
                                weights: convert_weights_type_for_block::<OW, TW, _>(current_block, |w| w.weight())?,
                            };
                            output_simple_posting_blocks.push(block);
                        }
                        CompressedBlockType::Extended => {
                            let block: ExtendedCompressedPostingBlock<TW> = ExtendedCompressedPostingBlock {
                                row_id_start: row_id_start_in_block,
                                block_offset: block_offsets,
                                row_ids_compressed_size: row_ids_compressed_in_block.len() as u16, // We can ensure that the block row_ids compressed size won't exceed u16::max -> 65535.
                                row_ids_count: current_block.len() as u8,                          // We can ensure that the block size won't exceed `COMPRESSION_BLOCK_SIZE`.
                                num_bits,
                                weights: convert_weights_type_for_block::<OW, TW, _>(current_block, |w| w.weight())?,
                                max_next_weights: convert_weights_type_for_block::<OW, TW, _>(current_block, |w| w.max_next_weight())?,
                            };
                            output_extended_posting_blocks.push(block);
                        }
                    }
                }
            }
            // Update block_offsets for next block.
            block_offsets += row_ids_compressed_in_block.len() as u64;
        }

        return Ok((output_row_ids_compressed_in_posting, output_simple_posting_blocks, output_extended_posting_blocks, total_row_ids_count, max_row_id));
    }

    pub fn build(mut self) -> Result<CompressedPostingList<TW>, PostingListError> {
        let element_type = self.element_type;

        let quantized_param = self.propagate_and_quantize()?;

        let (output_row_ids_compressed_in_posting, output_simple_posting_blocks, output_extended_posting_blocks, total_row_ids_count, max_row_id) =
            self.compress_blocks(quantized_param.clone())?;

        let compressed_posting: CompressedPostingList<TW> = CompressedPostingList::<TW> {
            row_ids_compressed: output_row_ids_compressed_in_posting,
            simple_blocks: output_simple_posting_blocks,
            extended_blocks: output_extended_posting_blocks,
            compressed_block_type: CompressedBlockType::from(element_type),
            quantization_params: quantized_param,
            row_ids_count: total_row_ids_count,
            max_row_id,
        };

        return Ok(compressed_posting);
    }
}

fn quantized_weights_for_block<OW: QuantizedWeight, TW: QuantizedWeight, F: Fn(&GenericElement<OW>) -> OW>(
    block: &[GenericElement<OW>],
    quantization_params: Option<QuantizedParam>,
    weight_selector: F,
) -> Result<[TW; COMPRESSION_BLOCK_SIZE], PostingListError> {
    let quantized_weights: Vec<TW> = block.iter().map(|e| TW::from_u8(OW::quantize_with_param(weight_selector(e), quantization_params.unwrap()))).collect::<Vec<TW>>();
    if quantized_weights.len() > COMPRESSION_BLOCK_SIZE {
        let error_msg = format!("Expected at most {} elements in a single block, found {}", COMPRESSION_BLOCK_SIZE, quantized_weights.len());
        error!("{}", error_msg);
        return Err(PostingListError::LogicError(error_msg));
    }
    let mut quantized_weights_slice: [TW; COMPRESSION_BLOCK_SIZE] = [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE];
    quantized_weights_slice[..quantized_weights.len()].copy_from_slice(&quantized_weights);

    Ok(quantized_weights_slice)
}

fn convert_weights_type_for_block<OW: QuantizedWeight, TW: QuantizedWeight, F: Fn(&GenericElement<OW>) -> OW>(
    block: &[GenericElement<OW>],
    weight_selector: F,
) -> Result<[TW; COMPRESSION_BLOCK_SIZE], PostingListError> {
    let weights: Vec<TW> = block.iter().map(|e: &GenericElement<OW>| TW::from_f32(OW::to_f32(weight_selector(e)))).collect::<Vec<TW>>();
    if weights.len() > COMPRESSION_BLOCK_SIZE {
        let error_msg = format!("Expected at most {} elements in a single block, found {}", COMPRESSION_BLOCK_SIZE, weights.len());
        error!("{}", error_msg);
        return Err(PostingListError::LogicError(error_msg));
    }
    let mut weights_slice: [TW; COMPRESSION_BLOCK_SIZE] = [TW::MINIMUM(); COMPRESSION_BLOCK_SIZE];
    weights_slice[..weights.len()].copy_from_slice(&weights);

    Ok(weights_slice)
}

#[cfg(test)]
mod test {
    use super::super::test::{generate_elements, mock_build_compressed_posting, uncompress_row_ids_from_compressed_posting};
    use crate::core::{CompressedBlockType, ElementType, QuantizedParam, QuantizedWeight, WeightType, DEFAULT_MAX_NEXT_WEIGHT};
    use itertools::Itertools;

    use super::CompressedPostingBuilder;

    #[test]
    fn test_compressed_posting_builder() {
        // Generate serval elements for building compressed posting.
        let elements = vec![(12, 3.56), (11, 2.41), (7, 1.37), (7, 1.389), (13, 0.9), (15, 0.31)];
        // Extended without quantized
        {
            let (cmp_posting, _) = mock_build_compressed_posting::<f32, f32>(ElementType::EXTENDED, elements.clone());

            assert_eq!(cmp_posting.simple_blocks.len(), 0);
            assert_eq!(cmp_posting.extended_blocks.len(), 1);
            assert_eq!(cmp_posting.extended_blocks[0].row_id_start, 7);
            assert_eq!(cmp_posting.extended_blocks[0].row_ids_count, 5);
            assert_eq!(cmp_posting.extended_blocks[0].block_offset, 0);
            assert_eq!(cmp_posting.extended_blocks[0].weights[0..5], [1.389, 2.41, 3.56, 0.9, 0.31]);
            assert_eq!(cmp_posting.extended_blocks[0].max_next_weights[0..5], [3.56, 3.56, 0.9, 0.31, DEFAULT_MAX_NEXT_WEIGHT]);
        }
        // Extended with quantized
        {
            let builder = CompressedPostingBuilder::<f32, u8>::new(ElementType::EXTENDED, true, false);
            assert!(builder.is_err());
        }
        // Simple without quantized
        {
            let (cmp_posting, _) = mock_build_compressed_posting::<f32, f32>(ElementType::SIMPLE, elements.clone());

            assert_eq!(cmp_posting.simple_blocks.len(), 1);
            assert_eq!(cmp_posting.extended_blocks.len(), 0);
            assert_eq!(cmp_posting.simple_blocks[0].row_id_start, 7);
            assert_eq!(cmp_posting.simple_blocks[0].row_ids_count, 5);
            assert_eq!(cmp_posting.simple_blocks[0].block_offset, 0);
            assert_eq!(cmp_posting.simple_blocks[0].weights[0..5], [1.389, 2.41, 3.56, 0.9, 0.31]);
        }
        // Simple with quantized
        {
            let (cmp_posting, _) = mock_build_compressed_posting::<f32, u8>(ElementType::SIMPLE, elements.clone());
            assert_eq!(cmp_posting.simple_blocks[0].weights[0..5], [85, 165, 255, 46, 0]);
            assert_eq!(cmp_posting.quantization_params.unwrap(), QuantizedParam::from_minmax(0.31, 3.56));
        }
    }

    fn inner_test_compressed_posting_compress<OW: QuantizedWeight, TW: QuantizedWeight>(count: usize, use_random_row_id: bool, element_type: ElementType) {
        let need_quantized = OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        let elements = generate_elements(count, use_random_row_id);
        let row_ids_origin = elements.iter().map(|e| e.0).collect::<Vec<u32>>();
        let weights = elements.iter().map(|e| e.1).collect::<Vec<f32>>();

        // Caculate the quantized param
        let (min, max) = match weights.iter().minmax() {
            itertools::MinMaxResult::NoElements => (f32::MINIMUM(), f32::MINIMUM()),
            itertools::MinMaxResult::OneElement(&e) => (e, e),
            itertools::MinMaxResult::MinMax(&min, &max) => (min, max),
        };
        // This quantized logic is same with the `CompressedPostingBuilder::build` method.
        let quantized_param = match need_quantized {
            true => Some(OW::gen_quantized_param(OW::from_f32(min), OW::from_f32(max))),
            false => None,
        };

        let mut builder = CompressedPostingBuilder::<OW, TW>::new(element_type, true, false).expect("");
        for (row_id, weight) in elements {
            builder.add(row_id, weight);
        }
        let cmp_posting = builder.build().unwrap();
        let row_ids_restore = uncompress_row_ids_from_compressed_posting(&cmp_posting);
        let weights_restore = match cmp_posting.compressed_block_type {
            CompressedBlockType::Simple => cmp_posting.simple_blocks.iter().map(|e| e.weights[0..e.row_ids_count as usize].to_vec()).flatten().collect::<Vec<TW>>(),
            CompressedBlockType::Extended => cmp_posting.extended_blocks.iter().map(|e| e.weights[0..e.row_ids_count as usize].to_vec()).flatten().collect::<Vec<TW>>(),
        };

        // Assert the row_ids compressed and restored are equal.
        assert_eq!(row_ids_origin, row_ids_restore);

        if need_quantized {
            // Assert the quantized_params are equal.
            assert_eq!(cmp_posting.quantization_params.unwrap(), quantized_param.unwrap());
            // Assert the quantized weights are equal.
            assert_eq!(
                weights.iter().map(|e| TW::from_u8(OW::quantize_with_param(OW::from_f32(*e), cmp_posting.quantization_params.unwrap()))).collect::<Vec<_>>(),
                weights_restore
            );
        } else {
            // Assert the weights are equal.
            assert_eq!(weights.iter().map(|e| TW::from_f32(*e)).collect::<Vec<_>>(), weights_restore);
        }
    }

    #[test]
    fn test_compressed_posting_compress() {
        // Sequence row_ids.
        inner_test_compressed_posting_compress::<f32, f32>(20000, false, ElementType::EXTENDED);
        inner_test_compressed_posting_compress::<f32, f32>(20000, false, ElementType::SIMPLE);
        inner_test_compressed_posting_compress::<f32, u8>(20000, false, ElementType::SIMPLE);

        inner_test_compressed_posting_compress::<half::f16, half::f16>(20000, false, ElementType::EXTENDED);
        inner_test_compressed_posting_compress::<half::f16, half::f16>(20000, false, ElementType::SIMPLE);
        inner_test_compressed_posting_compress::<half::f16, u8>(20000, false, ElementType::SIMPLE);

        inner_test_compressed_posting_compress::<u8, u8>(20000, false, ElementType::EXTENDED);
        inner_test_compressed_posting_compress::<u8, u8>(20000, false, ElementType::SIMPLE);

        // Random row_ids.
        inner_test_compressed_posting_compress::<f32, f32>(20000, true, ElementType::EXTENDED);
        inner_test_compressed_posting_compress::<f32, f32>(20000, true, ElementType::SIMPLE);
        inner_test_compressed_posting_compress::<f32, u8>(20000, true, ElementType::SIMPLE);

        inner_test_compressed_posting_compress::<half::f16, half::f16>(20000, true, ElementType::EXTENDED);
        inner_test_compressed_posting_compress::<half::f16, half::f16>(20000, true, ElementType::SIMPLE);
        inner_test_compressed_posting_compress::<half::f16, u8>(20000, true, ElementType::SIMPLE);

        inner_test_compressed_posting_compress::<u8, u8>(20000, true, ElementType::EXTENDED);
        inner_test_compressed_posting_compress::<u8, u8>(20000, true, ElementType::SIMPLE);
    }
}
