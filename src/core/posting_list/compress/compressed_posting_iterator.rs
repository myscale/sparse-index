use log::error;

use crate::{
    core::{
        BlockDecoder, ElementRead, ExtendedElement, GenericElement, PostingListIter,
        QuantizedWeight, SimpleElement, WeightType, COMPRESSION_BLOCK_SIZE,
    },
    RowId,
};
use std::marker::PhantomData;

use super::{
    CompressedPostingListView, ExtendedCompressedPostingBlock, SimpleCompressedPostingBlock,
};

/// `TW` means wieght type stored in disk.
/// `OW` means weight type before stored or quantized.
#[derive(Debug, Clone)]
pub struct CompressedPostingListIterator<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    posting: CompressedPostingListView<'a, TW>,
    is_uncompressed: bool,
    row_ids_uncompressed_in_block: Vec<RowId>,
    cursor: usize,
    use_quantized: bool,
    decoder: BlockDecoder,
    _tw: PhantomData<OW>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingListIterator<'a, OW, TW> {
    pub fn new(posting: &CompressedPostingListView<'a, TW>) -> Self {
        // Boundary.
        let use_quantized =
            OW::weight_type() != TW::weight_type() && OW::weight_type() == WeightType::WeightU8;

        if use_quantized && posting.quantization_params.is_none() {
            let error_msg = "Error happened when create `CompressedPostingListIterator`, `posting.quantization_params` can't be none when quantized is enabled.";
            error!("{}", error_msg);
            panic!("{}", error_msg);
        }

        Self {
            posting: posting.clone(),
            is_uncompressed: false,
            row_ids_uncompressed_in_block: vec![],
            cursor: 0,
            use_quantized,
            decoder: BlockDecoder::default(),
            _tw: PhantomData,
        }
    }
    // convert OW (inner storage type) into TW (unquantized type)
    // fn convert_type(&self, raw_element: &ExtendedElement<OW>) -> ExtendedElement<TW> {
    //     if self.quantized_param.is_none() {
    //         assert_eq!(OW::weight_type(), TW::weight_type());

    //         let weight_convert = TW::from_f32(OW::to_f32(raw_element.weight));
    //         let max_next_weight_convert = TW::from_f32(OW::to_f32(raw_element.max_next_weight));
    //         let converted_element: ExtendedElement<TW> = ExtendedElement {
    //             row_id: raw_element.row_id,
    //             weight: weight_convert,
    //             max_next_weight: max_next_weight_convert,
    //         };

    //         return converted_element;
    //     } else {
    //         assert_eq!(OW::weight_type(), WeightType::WeightU8);
    //         let param: QuantizedParam = self.quantized_param.unwrap();
    //         let converted: ExtendedElement<TW> = ExtendedElement::<TW> {
    //             row_id: raw_element.row_id,
    //             weight: TW::unquantize_with_param(OW::to_u8(raw_element.weight), param),
    //             max_next_weight: TW::unquantize_with_param(
    //                 OW::to_u8(raw_element.max_next_weight),
    //                 param,
    //             ),
    //         };
    //         return converted;
    //     }
    // }

    // TODO: make sure element returned should be current element, and then increase cursor, keep same with SimplePosting.
    pub fn next(&mut self) -> Option<GenericElement<OW>> {
        // Boundary
        if self.cursor >= self.posting.row_ids_count as usize {
            return None;
        }
        // If cursor enter new block range, mark it not been decompressed.
        if self.cursor % COMPRESSION_BLOCK_SIZE == 0 {
            self.is_uncompressed = false;
        }
        let element_opt = self.peek();
        // increase cursor
        self.cursor += 1;
        element_opt
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIter<OW, TW>
    for CompressedPostingListIterator<'a, OW, TW>
{
    fn peek(&mut self) -> Option<GenericElement<OW>> {
        // Boundary
        if self.cursor >= self.posting.row_ids_count as usize {
            return None;
        }

        let block_idx = self.cursor / COMPRESSION_BLOCK_SIZE;

        if !self.is_uncompressed {
            // dynamic decompresse block in `CompressedPostingListView`
            match self.posting.compressed_block_type {
                super::CompressedBlockType::Simple => {
                    self.posting.uncompress_simple_block(
                        block_idx,
                        &mut self.decoder,
                        &mut self.row_ids_uncompressed_in_block,
                    );
                }
                super::CompressedBlockType::Extended => {
                    self.posting.uncompress_extended_block(
                        block_idx,
                        &mut self.decoder,
                        &mut self.row_ids_uncompressed_in_block,
                    );
                }
            }

            self.is_uncompressed = true;
        }

        let relative_row_id = self.cursor % COMPRESSION_BLOCK_SIZE;

        match self.posting.compressed_block_type {
            super::CompressedBlockType::Simple => {
                let block: &SimpleCompressedPostingBlock<TW> =
                    &self.posting.simple_blocks[block_idx];

                let raw_simple_element = GenericElement::SimpleElement(SimpleElement {
                    row_id: self.row_ids_uncompressed_in_block[relative_row_id],
                    weight: block.weights[relative_row_id],
                });
                Some(
                    raw_simple_element
                        .convert_or_unquantize::<OW>(self.posting.quantization_params),
                )
            }
            super::CompressedBlockType::Extended => {
                let block: &ExtendedCompressedPostingBlock<TW> =
                    &self.posting.extended_blocks[block_idx];

                let raw_extended_element = GenericElement::ExtendedElement(ExtendedElement {
                    row_id: self.row_ids_uncompressed_in_block[relative_row_id],
                    weight: block.weights[relative_row_id],
                    max_next_weight: block.max_next_weights[relative_row_id],
                });
                Some(
                    raw_extended_element
                        .convert_or_unquantize::<OW>(self.posting.quantization_params),
                )
            }
        }
    }

    fn last_id(&self) -> Option<RowId> {
        self.posting.max_row_id
    }

    fn skip_to(&mut self, row_id: RowId) -> Option<GenericElement<OW>> {
        while let Some(element) = self.peek() {
            match element.row_id().cmp(&row_id) {
                std::cmp::Ordering::Less => {
                    self.next();
                }
                std::cmp::Ordering::Equal => return Some(element),
                std::cmp::Ordering::Greater => return None,
            }
        }
        None
    }

    fn skip_to_end(&mut self) {
        // If skip operation trigger cursor enter a new block range, we should mark it with uncompressed status.
        if (self.posting.row_ids_count - self.cursor as u32) / COMPRESSION_BLOCK_SIZE as u32 >= 1 {
            self.is_uncompressed = false;
        }
        self.cursor = (self.posting.row_ids_count - 1) as usize;
    }

    fn remains(&self) -> usize {
        self.posting.row_ids_count as usize - self.cursor
    }

    fn cursor(&self) -> usize {
        self.cursor
    }

    fn for_each_till_row_id(&mut self, row_id: RowId, mut f: impl FnMut(&GenericElement<OW>)) {
        let mut element_opt = self.peek();
        while let Some(element) = element_opt {
            if element.row_id() > row_id {
                break;
            }
            f(&element);
            element_opt = self.next();
        }
    }
}
