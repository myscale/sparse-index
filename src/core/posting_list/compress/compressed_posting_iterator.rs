use crate::{
    core::{BlockDecoder, ElementRead, ExtendedElement, GenericElement, PostingListIter, QuantizedWeight, SimpleElement, COMPRESSION_BLOCK_SIZE},
    RowId,
};
use std::marker::PhantomData;

use super::{CompressedPostingListView, ExtendedCompressedPostingBlock, SimpleCompressedPostingBlock};

/// `TW` means wieght type stored in disk.
/// `OW` means weight type before stored or quantized.
#[derive(Debug, Clone)]
pub struct CompressedPostingListIterator<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    posting: CompressedPostingListView<'a, TW>,
    is_uncompressed: bool,
    row_ids_uncompressed_in_block: Vec<RowId>,
    cursor: usize,
    decoder: BlockDecoder,
    _tw: PhantomData<OW>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingListIterator<'a, OW, TW> {
    pub fn new(posting: &CompressedPostingListView<'a, TW>) -> Self {
        Self { posting: posting.clone(), is_uncompressed: false, row_ids_uncompressed_in_block: vec![], cursor: 0, decoder: BlockDecoder::default(), _tw: PhantomData }
    }

    // TODO: make sure element returned should be current element, and then increase cursor, keep same with SimplePosting.Qzz
    pub fn next(&mut self) -> Option<GenericElement<OW>> {
        // Boundary
        if self.cursor >= self.posting.row_ids_count as usize {
            return None;
        }
        // If cursor enter new block range, mark it not been decompressed.
        // Code logic in func `skip_to` will iter all elements one by one.
        if self.cursor % COMPRESSION_BLOCK_SIZE == 0 && self.is_uncompressed {
            self.is_uncompressed = false;
        }
        let element_opt = self.peek();
        // increase cursor
        self.cursor += 1;

        // make sure cursor is valid.
        if self.cursor % COMPRESSION_BLOCK_SIZE == 0 && self.is_uncompressed {
            self.is_uncompressed = false;
        }
        element_opt
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIter<OW, TW> for CompressedPostingListIterator<'a, OW, TW> {
    fn peek(&mut self) -> Option<GenericElement<OW>> {
        // Boundary
        if self.cursor >= self.posting.row_ids_count as usize {
            return None;
        }

        let block_idx = self.cursor / COMPRESSION_BLOCK_SIZE;

        if !self.is_uncompressed {
            // dynamic decompresse block in `CompressedPostingListView`
            // swallow error exception.
            self.posting.uncompress_block(self.posting.compressed_block_type, block_idx, &mut self.decoder, &mut self.row_ids_uncompressed_in_block).unwrap_or_default();
            self.is_uncompressed = true;
        }

        let relative_row_id = self.cursor % COMPRESSION_BLOCK_SIZE;

        match self.posting.compressed_block_type {
            super::CompressedBlockType::Simple => {
                let block: &SimpleCompressedPostingBlock<TW> = &self.posting.simple_blocks[block_idx];
                let row_id = self.row_ids_uncompressed_in_block[relative_row_id];

                let raw_simple_element = GenericElement::SimpleElement(SimpleElement { row_id, weight: block.weights[relative_row_id] });
                Some(raw_simple_element.convert_or_unquantize::<OW>(self.posting.quantization_params))
            }
            super::CompressedBlockType::Extended => {
                let block: &ExtendedCompressedPostingBlock<TW> = &self.posting.extended_blocks[block_idx];
                let row_id = self.row_ids_uncompressed_in_block[relative_row_id];

                let raw_extended_element =
                    GenericElement::ExtendedElement(ExtendedElement { row_id, weight: block.weights[relative_row_id], max_next_weight: block.max_next_weights[relative_row_id] });
                Some(raw_extended_element.convert_or_unquantize::<OW>(self.posting.quantization_params))
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
        while let Some(element) = self.next() {
            if element.row_id() > row_id {
                break;
            }
            f(&element);
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::test::{get_compressed_posting_iterator, mock_build_compressed_posting, mock_compressed_posting_from_sequence_elements};
    use crate::{
        core::{CompressedPostingListView, ElementRead, ElementType, PostingListIter, QuantizedWeight},
        RowId,
    };

    fn inner_test_iterator_clone_from_view<OW: QuantizedWeight, TW: QuantizedWeight>(count: usize, element_type: ElementType) {
        let (cmp_posting, _) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, count);

        // Get references from cmp_posting.
        let row_ids_ref = cmp_posting.row_ids_compressed.as_slice();
        let simple_blocks_ref = cmp_posting.simple_blocks.as_slice();
        let extend_blocks_ref = cmp_posting.extended_blocks.as_slice();

        // create view from this compressed posting.
        let cmp_posting_view: CompressedPostingListView<'_, TW> = cmp_posting.view();

        // Assert the address of [`row_ids_compressed`, `simple_blocks`, `extended_blocks`] in cmp_posting.view() is same with cmp_posting.
        assert!(std::ptr::addr_eq(row_ids_ref as *const _, cmp_posting_view.row_ids_compressed as *const _));
        assert!(std::ptr::addr_eq(simple_blocks_ref as *const _, cmp_posting_view.simple_blocks as *const _));
        assert!(std::ptr::addr_eq(extend_blocks_ref as *const _, cmp_posting_view.extended_blocks as *const _));

        // Create iterator from this cmp_posting.view().
        let iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);

        // Assert the address of [`row_ids_compressed`, `simple_blocks`, `extended_blocks`] in iterator is same with cmp_posting.view().
        assert!(std::ptr::addr_eq(cmp_posting_view.row_ids_compressed as *const _, iterator.posting.row_ids_compressed as *const _));
        assert!(std::ptr::addr_eq(cmp_posting_view.simple_blocks as *const _, iterator.posting.simple_blocks as *const _));
        assert!(std::ptr::addr_eq(cmp_posting_view.extended_blocks as *const _, iterator.posting.extended_blocks as *const _));
    }

    #[test]
    fn test_iterator_clone_from_view() {
        // Boundary Test
        inner_test_iterator_clone_from_view::<f32, f32>(0, ElementType::SIMPLE);
        inner_test_iterator_clone_from_view::<f32, f32>(1, ElementType::SIMPLE);
        inner_test_iterator_clone_from_view::<f32, u8>(0, ElementType::SIMPLE);
        inner_test_iterator_clone_from_view::<f32, u8>(1, ElementType::SIMPLE);

        // Normal Test
        inner_test_iterator_clone_from_view::<f32, f32>(20097, ElementType::SIMPLE);
        inner_test_iterator_clone_from_view::<f32, f32>(20097, ElementType::EXTENDED);
        inner_test_iterator_clone_from_view::<f32, u8>(20097, ElementType::SIMPLE);

        inner_test_iterator_clone_from_view::<half::f16, half::f16>(20097, ElementType::SIMPLE);
        inner_test_iterator_clone_from_view::<half::f16, half::f16>(20097, ElementType::EXTENDED);
        inner_test_iterator_clone_from_view::<half::f16, u8>(20097, ElementType::SIMPLE);

        inner_test_iterator_clone_from_view::<u8, u8>(20097, ElementType::SIMPLE);
        inner_test_iterator_clone_from_view::<u8, u8>(20097, ElementType::EXTENDED);
    }

    fn inner_test_iterator_peek_and_next<OW: QuantizedWeight, TW: QuantizedWeight>(count: usize, element_type: ElementType) {
        let (cmp_posting, elements) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, count);
        let mut cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);

        while let Some(peek_element) = cmp_iterator.peek() {
            assert_eq!(peek_element.row_id(), elements[cmp_iterator.cursor()].0);
            if cmp_posting.quantization_params.is_some() {
                assert!(cmp_posting.quantization_params.unwrap().approximately_eq(peek_element.weight(), OW::from_f32(elements[cmp_iterator.cursor()].1)));
            } else {
                assert_eq!(peek_element.weight(), OW::from_f32(elements[cmp_iterator.cursor()].1));
            }
            cmp_iterator.next();
        }
    }

    #[test]
    fn test_iterator_peek_and_next() {
        // Boundary Test
        inner_test_iterator_peek_and_next::<f32, f32>(0, ElementType::SIMPLE);
        inner_test_iterator_peek_and_next::<f32, f32>(1, ElementType::SIMPLE);
        inner_test_iterator_peek_and_next::<f32, u8>(0, ElementType::SIMPLE);
        inner_test_iterator_peek_and_next::<f32, u8>(1, ElementType::SIMPLE);

        // Normal Test
        inner_test_iterator_peek_and_next::<f32, f32>(20097, ElementType::SIMPLE);
        inner_test_iterator_peek_and_next::<f32, f32>(20097, ElementType::EXTENDED);
        inner_test_iterator_peek_and_next::<f32, u8>(20097, ElementType::SIMPLE);

        inner_test_iterator_peek_and_next::<half::f16, half::f16>(20097, ElementType::SIMPLE);
        inner_test_iterator_peek_and_next::<half::f16, half::f16>(20097, ElementType::EXTENDED);
        inner_test_iterator_peek_and_next::<half::f16, u8>(20097, ElementType::SIMPLE);

        inner_test_iterator_peek_and_next::<u8, u8>(20097, ElementType::SIMPLE);
        inner_test_iterator_peek_and_next::<u8, u8>(20097, ElementType::EXTENDED);
    }

    fn inner_test_iterator_next<OW: QuantizedWeight, TW: QuantizedWeight>(count: usize, element_type: ElementType) {
        let (cmp_posting, elements) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, count);
        let mut cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);

        while let Some(peek_element) = cmp_iterator.next() {
            assert_eq!(peek_element.row_id(), elements[cmp_iterator.cursor() - 1].0);
            if cmp_posting.quantization_params.is_some() {
                assert!(cmp_posting.quantization_params.unwrap().approximately_eq(peek_element.weight(), OW::from_f32(elements[cmp_iterator.cursor() - 1].1)));
            } else {
                assert_eq!(peek_element.weight(), OW::from_f32(elements[cmp_iterator.cursor() - 1].1));
            }
        }
    }

    #[test]
    fn test_iterator_next() {
        // Boundary Test
        inner_test_iterator_next::<f32, f32>(0, ElementType::SIMPLE);
        inner_test_iterator_next::<f32, f32>(1, ElementType::SIMPLE);
        inner_test_iterator_next::<f32, u8>(0, ElementType::SIMPLE);
        inner_test_iterator_next::<f32, u8>(1, ElementType::SIMPLE);

        // Normal Test
        inner_test_iterator_next::<f32, f32>(20097, ElementType::SIMPLE);
        inner_test_iterator_next::<f32, f32>(20097, ElementType::EXTENDED);
        inner_test_iterator_next::<f32, u8>(20097, ElementType::SIMPLE);

        inner_test_iterator_next::<half::f16, half::f16>(20097, ElementType::SIMPLE);
        inner_test_iterator_next::<half::f16, half::f16>(20097, ElementType::EXTENDED);
        inner_test_iterator_next::<half::f16, u8>(20097, ElementType::SIMPLE);

        inner_test_iterator_next::<u8, u8>(20097, ElementType::SIMPLE);
        inner_test_iterator_next::<u8, u8>(20097, ElementType::EXTENDED);
    }

    fn inner_test_last_id<OW: QuantizedWeight, TW: QuantizedWeight>(count: usize, element_type: ElementType) {
        let (cmp_posting, _) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, count);
        let cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);
        if count == 0 {
            assert!(cmp_iterator.last_id().is_none());
        } else {
            assert_eq!(cmp_iterator.last_id(), Some(count as u32));
        }
    }

    #[test]
    fn test_last_id() {
        // Boundary Test
        inner_test_last_id::<f32, f32>(0, ElementType::SIMPLE);
        inner_test_last_id::<f32, f32>(1, ElementType::SIMPLE);

        // Normal Test
        inner_test_last_id::<f32, f32>(20097, ElementType::SIMPLE);
    }

    fn inner_test_remains_by_peek_and_next<OW: QuantizedWeight, TW: QuantizedWeight>(count: usize, element_type: ElementType) {
        let (cmp_posting, _) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, count);
        let mut cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);
        assert_eq!(cmp_iterator.remains(), count);

        let mut cnt = 0;
        while let Some(_) = cmp_iterator.peek() {
            assert_eq!(cmp_iterator.remains(), count - cnt);
            cmp_iterator.next();
            cnt += 1;
        }
    }

    fn inner_test_remains_by_next<OW: QuantizedWeight, TW: QuantizedWeight>(count: usize, element_type: ElementType) {
        let (cmp_posting, _) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, count);
        let mut cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);
        assert_eq!(cmp_iterator.remains(), count);

        let mut cnt = 0;
        while let Some(_) = cmp_iterator.next() {
            cnt += 1;
            assert_eq!(cmp_iterator.remains(), count - cnt);
        }
    }

    #[test]
    fn test_remains() {
        // Boundary Test
        inner_test_remains_by_peek_and_next::<f32, f32>(0, ElementType::SIMPLE);
        inner_test_remains_by_next::<f32, f32>(0, ElementType::SIMPLE);
        inner_test_remains_by_peek_and_next::<f32, f32>(1, ElementType::SIMPLE);
        inner_test_remains_by_next::<f32, f32>(1, ElementType::SIMPLE);

        // Normal Test
        inner_test_remains_by_peek_and_next::<f32, f32>(20097, ElementType::SIMPLE);
        inner_test_remains_by_next::<f32, f32>(20097, ElementType::SIMPLE);
    }

    fn inner_test_for_each_till_row_id<OW: QuantizedWeight, TW: QuantizedWeight>(element_type: ElementType, element_total_count: usize, element_iter_count: usize) {
        let (cmp_posting, elements) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, element_total_count);
        let mut cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);
        assert_eq!(cmp_iterator.remains(), elements.len());

        // When we haven't call func `next`, cursor should be 0.
        assert_eq!(cmp_iterator.cursor(), 0);

        // Try iterate `element_iter_count` elements.
        let mut elements_idx = 0;
        cmp_iterator.for_each_till_row_id(element_iter_count as RowId, |e| {
            assert_eq!(e.row_id(), elements[elements_idx].0);
            if cmp_posting.quantization_params.is_some() {
                assert!(cmp_posting.quantization_params.unwrap().approximately_eq(e.weight(), OW::from_f32(elements[elements_idx].1)));
            } else {
                assert_eq!(e.weight(), OW::from_f32(elements[elements_idx].1));
            }
            elements_idx += 1;
        });

        // The iterator's cursor should be `element_iter_count` + 1, cause we have called func `next` when `row_id` is invalid.
        assert_eq!(cmp_iterator.cursor(), element_iter_count + 1);
        assert_eq!(elements_idx, element_iter_count);
    }

    #[test]
    fn test_for_each_till_row_id() {
        // Boundary Test
        inner_test_for_each_till_row_id::<f32, f32>(ElementType::SIMPLE, 20097, 0);
        inner_test_for_each_till_row_id::<f32, u8>(ElementType::SIMPLE, 20097, 0);

        // Normal Test
        inner_test_for_each_till_row_id::<f32, f32>(ElementType::SIMPLE, 20097, 1776);
        inner_test_for_each_till_row_id::<f32, u8>(ElementType::SIMPLE, 20097, 1776);
        inner_test_for_each_till_row_id::<half::f16, half::f16>(ElementType::SIMPLE, 20097, 1776);
        inner_test_for_each_till_row_id::<half::f16, u8>(ElementType::SIMPLE, 20097, 1776);
    }

    fn inner_test_skip_to<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        elements: Vec<(RowId, f32)>,
        skip_to_row_id: RowId,
        expected_cursor: usize,
        can_find: bool,
    ) {
        let (cmp_posting, _) = mock_build_compressed_posting::<OW, TW>(element_type, elements.clone());
        let mut cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);
        assert_eq!(cmp_iterator.remains(), elements.len());
        assert_eq!(cmp_iterator.cursor(), 0);

        // Execute skip_to operation.
        let element = cmp_iterator.skip_to(skip_to_row_id);

        assert_eq!(cmp_iterator.cursor(), expected_cursor);
        if can_find {
            let element = element.unwrap();
            // Assert row_id is equal.
            assert_eq!(element.row_id(), elements[expected_cursor].0);
            // Assert weights is equal.
            if cmp_posting.quantization_params.is_some() {
                assert!(cmp_posting.quantization_params.unwrap().approximately_eq(element.weight(), OW::from_f32(elements[expected_cursor].1)));
            } else {
                assert_eq!(element.weight(), OW::from_f32(elements[expected_cursor].1));
            }
        } else {
            assert!(element.is_none());
        }
    }

    #[test]
    fn test_skip_to() {
        let elements = vec![(2, 0.2), (3, 0.3), (6, 0.6), (7, 0.7), (8, 0.8), (9, 0.9), (10, 1.0)];
        inner_test_skip_to::<f32, f32>(ElementType::SIMPLE, elements.clone(), 0, 0, false);
        inner_test_skip_to::<f32, f32>(ElementType::EXTENDED, elements.clone(), 8, 4, true);
        inner_test_skip_to::<f32, f32>(ElementType::EXTENDED, elements.clone(), 5, 2, false);

        inner_test_skip_to::<f32, u8>(ElementType::SIMPLE, elements.clone(), 0, 0, false);
        inner_test_skip_to::<half::f16, u8>(ElementType::SIMPLE, elements.clone(), 8, 4, true);
        inner_test_skip_to::<u8, u8>(ElementType::SIMPLE, elements.clone(), 5, 2, false);
    }

    fn inner_test_skip_to_end<OW: QuantizedWeight, TW: QuantizedWeight>(element_type: ElementType, count: usize) {
        let (cmp_posting, elements) = mock_compressed_posting_from_sequence_elements::<OW, TW>(element_type, count);
        let mut cmp_iterator = get_compressed_posting_iterator::<OW, TW>(&cmp_posting);
        assert_eq!(cmp_iterator.remains(), elements.len());
        assert_eq!(cmp_iterator.cursor(), 0);
        let _ = cmp_iterator.skip_to_end();
        assert_eq!(cmp_iterator.remains(), 1);
        assert_eq!(cmp_iterator.cursor(), count - 1);

        // Call func `peek` after `skip_to_end` should return last element.
        let last_element = cmp_iterator.peek().unwrap();
        assert_eq!(last_element.row_id(), elements[count - 1].0);
        if cmp_posting.quantization_params.is_some() {
            assert!(cmp_posting.quantization_params.unwrap().approximately_eq(last_element.weight(), OW::from_f32(elements[count - 1].1)));
        } else {
            assert_eq!(last_element.weight(), OW::from_f32(elements[count - 1].1));
        }

        // Call func `next` after `skip_to_end` should return None.
        let next_element = cmp_iterator.next().unwrap();
        assert_eq!(next_element.row_id(), elements[count - 1].0);
        if cmp_posting.quantization_params.is_some() {
            assert!(cmp_posting.quantization_params.unwrap().approximately_eq(next_element.weight(), OW::from_f32(elements[count - 1].1)));
        } else {
            assert_eq!(next_element.weight(), OW::from_f32(elements[count - 1].1));
        }
        assert!(cmp_iterator.peek().is_none());
        assert!(cmp_iterator.next().is_none());
    }

    #[test]
    fn test_skip_to_end() {
        // Boundary Test
        inner_test_skip_to_end::<f32, f32>(ElementType::EXTENDED, 1);
        inner_test_skip_to_end::<f32, u8>(ElementType::SIMPLE, 1);

        // Normal Test
        inner_test_skip_to_end::<f32, f32>(ElementType::EXTENDED, 20096);
        inner_test_skip_to_end::<f32, u8>(ElementType::SIMPLE, 20096);
        inner_test_skip_to_end::<half::f16, half::f16>(ElementType::EXTENDED, 20096);
        inner_test_skip_to_end::<half::f16, u8>(ElementType::SIMPLE, 20096);
    }
}
