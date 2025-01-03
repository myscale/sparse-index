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

#[cfg(test)]
mod test {
    use super::super::test::{enlarge_elements, generate_random_float, get_compressed_posting_iterator, mock_build_compressed_posting};
    use crate::core::{CompressedPostingList, CompressedPostingListView, ElementRead, ElementType, PostingListIter, QuantizedParam, QuantizedWeight};

    fn generate_elements<OW: QuantizedWeight, TW: QuantizedWeight>(element_type: ElementType, count: usize) -> (CompressedPostingList<TW>, Vec<(u32, f32)>) {
        #[rustfmt::skip]
        let mut elements: Vec<(u32, f32)> = Vec::new();
        // vec![
        //     (1, 2.3), (2, 1.2), (3, 0.3), (4, 4.3), (5, 1.4), (6, 2.1), (7, 2.3), (8, 3.4), (9, 2.9), (10, 2.8), (11, 1.8), (12, 3.4), (13, 1.2), (14, 2.1), (15, 3.1), (16, 1.1), (17, 3.2), (18, 1.5), (19, 2.1), (20, 2.8),
        //     (21, 1.9), (22, 3.8), (23, 4.2), (24, 3.9), (25, 4.2), (26, 1.6), (27, 1.1), (28, 4.1), (29, 7.4), (30, 9.64), (31, 2.88), (32, 9.79), (33, 7.77), (34, 0.66), (35, 8.74), (36, 3.08), (37, 2.09), (38, 6.54), (39, 7.09), (40, 4.79),
        //     (41, 6.22), (42, 5.83), (43, 6.24), (44, 6.08), (45, 4.29), (46, 0.53), (47, 5.15), (48, 7.57), (49, 1.03), (50, 9.97), (51, 3.41), (52, 2.03), (53, 1.9), (54, 2.25), (55, 7.52), (56, 0.74), (57, 1.96), (58, 2.76), (59, 5.37), (60, 4.19),
        //     (61, 3.01), (62, 7.04), (63, 4.04), (64, 3.09), (65, 8.42), (66, 8.85), (67, 7.76), (68, 0.15), (69, 6.83), (70, 0.55), (71, 9.35), (72, 1.05), (73, 0.41), (74, 0.78), (75, 7.34), (76, 9.23), (77, 9.47), (78, 5.29), (79, 8.39), (80, 4.67),
        //     (81, 2.0), (82, 3.48), (83, 1.06), (84, 9.52), (85, 8.76), (86, 3.8), (87, 7.55), (88, 1.08), (89, 9.75), (90, 0.39), (91, 2.01), (92, 7.56), (93, 1.83), (94, 1.02), (95, 1.83), (96, 1.94), (97, 7.21), (98, 0.45), (99, 7.87), (100, 2.4),
        //     (101, 5.73), (102, 8.55), (103, 9.74), (104, 0.12), (105, 4.7), (106, 5.25), (107, 8.38), (108, 9.02), (109, 0.09), (110, 4.04), (111, 4.07), (112, 2.03), (113, 4.06), (114, 1.69), (115, 4.77), (116, 0.06), (117, 8.03), (118, 7.57), (119, 4.44), (120, 1.48),
        //     (121, 9.14), (122, 2.32), (123, 3.47), (124, 3.25), (125, 0.74), (126, 3.09), (127, 1.83), (128, 0.98), (129, 7.37), (130, 8.33), (131, 5.01), (132, 3.54), (133, 6.41), (134, 9.27), (135, 5.41), (136, 7.71), (137, 9.79), (138, 7.85), (139, 6.87), (140, 0.2),
        //     (141, 3.15), (142, 7.9), (143, 5.05), (144, 0.38), (145, 4.06), (146, 9.74), (147, 3.74), (148, 1.29), (149, 3.75), (150, 7.27), (151, 9.47), (152, 5.07), (153, 3.04), (154, 6.03), (155, 0.31), (156, 4.65), (157, 5.27), (158, 8.19), (159, 3.0), (160, 9.49),
        //     (161, 8.9), (162, 7.33), (163, 3.8), (164, 9.07), (165, 7.87), (166, 7.2), (167, 1.77), (168, 9.54), (169, 7.0), (170, 9.68), (171, 1.84), (172, 9.23), (173, 6.66), (174, 5.79), (175, 4.23), (176, 4.25), (177, 3.67), (178, 9.8), (179, 1.89), (180, 0.78),
        //     (181, 2.99), (182, 7.79), (183, 9.32), (184, 9.16), (185, 8.42), (186, 0.91), (187, 5.84), (188, 5.74), (189, 6.91), (190, 6.7), (191, 4.29), (192, 7.34), (193, 8.27), (194, 5.66), (195, 6.43), (196, 0.95), (197, 0.63), (198, 1.78), (199, 4.74), (200, 4.24),
        //     (201, 6.59), (202, 8.69), (203, 8.37), (204, 6.66), (205, 5.49), (206, 0.17), (207, 2.38), (208, 8.61), (209, 2.16), (210, 3.65), (211, 0.68), (212, 0.52), (213, 5.42), (214, 9.92), (215, 9.89), (216, 2.16), (217, 4.55), (218, 3.98), (219, 2.52), (220, 7.23),
        //     (221, 2.64), (222, 4.36), (223, 6.73), (224, 4.29)];

        for row_id in 1..=count {
            elements.push((row_id as u32, format!("{:.3}", generate_random_float()).parse::<f32>().unwrap()));
        }

        let (cmp_posting, _) = mock_build_compressed_posting::<OW, TW>(element_type, elements.clone());
        (cmp_posting, elements)
    }

    #[test]
    fn test_iterator_clone_from_view() {
        let (cmp_posting, _) = generate_elements::<f32, f32>(ElementType::SIMPLE, 20097);
        // get reference of row_ids_compressed in compressed posting.
        let row_ids_ref = cmp_posting.row_ids_compressed.as_slice();
        // create view from this compressed posting.
        let cmp_posting_view: CompressedPostingListView<'_, f32> = cmp_posting.view();

        assert!(std::ptr::addr_eq(row_ids_ref as *const _, cmp_posting_view.row_ids_compressed as *const _));

        // create iterator from this view.
        let iterator = get_compressed_posting_iterator::<f32, f32>(&cmp_posting);

        assert!(std::ptr::addr_eq(cmp_posting_view.row_ids_compressed as *const _, iterator.posting.row_ids_compressed as *const _));
    }

    #[test]
    fn test_iterator_peek_and_next() {
        let (cmp_posting, elements) = generate_elements::<f32, f32>(ElementType::SIMPLE, 20097);
        let mut cmp_iterator = get_compressed_posting_iterator::<f32, f32>(&cmp_posting);

        while let Some(peek_element) = cmp_iterator.peek() {
            assert_eq!(peek_element.row_id(), elements[cmp_iterator.cursor()].0);
            assert_eq!(peek_element.weight(), elements[cmp_iterator.cursor()].1);
            cmp_iterator.next();
        }
    }

    #[test]
    fn test_iterator_next() {
        let (cmp_posting, elements) = generate_elements::<f32, f32>(ElementType::SIMPLE, 20097);
        let mut cmp_iterator = get_compressed_posting_iterator::<f32, f32>(&cmp_posting);

        while let Some(peek_element) = cmp_iterator.next() {
            assert_eq!(peek_element.row_id(), elements[cmp_iterator.cursor() - 1].0);
            assert_eq!(peek_element.weight(), elements[cmp_iterator.cursor() - 1].1);
        }
    }

    #[test]
    fn test_last_id() {
        let (cmp_posting, elements) = generate_elements::<f32, f32>(ElementType::SIMPLE, 563);
        let cmp_iterator = get_compressed_posting_iterator::<f32, f32>(&cmp_posting);
        assert_eq!(cmp_iterator.last_id(), Some(563));
    }

    #[test]
    fn test_remains() {
        // for next and peek.
        {
            let (cmp_posting, elements) = generate_elements::<f32, f32>(ElementType::SIMPLE, 563);
            let mut cmp_iterator = get_compressed_posting_iterator::<f32, f32>(&cmp_posting);
            assert_eq!(cmp_iterator.remains(), elements.len());

            let mut cnt = 0;
            while let Some(_) = cmp_iterator.peek() {
                assert_eq!(cmp_iterator.remains(), elements.len() - cnt);
                cmp_iterator.next();
                cnt += 1;
            }
        }
        // for next.
        {
            let (cmp_posting, elements) = generate_elements::<f32, f32>(ElementType::SIMPLE, 563);
            let mut cmp_iterator = get_compressed_posting_iterator::<f32, f32>(&cmp_posting);
            assert_eq!(cmp_iterator.remains(), elements.len());

            let mut cnt = 0;
            while let Some(_) = cmp_iterator.next() {
                cnt += 1;
                assert_eq!(cmp_iterator.remains(), elements.len() - cnt);
            }
        }
    }

    #[test]
    fn test_for_each_till_row_id() {
        let (cmp_posting, elements) = generate_elements::<f32, f32>(ElementType::SIMPLE, 20097);
        let mut cmp_iterator = get_compressed_posting_iterator::<f32, f32>(&cmp_posting);
        assert_eq!(cmp_iterator.remains(), elements.len());

        let mut cnt = 0;
        cmp_iterator.for_each_till_row_id(1126, |e| {
            println!("element: {:?}, cnt: {}, el: {:?}", e, cnt, elements[cnt]);
            // assert_eq!(e.row_id(), elements[cnt].0);
            // assert_eq!(e.weight(), elements[cnt].1);
            cnt += 1;
        });
        assert_eq!(cnt, 1125);
    }
}
