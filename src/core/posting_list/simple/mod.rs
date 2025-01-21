mod posting_list;
mod posting_list_builder;
mod posting_list_iterator;
mod posting_list_merge;

pub use posting_list::PostingList;
pub use posting_list_builder::PostingListBuilder;
pub use posting_list_iterator::PostingListIterator;
pub use posting_list_merge::PostingListMerger;

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use rand::Rng;

    use crate::{
        core::{ElementRead, ElementType, ExtendedElement, GenericElement, QuantizedParam, QuantizedWeight, SimpleElement, WeightType},
        RowId,
    };

    use super::{PostingList, PostingListBuilder};

    pub(super) fn mock_build_simple_posting<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        elements: Vec<(RowId, f32)>,
    ) -> (PostingList<TW>, Option<QuantizedParam>) {
        let mut builder = PostingListBuilder::<OW, TW>::new(element_type, false).expect("");
        for (row_id, weight) in elements {
            builder.add(row_id, weight);
        }
        builder.build().expect("msg")
    }

    // generate random f32
    pub(super) fn generate_random_float() -> f32 {
        let mut rng = rand::thread_rng();
        rng.gen_range(0.010101..10.1111)
    }

    // generate random int value from `min` to `max`
    pub(super) fn generate_random_int(min: u32, max: u32) -> u32 {
        if min >= max {
            return min;
        } else {
            let mut rng = rand::thread_rng();
            rng.gen_range(min..=max)
        }
    }

    /// Generate a simple posting list.
    /// - `element_type`: Simple or Extended.
    /// - `count`: The number of elements.
    /// - `row_id_start`: The start row_id when generating a group of elements.
    /// - `enable_elements_sequential`: If `false`, the row_id will be generated sortly but not sequence.
    pub(super) fn build_simple_posting<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        count: usize,
        row_id_start: RowId,
        enable_elements_sequential: bool,
    ) -> (PostingList<TW>, Option<QuantizedParam>, Vec<(u32, f32)>, (Vec<(u32, TW)>, Vec<(u32, TW, TW)>)) {
        let elements: Vec<(u32, f32)> = generate_raw_elements(count, row_id_start, enable_elements_sequential);
        let mut builder = PostingListBuilder::<OW, TW>::new(element_type, false).expect("");

        for (row_id, weight) in elements.clone() {
            builder.add(row_id, weight);
        }

        let result = builder.build();
        assert!(result.is_ok());
        let (posting, quantized_param) = result.unwrap();

        let (elements_simple, elements_extended) = match element_type {
            ElementType::SIMPLE => (posting.elements.iter().map(|e| (e.row_id(), e.weight())).collect::<Vec<_>>(), vec![]),
            ElementType::EXTENDED => (vec![], posting.elements.iter().map(|e| (e.row_id(), e.weight(), e.max_next_weight())).collect::<Vec<_>>()),
        };

        (posting, quantized_param, elements, (elements_simple, elements_extended))
    }

    pub(super) fn build_simple_posting_from_elements<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        elements: Vec<(RowId, f32)>,
    ) -> (PostingList<TW>, Option<QuantizedParam>) {
        let mut builder = PostingListBuilder::<OW, TW>::new(element_type, false).expect("");
        for (row_id, weight) in elements {
            builder.add(row_id, weight);
        }
        let (posting, param) = builder.build().expect("");
        (posting, param)
    }

    // Only for testing.
    pub(super) fn expect_posting_with_simple_elements<OW: QuantizedWeight, TW: QuantizedWeight>(elements: Vec<(RowId, f32)>) -> (PostingList<TW>, Option<QuantizedParam>) {
        let need_quantized = OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;

        let mut elements = elements;
        elements.sort_by(|l, r| l.0.cmp(&r.0));

        // Assume that the `elements` is sorted by `row_id`.
        let (min, max) = match elements.iter().map(|(_, w)| w).minmax() {
            itertools::MinMaxResult::NoElements => (OW::MINIMUM(), OW::MINIMUM()),
            itertools::MinMaxResult::OneElement(&e) => (OW::from_f32(e), OW::from_f32(e)),
            itertools::MinMaxResult::MinMax(&min, &max) => (OW::from_f32(min), OW::from_f32(max)),
        };
        let quantized_param = match need_quantized {
            true => {
                if min == OW::MINIMUM() && max == OW::MINIMUM() {
                    Some(QuantizedParam::default())
                } else {
                    Some(OW::gen_quantized_param(min, max))
                }
            }
            false => None,
        };

        let mut posting_elements: Vec<GenericElement<TW>> = Vec::new();
        for (row_id, weight) in elements {
            let generic_element: GenericElement<OW> = SimpleElement { row_id, weight: OW::from_f32(weight) }.into();
            if need_quantized {
                posting_elements.push(generic_element.quantize_with_param(quantized_param.unwrap()));
            } else {
                posting_elements.push(generic_element.type_convert::<TW>());
            }
        }

        let posting = PostingList::<TW> { elements: posting_elements, element_type: ElementType::SIMPLE };

        (posting, quantized_param)
    }

    // Only for testing.
    pub(super) fn expect_posting_with_extended_elements<OW: QuantizedWeight, TW: QuantizedWeight>(elements: Vec<(RowId, f32, f32)>) -> (PostingList<TW>, Option<QuantizedParam>) {
        let need_quantized = OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        // Keep the same logic with SparseIndex, ExtendedElement can't be quantized.
        assert_eq!(need_quantized, false);

        let mut elements = elements;
        elements.sort_by(|l, r| l.0.cmp(&r.0));

        // Assume that the `elements` is sorted by `row_id`.
        let mut posting_elements: Vec<GenericElement<TW>> = Vec::new();
        for (row_id, weight, max_next_weight) in elements {
            let generic_element: GenericElement<OW> = ExtendedElement { row_id, weight: OW::from_f32(weight), max_next_weight: OW::from_f32(max_next_weight) }.into();
            posting_elements.push(generic_element.type_convert::<TW>());
        }
        let posting = PostingList::<TW> { elements: posting_elements, element_type: ElementType::EXTENDED };

        (posting, None)
    }

    /// When `sequential` is true, the row_id will be generated sortly but not sequence.
    pub(super) fn generate_raw_elements(count: usize, row_id_start: RowId, sequential: bool) -> Vec<(u32, f32)> {
        let mut elements: Vec<(u32, f32)> = Vec::new();
        // Boundary
        if count == 0 {
            return elements;
        }
        // Init first element
        elements.push((row_id_start, format!("{:.3}", generate_random_float()).parse::<f32>().unwrap()));

        for _ in (row_id_start + 1)..(row_id_start + count as RowId) {
            let row_id = elements.last().unwrap().0
                + match sequential {
                    true => 1,
                    false => generate_random_int(1, 128),
                };
            elements.push((row_id as u32, format!("{:.3}", generate_random_float()).parse::<f32>().unwrap()));
        }
        elements
    }
}
