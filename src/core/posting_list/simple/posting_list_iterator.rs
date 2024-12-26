use std::marker::PhantomData;

use crate::core::{ElementRead, ElementSlice, GenericElement, GenericElementSlice, PostingListIter, QuantizedParam, QuantizedWeight};
use crate::RowId;

#[derive(Debug, Clone)]
pub struct PostingListIterator<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    pub generic_elements_slice: GenericElementSlice<'a, TW>,
    pub quantized_param: Option<QuantizedParam>,
    pub cursor: usize,
    _ow: PhantomData<OW>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIterator<'a, OW, TW> {
    pub fn new(generic_elements_slice: GenericElementSlice<'a, TW>, quantized_param: Option<QuantizedParam>) -> PostingListIterator<'a, OW, TW> {
        PostingListIterator { generic_elements_slice, quantized_param, cursor: 0, _ow: PhantomData }
    }

    fn type_convert(&self, raw_element: &GenericElement<TW>) -> GenericElement<OW> {
        raw_element.convert_or_unquantize(self.quantized_param)
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIter<OW, TW> for PostingListIterator<'a, OW, TW> {
    fn peek(&mut self) -> Option<GenericElement<OW>> {
        self.generic_elements_slice.get_opt(self.cursor).map(|element| self.type_convert(&element.to_owned()))
    }

    fn last_id(&self) -> Option<RowId> {
        self.generic_elements_slice.last_opt().map(|e| e.row_id() as RowId)
    }

    fn skip_to(&mut self, row_id: RowId) -> Option<GenericElement<OW>> {
        if self.cursor >= self.generic_elements_slice.length() {
            return None;
        }

        // find the first position: row_id ≥ target_row_id
        let next_element = self.generic_elements_slice.slice_from(self.cursor..).binary_search_by_row_id(row_id);

        match next_element {
            Ok(found_offset) => {
                self.cursor += found_offset;
                let raw_element = self.generic_elements_slice.get_opt(self.cursor).unwrap().to_owned();
                return Some(self.type_convert(&raw_element));
            }
            Err(insert_index) => {
                self.cursor += insert_index;
                None
            }
        }
    }

    fn skip_to_end(&mut self) {
        self.cursor = self.generic_elements_slice.length();
    }

    fn remains(&self) -> usize {
        self.generic_elements_slice.length() - self.cursor
    }

    fn cursor(&self) -> usize {
        self.cursor
    }

    fn for_each_till_row_id(&mut self, row_id: RowId, mut f: impl FnMut(&GenericElement<OW>)) {
        let mut cursor = self.cursor;

        for element in self.generic_elements_slice.slice_from(cursor..).generic_iter() {
            if element.row_id() > row_id {
                break;
            }
            // TODO: improve performance
            let converted: GenericElement<OW> = self.type_convert(&element.to_owned());
            f(&converted);
            cursor += 1;
        }
        self.cursor = cursor;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{GenericElement, GenericElementSlice, QuantizedWeight, SimpleElement};
    use crate::RowId;

    fn create_simple_elements<W: QuantizedWeight>(elements: Vec<(RowId, W)>) -> Vec<SimpleElement<W>> {
        elements.into_iter().map(|(row_id, weight)| SimpleElement { row_id, weight }).collect::<Vec<_>>()
    }

    #[test]
    fn test_peek() {
        let simple_elements = create_simple_elements::<f32>(vec![(6, 70.0), (14, 45.0), (17, 10.0), (18, 30.0), (21, 20.0)]);
        // test without quantized.
        {
            let generic_elements_slice = GenericElementSlice::from_simple_slice(&simple_elements);
            let mut iterator = PostingListIterator::<f32, f32>::new(generic_elements_slice, None);

            let peeked = iterator.peek().unwrap();
            assert_eq!(peeked.row_id(), 6);
            assert_eq!(peeked.weight(), 70.0);
        }
        // test with quantized.
        {
            let param = f32::gen_quantized_param(10.0, 70.0);
            let generic_elements: Vec<GenericElement<f32>> = simple_elements.clone().into_iter().map(|e| e.into()).collect::<Vec<GenericElement<f32>>>();
            let quantized_simple_elements: Vec<SimpleElement<u8>> = generic_elements.iter().map(|e| e.quantize_with_param::<u8>(param).as_simple().unwrap().clone()).collect();
            let generic_elements_slice = GenericElementSlice::from_simple_slice(&quantized_simple_elements);
            let mut iterator = PostingListIterator::<f32, u8>::new(generic_elements_slice, Some(param));

            let peeked = iterator.peek().unwrap();
            assert_eq!(peeked.row_id(), 6);
            assert_eq!(peeked.weight(), 70.0);
        }
    }

    #[test]
    fn test_skip_to() {
        let simple_elements = create_simple_elements::<f32>(vec![(6, 70.0), (14, 45.0), (17, 10.0), (18, 30.0), (21, 20.0)]);

        let generic_elements_slice = GenericElementSlice::from_simple_slice(&simple_elements);
        let mut iterator = PostingListIterator::<f32, f32>::new(generic_elements_slice, None);

        let skipped = iterator.skip_to(6);
        assert!(skipped.is_some());
        assert_eq!(skipped.unwrap().row_id(), 6);

        let skipped = iterator.skip_to(18);
        assert!(skipped.is_some());
        assert_eq!(skipped.unwrap().row_id(), 18);

        let skipped = iterator.skip_to(21);
        assert!(skipped.is_some());
        assert_eq!(skipped.unwrap().row_id(), 21);

        // skip to overflow element
        let skipped = iterator.skip_to(24);
        assert!(skipped.is_none());

        // skip to old elements
        let skipped = iterator.skip_to(14);
        assert!(skipped.is_none());
    }

    #[test]
    fn test_last_id() {
        let simple_elements = create_simple_elements::<f32>(vec![(6, 70.0), (14, 45.0), (17, 10.0), (18, 30.0), (21, 20.0)]);

        let generic_elements_slice = GenericElementSlice::from_simple_slice(&simple_elements);
        let iterator = PostingListIterator::<f32, f32>::new(generic_elements_slice, None);

        let last_id = iterator.last_id();
        assert_eq!(last_id, Some(21));
    }

    #[test]
    fn test_remains() {
        let simple_elements = create_simple_elements::<f32>(vec![(6, 70.0), (14, 45.0), (17, 10.0), (18, 30.0), (21, 20.0)]);

        let generic_elements_slice = GenericElementSlice::from_simple_slice(&simple_elements);
        let mut iterator = PostingListIterator::<f32, f32>::new(generic_elements_slice, None);

        assert_eq!(iterator.remains(), 5);

        // Skip one element
        iterator.skip_to(17);
        assert_eq!(iterator.remains(), 3);
    }

    #[test]
    fn test_for_each_till_row_id() {
        let simple_elements = create_simple_elements::<f32>(vec![(6, 70.0), (14, 45.0), (17, 10.0), (18, 30.0), (21, 20.0)]);

        let generic_elements_slice = GenericElementSlice::from_simple_slice(&simple_elements);
        let mut iterator = PostingListIterator::<f32, f32>::new(generic_elements_slice, None);

        let mut values = Vec::new();
        iterator.for_each_till_row_id(18, |element| {
            values.push(element.weight());
        });

        assert_eq!(values, vec![70.0, 45.0, 10.0, 30.0]);
    }
}
