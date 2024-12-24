use std::marker::PhantomData;

use crate::core::{
    ElementRead, ElementSlice, GenericElement, GenericElementSlice, PostingListIter,
    QuantizedParam, QuantizedWeight,
};
use crate::RowId;

#[derive(Debug, Clone)]
pub struct PostingListIterator<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    // pub posting: &'a [GenericElement<TW>],
    pub generic_elements_slice: GenericElementSlice<'a, TW>,
    pub quantized_param: Option<QuantizedParam>,
    pub cursor: usize,
    _ow: PhantomData<OW>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIterator<'a, OW, TW> {
    pub fn new(
        // posting: &'a [GenericElement<TW>],
        generic_elements_slice: GenericElementSlice<'a, TW>,
        quantized_param: Option<QuantizedParam>,
    ) -> PostingListIterator<'a, OW, TW> {
        PostingListIterator { generic_elements_slice, quantized_param, cursor: 0, _ow: PhantomData }
    }

    fn type_convert(&self, raw_element: &GenericElement<TW>) -> GenericElement<OW> {
        raw_element.convert_or_unquantize(self.quantized_param)
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIter<OW, TW>
    for PostingListIterator<'a, OW, TW>
{
    fn peek(&mut self) -> Option<GenericElement<OW>> {
        // let element_opt: Option<&GenericElement<TW>> = self.posting.get(self.cursor);
        let element_opt = self.generic_elements_slice.get_opt(self.cursor);
        if element_opt.is_none() {
            return None;
        } else {
            let element = element_opt.unwrap().to_owned();
            return Some(self.type_convert(&element));
        }
    }

    fn last_id(&self) -> Option<RowId> {
        self.generic_elements_slice.last_opt().map(|e| e.row_id() as RowId)
    }

    fn skip_to(&mut self, row_id: RowId) -> Option<GenericElement<OW>> {
        if self.cursor >= self.generic_elements_slice.length() {
            return None;
        }

        // find the first position: row_id ≥ target_row_id
        // let next_element: Result<usize, usize> = self.posting[self.cursor..].binary_search_by(|e| e.row_id().cmp(&row_id));
        let next_element =
            self.generic_elements_slice.slice_from(self.cursor..).binary_search_by_row_id(row_id);

        match next_element {
            Ok(found_offset) => {
                self.cursor += found_offset;
                // let raw_element: GenericElement<TW> = self.posting[self.cursor].clone();
                let raw_element =
                    self.generic_elements_slice.get_opt(self.cursor).unwrap().to_owned();
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
            // let element = element.to_owned();
            // TODO: 每次的 type convert 都需要写一个 element，可以尝试加速，使用一些 unsafe 操作之类的
            let converted: GenericElement<OW> = self.type_convert(&element.to_owned());
            f(&converted);
            cursor += 1;
        }
        self.cursor = cursor;
    }
}
