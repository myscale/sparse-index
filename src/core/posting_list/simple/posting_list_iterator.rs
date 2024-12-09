use std::marker::PhantomData;

use crate::core::{Element, GenericElement, PostingListIter, QuantizedParam, QuantizedWeight, WeightType};
use crate::RowId;

#[derive(Debug, Clone)]
pub struct PostingListIterator<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    pub posting: &'a [GenericElement<TW>],
    pub quantized_param: Option<QuantizedParam>,
    pub cursor: usize,
    _tw: PhantomData<TW>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIterator<'a, OW, TW> {
    pub fn new(
        posting: &'a [GenericElement<TW>],
        quantized_param: Option<QuantizedParam>,
    ) -> PostingListIterator<'a, OW, TW> {
        PostingListIterator { posting, quantized_param, cursor: 0, _tw: PhantomData }
    }

    fn type_convert(&self, raw_element: &GenericElement<TW>) -> GenericElement<OW> {
        raw_element.type_convert::<OW>(self.quantized_param)
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIter<OW, TW> for PostingListIterator<'a, OW, TW>
{
    fn peek(&mut self) -> Option<GenericElement<OW>> {
        let element_opt: Option<&GenericElement<TW>> = self.posting.get(self.cursor);
        if element_opt.is_none() {
            return None;
        } else {
            let element: GenericElement<TW> = element_opt.unwrap().clone();
            return Some(self.type_convert(&element));
        }
    }

    fn last_id(&self) -> Option<RowId> {
        self.posting.last().map(|e| e.row_id())
    }

    fn skip_to(&mut self, row_id: RowId) -> Option<GenericElement<OW>> {
        if self.cursor >= self.posting.len() {
            return None;
        }

        // find the first position: row_id ≥ target_row_id
        let next_element: Result<usize, usize> =
            self.posting[self.cursor..].binary_search_by(|e| e.row_id.cmp(&row_id));

        match next_element {
            Ok(found_offset) => {
                self.cursor += found_offset;
                let raw_element: GenericElement<TW> = self.posting[self.cursor].clone();
                return Some(self.type_convert(&raw_element));
            }
            Err(insert_index) => {
                self.cursor += insert_index;
                None
            }
        }
    }

    fn skip_to_end(&mut self) {
        self.cursor = self.posting.len();
    }

    fn remains(&self) -> usize {
        self.posting.len() - self.cursor
    }

    fn cursor(&self) -> usize {
        self.cursor
    }

    fn for_each_till_row_id(&mut self, row_id: RowId, mut f: impl FnMut(&GenericElement<OW>)) {
        let mut cursor = self.cursor;
        for element in &self.posting[cursor..] {
            if element.row_id() > row_id {
                break;
            }
            let converted: GenericElement<OW> = self.type_convert(element);
            f(&converted);
            cursor += 1;
        }
        self.cursor = cursor;
    }
}
