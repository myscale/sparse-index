use std::marker::PhantomData;

use crate::core::posting_list::traits::{PostingElementEx, PostingListIteratorTrait};
use crate::core::{QuantizedParam, QuantizedWeight, WeightType};
use crate::RowId;

use super::PostingList;

// OW 是 posting 内部存储的 weight 类型
// TW 是反量化之后的 weight
#[derive(Debug, Clone)]
pub struct PostingListIterator<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    pub posting: &'a [PostingElementEx<OW>],
    pub quantized_param: Option<QuantizedParam>,
    pub cursor: usize,
    _tw: PhantomData<TW>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIterator<'a, OW, TW> {
    pub fn new(
        posting: &'a [PostingElementEx<OW>],
        quantized_param: Option<QuantizedParam>,
    ) -> PostingListIterator<'a, OW, TW> {
        PostingListIterator {
            posting,
            quantized_param,
            cursor: 0,
            _tw: PhantomData,
        }
    }

    fn convert_type(&self, raw_element: &PostingElementEx<OW>) -> PostingElementEx<TW> {
        if self.quantized_param.is_none() {
            assert_eq!(OW::weight_type(), TW::weight_type());

            let weight_convert = TW::from_f32(OW::to_f32(raw_element.weight));
            let max_next_weight_convert = TW::from_f32(OW::to_f32(raw_element.max_next_weight));
            let converted_element: PostingElementEx<TW> = PostingElementEx {
                row_id: raw_element.row_id,
                weight: weight_convert,
                max_next_weight: max_next_weight_convert,
            };

            return converted_element;
        } else {
            assert_eq!(OW::weight_type(), WeightType::WeightU8);
            let param: QuantizedParam = self.quantized_param.unwrap();
            let converted: PostingElementEx<TW> = PostingElementEx::<TW> {
                row_id: raw_element.row_id,
                weight: TW::unquantize_with_param(OW::to_u8(raw_element.weight), param),
                max_next_weight: TW::unquantize_with_param(
                    OW::to_u8(raw_element.max_next_weight),
                    param,
                ),
            };
            return converted;
        }
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIteratorTrait<OW, TW>
    for PostingListIterator<'a, OW, TW>
{
    fn peek(&mut self) -> Option<PostingElementEx<TW>> {
        let element_opt: Option<&PostingElementEx<OW>> = self.posting.get(self.cursor);
        if element_opt.is_none() {
            return None;
        } else {
            let element: PostingElementEx<OW> = element_opt.unwrap().clone();
            return Some(self.convert_type(&element));
        }
    }

    fn last_id(&self) -> Option<RowId> {
        self.posting.last().map(|e| e.row_id)
    }

    fn skip_to(&mut self, row_id: RowId) -> Option<PostingElementEx<TW>> {
        if self.cursor >= self.posting.len() {
            return None;
        }

        // 查找第一个 row_id ≥ 目标 row_id 的元素位置
        let next_element: Result<usize, usize> =
            self.posting[self.cursor..].binary_search_by(|e| e.row_id.cmp(&row_id));

        match next_element {
            Ok(found_offset) => {
                self.cursor += found_offset;
                let raw_element: PostingElementEx<OW> = self.posting[self.cursor].clone();
                return Some(self.convert_type(&raw_element));
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

    fn for_each_till_row_id(&mut self, row_id: RowId, mut f: impl FnMut(&PostingElementEx<TW>)) {
        let mut cursor = self.cursor;
        for element in &self.posting[cursor..] {
            if element.row_id > row_id {
                break;
            }
            let converted: PostingElementEx<TW> = self.convert_type(element);
            f(&converted);
            cursor += 1;
        }
        self.cursor = cursor;
    }
}
