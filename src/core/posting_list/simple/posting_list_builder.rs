use itertools::Itertools;
use log::{error, warn};
use std::{marker::PhantomData, mem::size_of};

use super::super::traits::PostingElementEx;
use super::PostingList;
use crate::{
    core::{DimWeight, QuantizedParam, QuantizedWeight, WeightType, DEFAULT_MAX_NEXT_WEIGHT},
    RowId,
};

// OW 表示原始数据类型，TW 表示 build 之后得到的 Posting 数据类型
#[derive(Default)]
pub struct PostingListBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    posting: PostingList<OW>,
    propagate_while_upserting: bool,
    finally_sort: bool,
    finally_propagate: bool,

    pub(super) _phantom_ow: PhantomData<OW>,
    pub(super) _phantom_tw: PhantomData<TW>,
}

// Builder pattern
impl<OW: QuantizedWeight, TW: QuantizedWeight> PostingListBuilder<OW, TW> {
    pub fn new() -> Self {
        Self {
            posting: PostingList::new(),
            finally_sort: false,
            propagate_while_upserting: false,
            finally_propagate: true,

            _phantom_ow: PhantomData,
            _phantom_tw: PhantomData,
        }
    }

    pub fn with_finally_sort(mut self, sort: bool) -> Self {
        self.finally_sort = sort;
        self
    }

    pub fn with_finally_propagate(mut self, propagate: bool) -> Self {
        self.finally_propagate = propagate;
        self
    }

    pub fn with_propagate_while_upserting(mut self, propagate: bool) -> Self {
        self.propagate_while_upserting = propagate;
        self
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> PostingListBuilder<OW, TW> {
    /// ## brief
    /// add a new Element to the posting list.
    /// ## return
    /// bool: `ture` means the `insert` operation, `false` means `update`.
    pub fn add(&mut self, row_id: RowId, weight: DimWeight) -> bool {
        if self.propagate_while_upserting {
            self.posting
                .upsert_with_propagate(PostingElementEx::<OW>::new(row_id, weight))
        } else {
            self.posting
                .upsert(PostingElementEx::<OW>::new(row_id, weight))
                .1
        }
    }

    /// ## brief
    /// 返回 elements 占据的内存字节大小
    pub fn memory_usage(&self) -> usize {
        self.posting.len() * size_of::<PostingElementEx<OW>>()
    }

    // 根据是否开启 quantized 来判断输出类型
    pub fn build(mut self) -> (PostingList<TW>, Option<QuantizedParam>) {
        let need_quantized =
            TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;

        if need_quantized {
            assert_eq!(TW::weight_type(), WeightType::WeightU8);
        } else {
            assert_eq!(OW::weight_type(), TW::weight_type());
        }

        // 根据 row_id 进行排序
        if self.finally_sort {
            self.posting.elements.sort_unstable_by_key(|e| e.row_id);
        }
        // 检查在一个 PostingList 中是否存在重复的 row_id, 以及这个 Posting 是否是正确排序了的
        #[cfg(debug_assertions)]
        {
            if let Some(res) = self
                .posting
                .elements
                .windows(2)
                .find(|e| e[0].row_id >= e[1].row_id)
            {
                let error_msg = format!("Duplicated row_id, or Posting is not sorted by row_id correctly, left: {:?}, right: {:?}.", res[0], res[1]);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
        // 从后往前修改每个 element 的 max_next_weight
        let mut quantized_param: Option<QuantizedParam> = None;
        if self.finally_propagate {
            let mut max_next_weight: OW = OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT);
            let mut min_weight = OW::from_f32(DimWeight::INFINITY);
            let mut max_weight = OW::from_f32(DimWeight::NEG_INFINITY);

            for element in self.posting.elements.iter_mut().rev() {
                element.max_next_weight = max_next_weight;
                max_next_weight = max_next_weight.max(element.weight);

                min_weight = OW::min(min_weight, element.weight);
                max_weight = OW::max(max_weight, element.weight);
            }
            if need_quantized {
                quantized_param = Some(OW::gen_quantized_param(min_weight, max_weight));
            }
        } else {
            warn!("Skip propagating the Posting finally, please make sure it has already been propagated.");
            if need_quantized {
                let elements_iter = self.posting.elements.iter().map(|e| e.weight);
                let (min, max) = match elements_iter.minmax() {
                    itertools::MinMaxResult::NoElements => (OW::MINIMUM(), OW::MINIMUM()),
                    itertools::MinMaxResult::OneElement(e) => (e, e),
                    itertools::MinMaxResult::MinMax(min, max) => (min, max),
                };
                quantized_param = Some(OW::gen_quantized_param(min, max));
            }
        }

        if need_quantized {
            assert!(quantized_param.is_some());
            let mut tw_posting_list: PostingList<TW> = PostingList::<TW>::new();
            for element in self.posting.elements {
                let quantized_element_u8: PostingElementEx<u8> = PostingElementEx {
                    row_id: element.row_id,
                    weight: OW::quantize_with_param(element.weight, quantized_param.unwrap()),
                    max_next_weight: OW::quantize_with_param(
                        element.max_next_weight,
                        quantized_param.unwrap(),
                    ),
                };

                let quantized_element_convert: PostingElementEx<TW> = PostingElementEx {
                    row_id: quantized_element_u8.row_id,
                    weight: TW::unquantize_with_param(
                        quantized_element_u8.weight,
                        quantized_param.unwrap(),
                    ),
                    max_next_weight: TW::unquantize_with_param(
                        quantized_element_u8.max_next_weight,
                        quantized_param.unwrap(),
                    ),
                };

                tw_posting_list.elements.push(quantized_element_convert);
            }
            (tw_posting_list, quantized_param)
        } else {
            let tw_posting_list: PostingList<TW> = unsafe { std::mem::transmute(self.posting) };
            (tw_posting_list, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::posting_list::traits::PostingElementEx;

    // TODO 这个测试应该放到 traits 里面，验证 Element 的排序
    #[test]
    fn test_sort_unstable_by() {
        let mut elements = vec![
            PostingElementEx::<f32>::new(2, 0.9),
            PostingElementEx::<f32>::new(1, 1.2),
            PostingElementEx::<f32>::new(3, 0.2),
        ];
        elements.sort_unstable_by_key(|e| e.row_id);
        println!("{:?}", elements);
    }

    #[test]
    fn test_deref() {
        let x = 8;
        let x_r = &x;
        let x_rr = &x_r;
        println!("{}", x_r);
        println!("{}", x_rr);
    }
}
