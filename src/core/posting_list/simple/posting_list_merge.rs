use log::error;
use crate::{
    core::{posting_list::errors::PostingListError, Element, ElementType, GenericElement, QuantizedParam, QuantizedWeight, WeightType},
    RowId,
};
use super::PostingList;

/// 在 merge 时需要传入 element type 类型，目前支持对 simple 和 extended 两种类型进行 merge
/// 其中 simple 类型支持量化；extended 类型不支持量化
/// simple 合并过程从前往后，extended 类型合并从后往前
/// 这是一个简单的合并策略，后面可以考虑使用策略模式去集成
pub struct PostingListMerger;

impl PostingListMerger {
    fn calculate_quantized_param<OW: QuantizedWeight>(
        min_weight: Option<OW>,
        max_weight: Option<OW>
    ) -> QuantizedParam {
        match (min_weight, max_weight) {
            (Some(min), Some(max)) => OW::gen_quantized_param(min, max),
            _ => QuantizedParam::default(),
        }
    }

    fn build_posting_list<OW: QuantizedWeight, TW: QuantizedWeight>(
        merged: PostingList<OW>,
        quantized_param: Option<QuantizedParam>,
        use_quantized: bool
    ) -> PostingList<TW> {
        if use_quantized {
            PostingList {
                elements: merged.elements.into_iter()
                    .map(|e| e.quantized_with_param::<TW>(quantized_param.unwrap()))
                    .collect(),
                element_type: ElementType::SIMPLE,
            }
        } else {
            unsafe { std::mem::transmute(merged) }
        }
    }


    // simple posting 不需要计算 max_next_weight，只需要考虑quantized
    fn merge_simple_postings<OW: QuantizedWeight, TW: QuantizedWeight>(
        lists: &[Vec<GenericElement<OW>>]
    ) -> Result<(PostingList<TW>, Option<QuantizedParam>), PostingListError> {
        // Boundary.
        let use_quantized = OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if !use_quantized && OW::weight_type()!=TW::weight_type() {
            let error_msg = "Merging for `ElementType::SIMPLE` without quantized, weight_type should keep same.";
            error!("{}", error_msg);
            return Err(PostingListError::MergeError(error_msg.to_string()));
        }

        let mut merged: PostingList<OW> = PostingList::<OW>::new(ElementType::SIMPLE);
        // quantized variables
        let mut min_weight: Option<OW> = None;
        let mut max_weight: Option<OW> = None;
        let mut quantized_param: Option<QuantizedParam> = None;

        // `cursors` has recordes each posting's cursor for iteration.
        let mut cursors: Vec<usize> = vec![0; lists.len()];
        let mut merged_count: usize = 0;
        let total_elements: usize = lists.iter().map(|list|list.len()).sum();

        while merged_count<total_elements {
            let min_row_id_posting_idx = cursors.iter()
                .enumerate()
                .filter(|&(i, &index)| index < lists[i].len())
                .min_by_key(|&(i, &index)| lists[i][index].row_id())
                .map(|(i, _)| i);

            // Processing the PostingList (this PostingList contains min_row_id)
            if let Some(posting_idx) = min_row_id_posting_idx {
                let mut element = lists[posting_idx][cursors[posting_idx]].clone();
                // Boundary
                if element.element_type() != ElementType::SIMPLE {
                    let error_msg = "During merging process, the PostingElement type can only be `ElementType::SIMPLE`";
                    error!("{}", error_msg);
                    return Err(PostingListError::MergeError(error_msg.to_string()));
                }
                merged.elements.push(element.clone());
                // update cur_max_next_weight
                if use_quantized{
                    match min_weight {
                        Some(min) => min_weight = Some(min.min(element.weight())),
                        None => min_weight = Some(element.weight()),
                    }
                    match max_weight {
                        Some(max) => max_weight = Some(max.max(element.weight())),
                        None => max_weight = Some(element.weight()),
                    }
                }
                // 步进 cursor
                cursors[posting_idx] += 1;
                merged_count += 1;
            }
        }

        if use_quantized {
            quantized_param = Some(Self::calculate_quantized_param(min_weight, max_weight));
        }
        let posting_list = Self::build_posting_list(merged, quantized_param.clone(), use_quantized);
        Ok((posting_list, quantized_param))
    }


    fn merge_extended_postings<OW: QuantizedWeight, TW: QuantizedWeight>(
        lists: &[Vec<GenericElement<OW>>]
    ) -> Result<(PostingList<TW>, Option<QuantizedParam>), PostingListError> {
        // Boundary.
        let use_quantized = OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if use_quantized {
            let error_msg = "`ElementType::EXTENDED` not support quantized! Can't execute merge.";
            error!("{}", error_msg);
            return Err(PostingListError::MergeError(error_msg.to_string()));
        }
        if OW::weight_type()!=TW::weight_type() {
            let error_msg = "Quantized not supported for `ElementType::EXTENDED`, weight_type should keep same.";
            error!("{}", error_msg);
            return Err(PostingListError::MergeError(error_msg.to_string()));
        }

        let mut merged: PostingList<OW> = PostingList::<OW>::new(ElementType::EXTENDED);
        let mut cursors_rev: Vec<usize> = lists.iter().map(|list: &Vec<GenericElement<OW>>| list.len()).collect::<Vec<_>>();
        let mut max_next_weight: OW = OW::MINIMUM();

        // When all PostingList indices become ZERO, it means that all PostingList pending to merge has been finished.
        while cursors_rev.iter().any(|&i| i > 0) {
            // `max_index` is used to record the index of the PostingList where the current maximum `row_id` is located.
            let mut max_row_id_posting_idx: Option<usize> = None;

            // `max_row_id` is used to record: in current position `i`, max_row_id in postings.
            let mut max_row_id: Option<RowId> = None;

            // Finding from all PostingList, get current max row_id ang it's located position in PostingLists.
            for (posting_idx, &cursor) in cursors_rev.iter().enumerate() {
                if cursor > 0 {
                    let cur_row_id = lists[posting_idx][cursor - 1].row_id();
                    if max_row_id.is_none() || cur_row_id > max_row_id.unwrap() {
                        max_row_id = Some(cur_row_id);
                        max_row_id_posting_idx = Some(posting_idx);
                    }
                }
            }

            // Processing the PostingList (this PostingList contains max_row_id)
            if let Some(posting_idx) = max_row_id_posting_idx {
                // TODO enum 可以直接使用原始类型进行注解？这里的注解是 Generic，改成 Extended 可以吗？
                let mut element: GenericElement<OW> = lists[posting_idx][cursors_rev[posting_idx] - 1].clone();
                // Boundary
                if element.element_type() != ElementType::EXTENDED {
                    let error_msg = "During merging process, the PostingElement type can only be `ElementType::EXTENDED`";
                    error!("{}", error_msg);
                    return Err(PostingListError::MergeError(error_msg.to_string()));
                }
                element.update_max_next_weight(max_next_weight);
                merged.elements.push(element.clone());

                max_next_weight = max_next_weight.max(element.weight());

                cursors_rev[posting_idx] -= 1;
            }
        }
        // 将结果反转，使其按照 row_id 从小到大排序
        merged.elements.reverse();

        let tw_posting_list: PostingList<TW> = unsafe { std::mem::transmute(merged) };
        Ok((tw_posting_list, None))
    }




    /// input a group of postings, they are in the same dim-id.
    /// 这里执行的 merge 是对同一个 dim 下面对应的所有 posting list 执行的 merge，所以说 merge 操作并不会遇到相同的 row_id
    pub fn merge_posting_lists<OW: QuantizedWeight, TW: QuantizedWeight>(
        lists: &Vec<Vec<GenericElement<OW>>>, element_type: ElementType
    ) -> (PostingList<TW>, Option<QuantizedParam>) {
        match element_type {
            ElementType::SIMPLE => {
                Self::merge_simple_postings(lists)
            },
            ElementType::EXTENDED => {
                Self::merge_extended_postings(lists)
            },
            _ => panic!("Not supported element type for merge!")
        }
    }
}

#[cfg(test)]
mod tests {
    use core::f32;

    use crate::core::{ElementType, ExtendedElement, PostingList};

    use super::PostingListMerger;

    /// mock 7 postings for the same dim-id.
    /// mock 7 postings for the same dim-id.
    fn get_mocked_postings() -> (Vec<Vec<ExtendedElement<f32>>>, PostingList<f32>) {
        let lists: Vec<Vec<ExtendedElement<f32>>> = vec![
            vec![], // 0
            vec![
                // 1
                ExtendedElement { row_id: 0, weight: 2.3, max_next_weight: 2.8 },
                ExtendedElement { row_id: 4, weight: 1.4, max_next_weight: 2.8 },
                ExtendedElement { row_id: 5, weight: 2.1, max_next_weight: 2.8 },
                ExtendedElement { row_id: 9, weight: 2.8, max_next_weight: 1.2 },
                ExtendedElement { row_id: 12, weight: 1.2, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![], // 2
            vec![
                // 3
                ExtendedElement { row_id: 1, weight: 1.2, max_next_weight: 4.3 },
                ExtendedElement { row_id: 3, weight: 4.3, max_next_weight: 3.1 },
                ExtendedElement { row_id: 8, weight: 2.9, max_next_weight: 3.1 },
                ExtendedElement { row_id: 10, weight: 1.8, max_next_weight: 3.1 },
                ExtendedElement { row_id: 14, weight: 3.1, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![
                // 4
                ExtendedElement { row_id: 2, weight: 0.3, max_next_weight: 4.2 },
                ExtendedElement { row_id: 11, weight: 3.4, max_next_weight: 4.2 },
                ExtendedElement { row_id: 13, weight: 2.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 15, weight: 1.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 17, weight: 1.5, max_next_weight: 4.2 },
                ExtendedElement { row_id: 21, weight: 3.8, max_next_weight: 4.2 },
                ExtendedElement { row_id: 24, weight: 4.2, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![
                // 5
                ExtendedElement { row_id: 6, weight: 2.3, max_next_weight: 3.4 },
                ExtendedElement { row_id: 7, weight: 3.4, max_next_weight: 3.2 },
                ExtendedElement { row_id: 16, weight: 3.2, max_next_weight: 2.8 },
                ExtendedElement { row_id: 19, weight: 2.8, max_next_weight: 1.9 },
                ExtendedElement { row_id: 20, weight: 1.9, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![
                // 6
                ExtendedElement { row_id: 18, weight: 2.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 22, weight: 4.2, max_next_weight: 4.1 },
                ExtendedElement { row_id: 23, weight: 3.9, max_next_weight: 4.1 },
                ExtendedElement { row_id: 25, weight: 1.6, max_next_weight: 4.1 },
                ExtendedElement { row_id: 26, weight: 1.2, max_next_weight: 4.1 },
                ExtendedElement { row_id: 30, weight: 4.1, max_next_weight: f32::NEG_INFINITY },
            ],
        ];

        let merged = PostingList {
            elements: vec![
                ExtendedElement { row_id: 0, weight: 2.3, max_next_weight: 4.3 },
                ExtendedElement { row_id: 1, weight: 1.2, max_next_weight: 4.3 },
                ExtendedElement { row_id: 2, weight: 0.3, max_next_weight: 4.3 },
                ExtendedElement { row_id: 3, weight: 4.3, max_next_weight: 4.2 },
                ExtendedElement { row_id: 4, weight: 1.4, max_next_weight: 4.2 },
                ExtendedElement { row_id: 5, weight: 2.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 6, weight: 2.3, max_next_weight: 4.2 },
                ExtendedElement { row_id: 7, weight: 3.4, max_next_weight: 4.2 },
                ExtendedElement { row_id: 8, weight: 2.9, max_next_weight: 4.2 },
                ExtendedElement { row_id: 9, weight: 2.8, max_next_weight: 4.2 },
                ExtendedElement { row_id: 10, weight: 1.8, max_next_weight: 4.2 },
                ExtendedElement { row_id: 11, weight: 3.4, max_next_weight: 4.2 },
                ExtendedElement { row_id: 12, weight: 1.2, max_next_weight: 4.2 },
                ExtendedElement { row_id: 13, weight: 2.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 14, weight: 3.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 15, weight: 1.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 16, weight: 3.2, max_next_weight: 4.2 },
                ExtendedElement { row_id: 17, weight: 1.5, max_next_weight: 4.2 },
                ExtendedElement { row_id: 18, weight: 2.1, max_next_weight: 4.2 },
                ExtendedElement { row_id: 19, weight: 2.8, max_next_weight: 4.2 },
                ExtendedElement { row_id: 20, weight: 1.9, max_next_weight: 4.2 },
                ExtendedElement { row_id: 21, weight: 3.8, max_next_weight: 4.2 },
                ExtendedElement { row_id: 22, weight: 4.2, max_next_weight: 4.2 },
                ExtendedElement { row_id: 23, weight: 3.9, max_next_weight: 4.2 },
                ExtendedElement { row_id: 24, weight: 4.2, max_next_weight: 4.1 },
                ExtendedElement { row_id: 25, weight: 1.6, max_next_weight: 4.1 },
                ExtendedElement { row_id: 26, weight: 1.2, max_next_weight: 4.1 },
                ExtendedElement { row_id: 30, weight: 4.1, max_next_weight: f32::NEG_INFINITY },
            ],
            element_type: ElementType::EXTENDED,
        };
        return (lists, merged);
    }
    #[test]
    fn test_merge_posting_lists() {
        let postings = get_mocked_postings();
        let result = PostingListMerger::merge_posting_lists::<f32, f32>(&postings.0, ElementType::EXTENDED);
        assert_eq!(result.0, postings.1);
    }
}
