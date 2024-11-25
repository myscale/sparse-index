use crate::{
    core::{DimWeight, PostingElementEx, QuantizedParam, QuantizedWeight, WeightType},
    RowId,
};

use super::PostingList;

pub struct PostingListMerger;

impl PostingListMerger {
    /// input a group of postings, they are in the same dim-id.
    pub fn merge_posting_lists<OW: QuantizedWeight, TW: QuantizedWeight>(
        lists: &Vec<Vec<PostingElementEx<OW>>>,
    ) -> (PostingList<TW>, Option<QuantizedParam>) {
        let use_quantized =
            OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;

        let mut merged: PostingList<OW> = PostingList { elements: Vec::new() };
        let mut min_weight = OW::from_f32(DimWeight::INFINITY);
        let mut max_weight = OW::from_f32(DimWeight::NEG_INFINITY);
        let mut quantized_param: Option<QuantizedParam> = None;
        // `indices` has recordes each posting's size, we will merge these postings from bottom.
        let mut indices: Vec<usize> = lists.iter().map(|list| list.len()).collect::<Vec<_>>();
        let mut cur_max_next_weight: OW = OW::MINIMUM();

        // When all PostingList indices become ZERO, it means that all PostingList pending to merge has been finished.
        while indices.iter().any(|&i| i > 0) {
            // `max_index` is used to record the index of the PostingList where the current maximum `row_id` is located.
            let mut max_index: Option<usize> = None;

            // `max_row_id` is used to record: in current position `i`, max_row_id in postings.
            let mut max_row_id: Option<RowId> = None;

            // Finding from all PostingList, get current max row_id ang it's located position in PostingLists.
            for (i, &index) in indices.iter().enumerate() {
                if index > 0 {
                    let cur_row_id = lists[i][index - 1].row_id;
                    if max_row_id.is_none() || cur_row_id > max_row_id.unwrap() {
                        max_index = Some(i);
                        max_row_id = Some(cur_row_id);
                    }
                }
            }

            // Processing the PostingList (this PostingList contains max_row_id)
            if let Some(max_idx) = max_index {
                // append max_row_id in merged result.
                let mut element: PostingElementEx<OW> =
                    lists[max_idx][indices[max_idx] - 1].clone();

                element.max_next_weight = cur_max_next_weight;
                merged.elements.push(element.clone());

                // update cur_max_next_weight
                cur_max_next_weight = cur_max_next_weight.max(element.weight);
                min_weight = min_weight.min(element.weight);
                max_weight = max_weight.max(element.weight);
                indices[max_idx] -= 1;
            }
        }

        // 将结果反转，使其按照 row_id 从小到大排序
        merged.elements.reverse();

        if use_quantized {
            quantized_param = Some(OW::gen_quantized_param(min_weight, max_weight));
            let mut tw_posting_list: PostingList<TW> = PostingList::<TW>::new();
            for element in merged.elements {
                let quantized_element: PostingElementEx<TW> = PostingElementEx {
                    row_id: element.row_id,
                    weight: TW::from_u8(OW::quantize_with_param(
                        element.weight,
                        quantized_param.unwrap(),
                    )),
                    max_next_weight: TW::from_u8(OW::quantize_with_param(
                        element.max_next_weight,
                        quantized_param.unwrap(),
                    )),
                };
                tw_posting_list.elements.push(quantized_element);
            }
            (tw_posting_list, quantized_param)
        } else {
            let tw_posting_list: PostingList<TW> = unsafe { std::mem::transmute(merged) };
            (tw_posting_list, quantized_param)
        }
    }
}

#[cfg(test)]
mod tests {
    use core::f32;

    use crate::core::{PostingElementEx, PostingList};

    use super::PostingListMerger;

    /// mock 7 postings for the same dim-id.
    /// mock 7 postings for the same dim-id.
    fn get_mocked_postings() -> (Vec<Vec<PostingElementEx<f32>>>, PostingList<f32>) {
        let lists: Vec<Vec<PostingElementEx<f32>>> = vec![
            vec![], // 0
            vec![
                // 1
                PostingElementEx { row_id: 0, weight: 2.3, max_next_weight: 2.8 },
                PostingElementEx { row_id: 4, weight: 1.4, max_next_weight: 2.8 },
                PostingElementEx { row_id: 5, weight: 2.1, max_next_weight: 2.8 },
                PostingElementEx { row_id: 9, weight: 2.8, max_next_weight: 1.2 },
                PostingElementEx { row_id: 12, weight: 1.2, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![], // 2
            vec![
                // 3
                PostingElementEx { row_id: 1, weight: 1.2, max_next_weight: 4.3 },
                PostingElementEx { row_id: 3, weight: 4.3, max_next_weight: 3.1 },
                PostingElementEx { row_id: 8, weight: 2.9, max_next_weight: 3.1 },
                PostingElementEx { row_id: 10, weight: 1.8, max_next_weight: 3.1 },
                PostingElementEx { row_id: 14, weight: 3.1, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![
                // 4
                PostingElementEx { row_id: 2, weight: 0.3, max_next_weight: 4.2 },
                PostingElementEx { row_id: 11, weight: 3.4, max_next_weight: 4.2 },
                PostingElementEx { row_id: 13, weight: 2.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 15, weight: 1.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 17, weight: 1.5, max_next_weight: 4.2 },
                PostingElementEx { row_id: 21, weight: 3.8, max_next_weight: 4.2 },
                PostingElementEx { row_id: 24, weight: 4.2, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![
                // 5
                PostingElementEx { row_id: 6, weight: 2.3, max_next_weight: 3.4 },
                PostingElementEx { row_id: 7, weight: 3.4, max_next_weight: 3.2 },
                PostingElementEx { row_id: 16, weight: 3.2, max_next_weight: 2.8 },
                PostingElementEx { row_id: 19, weight: 2.8, max_next_weight: 1.9 },
                PostingElementEx { row_id: 20, weight: 1.9, max_next_weight: f32::NEG_INFINITY },
            ],
            vec![
                // 6
                PostingElementEx { row_id: 18, weight: 2.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 22, weight: 4.2, max_next_weight: 4.1 },
                PostingElementEx { row_id: 23, weight: 3.9, max_next_weight: 4.1 },
                PostingElementEx { row_id: 25, weight: 1.6, max_next_weight: 4.1 },
                PostingElementEx { row_id: 26, weight: 1.2, max_next_weight: 4.1 },
                PostingElementEx { row_id: 30, weight: 4.1, max_next_weight: f32::NEG_INFINITY },
            ],
        ];

        let merged = PostingList {
            elements: vec![
                PostingElementEx { row_id: 0, weight: 2.3, max_next_weight: 4.3 },
                PostingElementEx { row_id: 1, weight: 1.2, max_next_weight: 4.3 },
                PostingElementEx { row_id: 2, weight: 0.3, max_next_weight: 4.3 },
                PostingElementEx { row_id: 3, weight: 4.3, max_next_weight: 4.2 },
                PostingElementEx { row_id: 4, weight: 1.4, max_next_weight: 4.2 },
                PostingElementEx { row_id: 5, weight: 2.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 6, weight: 2.3, max_next_weight: 4.2 },
                PostingElementEx { row_id: 7, weight: 3.4, max_next_weight: 4.2 },
                PostingElementEx { row_id: 8, weight: 2.9, max_next_weight: 4.2 },
                PostingElementEx { row_id: 9, weight: 2.8, max_next_weight: 4.2 },
                PostingElementEx { row_id: 10, weight: 1.8, max_next_weight: 4.2 },
                PostingElementEx { row_id: 11, weight: 3.4, max_next_weight: 4.2 },
                PostingElementEx { row_id: 12, weight: 1.2, max_next_weight: 4.2 },
                PostingElementEx { row_id: 13, weight: 2.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 14, weight: 3.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 15, weight: 1.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 16, weight: 3.2, max_next_weight: 4.2 },
                PostingElementEx { row_id: 17, weight: 1.5, max_next_weight: 4.2 },
                PostingElementEx { row_id: 18, weight: 2.1, max_next_weight: 4.2 },
                PostingElementEx { row_id: 19, weight: 2.8, max_next_weight: 4.2 },
                PostingElementEx { row_id: 20, weight: 1.9, max_next_weight: 4.2 },
                PostingElementEx { row_id: 21, weight: 3.8, max_next_weight: 4.2 },
                PostingElementEx { row_id: 22, weight: 4.2, max_next_weight: 4.2 },
                PostingElementEx { row_id: 23, weight: 3.9, max_next_weight: 4.2 },
                PostingElementEx { row_id: 24, weight: 4.2, max_next_weight: 4.1 },
                PostingElementEx { row_id: 25, weight: 1.6, max_next_weight: 4.1 },
                PostingElementEx { row_id: 26, weight: 1.2, max_next_weight: 4.1 },
                PostingElementEx { row_id: 30, weight: 4.1, max_next_weight: f32::NEG_INFINITY },
            ],
        };
        return (lists, merged);
    }
    #[test]
    fn test_merge_posting_lists() {
        let postings = get_mocked_postings();
        let result = PostingListMerger::merge_posting_lists::<f32, f32>(&postings.0);
        assert_eq!(result.0, postings.1);
    }
}
