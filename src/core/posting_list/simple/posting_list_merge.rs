use super::PostingList;
use crate::{
    core::{posting_list::errors::PostingListError, ElementRead, ElementType, ElementWrite, GenericElement, QuantizedParam, QuantizedWeight, WeightType},
    RowId,
};
use log::error;

/// 在 merge 时需要传入 element type 类型，目前支持对 simple 和 extended 两种类型进行 merge
/// 其中 simple 类型支持量化；extended 类型不支持量化
/// simple 合并过程从前往后，extended 类型合并从后往前
/// 这是一个简单的合并策略，后面可以考虑使用策略模式去集成
pub struct PostingListMerger;

impl PostingListMerger {
    fn calculate_quantized_param<OW: QuantizedWeight>(min_weight: Option<OW>, max_weight: Option<OW>) -> QuantizedParam {
        match (min_weight, max_weight) {
            (Some(min), Some(max)) => OW::gen_quantized_param(min, max),
            _ => QuantizedParam::default(),
        }
    }

    fn build_posting_list<OW: QuantizedWeight, TW: QuantizedWeight>(merged: PostingList<OW>, quantized_param: Option<QuantizedParam>, use_quantized: bool) -> PostingList<TW> {
        if use_quantized {
            // Only `SimpleElement` can be quantized.
            PostingList { elements: merged.elements.into_iter().map(|e| e.quantize_with_param::<TW>(quantized_param.unwrap())).collect(), element_type: ElementType::SIMPLE }
        } else {
            unsafe { std::mem::transmute(merged) }
        }
    }

    // For `SimpleElement`, doesn't need calculate `max_next_weight`, only needs quantized.
    pub fn merge_simple_postings<OW: QuantizedWeight>(lists: &[Vec<GenericElement<OW>>]) -> Result<(PostingList<OW>, Option<OW>, Option<OW>), PostingListError> {
        let mut merged: PostingList<OW> = PostingList::<OW>::new(ElementType::SIMPLE);
        // quantized variables
        let mut min_weight: Option<OW> = None;
        let mut max_weight: Option<OW> = None;

        // `cursors` has recordes each posting's cursor for iteration.
        let mut cursors: Vec<usize> = vec![0; lists.len()];
        let mut merged_count: usize = 0;
        let total_elements: usize = lists.iter().map(|list| list.len()).sum();

        while merged_count < total_elements {
            // Find the posting_idx, which contains the minnest row_id.
            let min_row_id_posting_idx: Option<usize> =
                cursors.iter().enumerate().filter(|&(i, &index)| index < lists[i].len()).min_by_key(|&(i, &index)| lists[i][index].row_id()).map(|(i, _)| i);

            // Processing the PostingList (this PostingList contains min_row_id)
            if let Some(posting_idx) = min_row_id_posting_idx {
                let element = &lists[posting_idx][cursors[posting_idx]];
                // Boundary
                if element.element_type() != ElementType::SIMPLE {
                    let error_msg = "Note expected!, the PostingElement type can only be `ElementType::SIMPLE` during merging process";
                    error!("{}", error_msg);
                    return Err(PostingListError::MergeError(error_msg.to_string()));
                }
                merged.elements.push(element.clone());

                // update min max weight
                match min_weight {
                    Some(min) => min_weight = Some(min.min(element.weight())),
                    None => min_weight = Some(element.weight()),
                }
                match max_weight {
                    Some(max) => max_weight = Some(max.max(element.weight())),
                    None => max_weight = Some(element.weight()),
                }
                // increase cursor
                cursors[posting_idx] += 1;
                merged_count += 1;
            } else {
                // Exit the loop if there's no valid posting left to process
                break;
            }
        }

        Ok((merged, min_weight, max_weight))
    }

    pub fn merge_extended_postings<OW: QuantizedWeight>(lists: &[Vec<GenericElement<OW>>]) -> Result<PostingList<OW>, PostingListError> {
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
                let mut element: GenericElement<OW> = lists[posting_idx][cursors_rev[posting_idx] - 1].clone();
                // Boundary
                if element.element_type() != ElementType::EXTENDED {
                    let error_msg = "Note expected!, the PostingElement type can only be `ElementType::Extended` during merging process";
                    error!("{}", error_msg);
                    return Err(PostingListError::MergeError(error_msg.to_string()));
                }
                element.update_max_next_weight(max_next_weight);
                merged.elements.push(element.clone());

                max_next_weight = max_next_weight.max(element.weight());

                cursors_rev[posting_idx] -= 1;
            }
        }
        // Reverse elements, make their `row_id` in order.
        merged.elements.reverse();

        // `ExtendedElement` is not allowed to be quantized.
        Ok(merged)
    }

    /// This merge operation is performed across multiple `InvertedIndex` segments for the same dimension,
    /// merging all the posting lists corresponding to the same dimension.
    /// Therefore, the merge operation will never encounter the same row_id.
    pub fn merge_posting_lists<OW: QuantizedWeight, TW: QuantizedWeight>(
        lists: &Vec<Vec<GenericElement<OW>>>,
        element_type: ElementType,
    ) -> Result<(PostingList<TW>, Option<QuantizedParam>), PostingListError> {
        let use_quantized = OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;

        match element_type {
            ElementType::SIMPLE => {
                // Boundary.
                if !use_quantized && OW::weight_type() != TW::weight_type() {
                    let error_msg = "Merging for `SimpleElement` without being quantized, weight_type should keep same.";
                    error!("{}", error_msg);
                    return Err(PostingListError::MergeError(error_msg.to_string()));
                }

                let (merged, min_weight, max_weight) = Self::merge_simple_postings(lists)?;

                let mut quantized_param = None;
                if use_quantized {
                    quantized_param = Some(Self::calculate_quantized_param(min_weight, max_weight));
                }
                let posting_list: PostingList<TW> = Self::build_posting_list(merged, quantized_param.clone(), use_quantized);

                return Ok((posting_list, quantized_param));
            }
            ElementType::EXTENDED => {
                if use_quantized {
                    let error_msg = "`ExtendedElement` can't be quantized! Can't continue merging process.";
                    error!("{}", error_msg);
                    return Err(PostingListError::MergeError(error_msg.to_string()));
                }
                if !use_quantized && OW::weight_type() != TW::weight_type() {
                    let error_msg = "For `ExtendedElement`, it's OW and TW weight_type should keep same.";
                    error!("{}", error_msg);
                    return Err(PostingListError::MergeError(error_msg.to_string()));
                }

                let merged = Self::merge_extended_postings(lists)?;

                let tw_posting_list: PostingList<TW> = unsafe { std::mem::transmute(merged) };
                return Ok((tw_posting_list, None));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::f32;

    use rand::Rng;

    use crate::{
        core::{ElementType, GenericElement, PostingList, PostingListBuilder, QuantizedParam, QuantizedWeight},
        RowId,
    };

    use super::PostingListMerger;

    fn mock_build_posting<OW: QuantizedWeight, TW: QuantizedWeight>(element_type: ElementType, elements: Vec<(RowId, f32)>) -> (PostingList<TW>, Option<QuantizedParam>) {
        let mut builder = PostingListBuilder::<OW, TW>::new(element_type, false).expect("");
        for (row_id, weight) in elements {
            builder.add(row_id, weight);
        }
        builder.build().unwrap()
    }

    fn generate_random_float() -> f32 {
        let mut rng = rand::thread_rng();
        rng.gen_range(0.010101..10.1111)
    }

    fn enlarge_elements(vector: Vec<(u32, f32)>, base: u32) -> Vec<(u32, f32)> {
        let mut enlarged = vector.clone();
        for (row_id, _) in vector {
            enlarged.push((row_id + base, generate_random_float()));
        }
        enlarged
    }

    fn mock_posting_candidates<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        enlarge: i32,
    ) -> (Vec<Vec<GenericElement<TW>>>, (PostingList<TW>, Option<QuantizedParam>)) {
        let mut vec_0: Vec<(u32, f32)> = vec![];
        let mut vec_1: Vec<(u32, f32)> = vec![(3, 4.3)];
        let mut vec_2: Vec<(u32, f32)> = vec![(0, 2.3), (4, 1.4), (5, 2.1), (9, 2.8), (12, 1.2)];
        let mut vec_3: Vec<(u32, f32)> = vec![(1, 1.2), (10, 1.8)];
        let mut vec_4: Vec<(u32, f32)> = vec![];
        let mut vec_5: Vec<(u32, f32)> = vec![(2, 0.3), (11, 3.4), (13, 2.1), (15, 1.1), (17, 1.5), (21, 3.8), (24, 4.2)];
        let mut vec_6: Vec<(u32, f32)> = vec![(8, 2.9), (14, 3.1)];
        let mut vec_7: Vec<(u32, f32)> = vec![(6, 2.3), (7, 3.4), (16, 3.2), (19, 2.8), (20, 1.9)];
        let mut vec_8: Vec<(u32, f32)> = vec![(18, 2.1), (22, 4.2), (23, 3.9), (25, 1.6), (30, 4.1)];
        let mut vec_9: Vec<(u32, f32)> = vec![(26, 1.1)];

        let mut mock_vectors =
            vec![vec_0.clone(), vec_1.clone(), vec_2.clone(), vec_3.clone(), vec_4.clone(), vec_5.clone(), vec_6.clone(), vec_7.clone(), vec_8.clone(), vec_9.clone()];

        for _ in 0..enlarge {
            let max_id = mock_vectors.iter().flat_map(|e| e.iter()).map(|(id, _)| *id).max().unwrap_or(0) + 1;
            vec_0 = enlarge_elements(vec_0.clone(), max_id as u32);
            vec_1 = enlarge_elements(vec_1.clone(), max_id as u32);
            vec_2 = enlarge_elements(vec_2.clone(), max_id as u32);
            vec_3 = enlarge_elements(vec_3.clone(), max_id as u32);
            vec_4 = enlarge_elements(vec_4.clone(), max_id as u32);
            vec_5 = enlarge_elements(vec_5.clone(), max_id as u32);
            vec_6 = enlarge_elements(vec_6.clone(), max_id as u32);
            vec_7 = enlarge_elements(vec_7.clone(), max_id as u32);
            vec_8 = enlarge_elements(vec_8.clone(), max_id as u32);
            vec_9 = enlarge_elements(vec_9.clone(), max_id as u32);
            mock_vectors =
                vec![vec_0.clone(), vec_1.clone(), vec_2.clone(), vec_3.clone(), vec_4.clone(), vec_5.clone(), vec_6.clone(), vec_7.clone(), vec_8.clone(), vec_9.clone()];
        }

        let mut combined_vec: Vec<(u32, f32)> = vec![];
        for v in mock_vectors.clone() {
            combined_vec.extend(v);
        }
        combined_vec.sort_by(|a, b| a.0.cmp(&b.0));

        let mut postings: Vec<Vec<GenericElement<TW>>> = Vec::new();

        for v in mock_vectors.clone() {
            let (posting, _) = mock_build_posting::<OW, TW>(element_type, v);
            let elements: Vec<GenericElement<TW>> = posting.elements;
            postings.push(elements);
        }

        let merged: (PostingList<TW>, Option<QuantizedParam>) = mock_build_posting::<OW, TW>(element_type, combined_vec);

        return (postings, merged);
    }

    #[test]
    fn test_merge_simple_posting_lists() {
        // merge for f32-f32 postings. (not-quantized)
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<f32, f32>(ElementType::SIMPLE, 12);
            let (merged_result, param) = PostingListMerger::merge_posting_lists::<f32, f32>(&candidates, ElementType::SIMPLE).unwrap();
            assert!(param.is_none());
            assert_eq!(merged_posting, merged_result);
        }
        // merge for f16-f16 postings. (not-quantized)
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<half::f16, half::f16>(ElementType::SIMPLE, 12);
            let (merged_result, param) = PostingListMerger::merge_posting_lists::<half::f16, half::f16>(&candidates, ElementType::SIMPLE).unwrap();
            assert!(param.is_none());
            assert_eq!(merged_posting, merged_result);
        }
        // merge for u8-u8 postings. (not-quantized)
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<u8, u8>(ElementType::SIMPLE, 12);
            let (merged_result, param) = PostingListMerger::merge_posting_lists::<u8, u8>(&candidates, ElementType::SIMPLE).unwrap();
            assert!(param.is_none());
            assert_eq!(merged_posting, merged_result);
        }
        // merge for f32-u8 postings (quantized).
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<f32, f32>(ElementType::SIMPLE, 12);
            let (merged_result, _) = PostingListMerger::merge_posting_lists::<f32, u8>(&candidates, ElementType::SIMPLE).unwrap();
            let mut builder = PostingListBuilder::<f32, u8>::new(ElementType::SIMPLE, false).unwrap();
            builder.update_inner_posting(merged_posting);
            let (expected_merged_posting, param) = builder.build().unwrap();
            assert!(param.is_some());
            assert_eq!(merged_result, expected_merged_posting);
        }
        // merge for f16-u8 postings (quantized).
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<half::f16, half::f16>(ElementType::SIMPLE, 12);
            let (merged_result, _) = PostingListMerger::merge_posting_lists::<half::f16, u8>(&candidates, ElementType::SIMPLE).unwrap();
            let mut builder = PostingListBuilder::<half::f16, u8>::new(ElementType::SIMPLE, false).unwrap();
            builder.update_inner_posting(merged_posting);
            let (expected_merged_posting, param) = builder.build().unwrap();
            assert!(param.is_some());
            assert_eq!(merged_result, expected_merged_posting);
        }
    }

    #[test]
    fn test_merge_extended_posting_lists() {
        // merge for f32-f32 postings. (not-quantized)
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<f32, f32>(ElementType::EXTENDED, 12);
            let (merged_result, param) = PostingListMerger::merge_posting_lists::<f32, f32>(&candidates, ElementType::EXTENDED).unwrap();
            assert!(param.is_none());
            assert_eq!(merged_posting, merged_result);
        }
        // merge for f16-f16 postings. (not-quantized)
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<half::f16, half::f16>(ElementType::EXTENDED, 12);
            let (merged_result, param) = PostingListMerger::merge_posting_lists::<half::f16, half::f16>(&candidates, ElementType::EXTENDED).unwrap();
            assert!(param.is_none());
            assert_eq!(merged_posting, merged_result);
        }
        // merge for u8-u8 postings. (not-quantized)
        {
            let (candidates, (merged_posting, _)) = mock_posting_candidates::<u8, u8>(ElementType::EXTENDED, 12);
            let (merged_result, param) = PostingListMerger::merge_posting_lists::<u8, u8>(&candidates, ElementType::EXTENDED).unwrap();
            assert!(param.is_none());
            assert_eq!(merged_posting, merged_result);
        }
        // invalid merge for f32-u8 postings (quantized).
        {
            let (candidates, (_, _)) = mock_posting_candidates::<f32, f32>(ElementType::EXTENDED, 12);
            let res = PostingListMerger::merge_posting_lists::<f32, u8>(&candidates, ElementType::EXTENDED);
            assert!(res.is_err());
        }
        // invalid merge for f16-u8 postings (quantized).
        {
            let (candidates, (_, _)) = mock_posting_candidates::<half::f16, half::f16>(ElementType::EXTENDED, 12);
            let res = PostingListMerger::merge_posting_lists::<half::f16, u8>(&candidates, ElementType::EXTENDED);
            assert!(res.is_err());
        }
    }
}
