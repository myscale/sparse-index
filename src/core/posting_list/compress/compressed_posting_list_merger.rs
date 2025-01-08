use crate::core::{ElementType, GenericElement, PostingListError, PostingListIter, PostingListMerger, QuantizedParam, QuantizedWeight};

use super::{CompressedPostingBuilder, CompressedPostingList, CompressedPostingListIterator};

pub struct CompressedPostingListMerger;

impl CompressedPostingListMerger {
    /// input a group of postings, they are in the same dim-id.
    pub fn merge_posting_lists<OW: QuantizedWeight, TW: QuantizedWeight>(
        compressed_posting_iterators: &mut Vec<CompressedPostingListIterator<'_, OW, TW>>,
        element_type: ElementType,
    ) -> Result<(CompressedPostingList<TW>, Option<QuantizedParam>), PostingListError> {
        let mut postings: Vec<Vec<GenericElement<OW>>> = Vec::with_capacity(compressed_posting_iterators.len());
        for iterator in compressed_posting_iterators {
            let mut elements = Vec::new();
            while iterator.remains() != 0 {
                let element = iterator.next();
                if element.is_some() {
                    let element = element.unwrap();
                    elements.push(element);
                } else {
                    break;
                }
            }
            postings.push(elements);
        }

        // Reuse the code of `PostingListMerger`
        match element_type {
            ElementType::SIMPLE => {
                let mut builder: CompressedPostingBuilder<OW, TW> = CompressedPostingBuilder::<OW, TW>::new(element_type, false, false)?;
                let (merged, _, _) = PostingListMerger::merge_simple_postings(&postings)?;
                builder.posting = merged;
                let compressed_merged = builder.build()?;
                let param = compressed_merged.quantization_params.clone();
                return Ok((compressed_merged, param));
            }
            ElementType::EXTENDED => {
                // For `ExtendedElement` type, we don't need execute `finally_propagate`
                let mut builder: CompressedPostingBuilder<OW, TW> = CompressedPostingBuilder::<OW, TW>::new(element_type, false, false)?;
                let merged = PostingListMerger::merge_extended_postings(&postings)?;
                builder.posting = merged;
                let compressed_merged = builder.build()?;
                return Ok((compressed_merged, None));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test::{enlarge_elements, get_compressed_posting_iterators, mock_build_compressed_posting};
    use crate::core::{CompressedPostingList, CompressedPostingListMerger, ElementType, QuantizedParam, QuantizedWeight, WeightType};
    use core::f32;

    fn mock_compressed_posting_candidates<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        enlarge: i32,
    ) -> (Vec<CompressedPostingList<TW>>, (CompressedPostingList<TW>, Option<QuantizedParam>)) {
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

        let mut postings = Vec::new();

        for v in mock_vectors.clone() {
            let (posting, _) = mock_build_compressed_posting::<OW, TW>(element_type, v);
            postings.push(posting);
        }

        let merged: (CompressedPostingList<TW>, Option<QuantizedParam>) = mock_build_compressed_posting::<OW, TW>(element_type, combined_vec);

        return (postings, merged);
    }

    fn inner_test_merge_compressed_posting_lists<OW: QuantizedWeight, TW: QuantizedWeight>(element_type: ElementType, enlarge: i32) {
        let use_quantized = OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        let (candidates, (expected_cmp_posting, expected_quantized_param)) = mock_compressed_posting_candidates::<OW, TW>(element_type, enlarge);
        let mut candidate_iterators = get_compressed_posting_iterators(&candidates);
        let (result_cmp_posting, result_quantized_param) = CompressedPostingListMerger::merge_posting_lists::<OW, TW>(&mut candidate_iterators, element_type).unwrap();

        if use_quantized {
            assert!(result_quantized_param.is_some());
            assert_eq!(expected_quantized_param, result_quantized_param);
            assert!(result_cmp_posting.approximately_eq(&expected_cmp_posting));
        } else {
            assert!(result_quantized_param.is_none());
            assert_eq!(expected_cmp_posting, result_cmp_posting);
        }
    }

    #[test]
    fn test_merge_compressed_posting_lists() {
        // TODO 缺乏边界测试，需要补充

        // Simple: merge for f32-f32 postings. (not-quantized)
        inner_test_merge_compressed_posting_lists::<f32, f32>(ElementType::SIMPLE, 12);
        // Simple: merge for f16-f16 postings. (not-quantized)
        inner_test_merge_compressed_posting_lists::<half::f16, half::f16>(ElementType::SIMPLE, 12);
        // Simple: merge for u8-u8 postings. (not-quantized)
        inner_test_merge_compressed_posting_lists::<u8, u8>(ElementType::SIMPLE, 12);
        // Simple: merge for f32-u8 postings (quantized).
        inner_test_merge_compressed_posting_lists::<f32, u8>(ElementType::SIMPLE, 12);
        // Simple: merge for f16-u8 postings (quantized).
        inner_test_merge_compressed_posting_lists::<half::f16, u8>(ElementType::SIMPLE, 12);

        // Extended: merge for f32-f32 postings. (not-quantized)
        inner_test_merge_compressed_posting_lists::<f32, f32>(ElementType::EXTENDED, 12);
        // Extended: merge for f16-f16 postings. (not-quantized)
        inner_test_merge_compressed_posting_lists::<half::f16, half::f16>(ElementType::EXTENDED, 12);
        // Extended: merge for u8-u8 postings. (not-quantized)
        inner_test_merge_compressed_posting_lists::<u8, u8>(ElementType::EXTENDED, 12);
    }
}
