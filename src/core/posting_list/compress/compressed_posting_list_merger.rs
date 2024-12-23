use crate::core::{
    ElementRead, ElementType, PostingListIter, QuantizedParam, QuantizedWeight,
};

use super::{CompressedPostingBuilder, CompressedPostingList, CompressedPostingListIterator};

pub struct CompressedPostingListMerger;

impl CompressedPostingListMerger {
    /// input a group of postings, they are in the same dim-id.
    pub fn merge_posting_lists<OW: QuantizedWeight, TW: QuantizedWeight>(
        compressed_posting_iterators: &mut Vec<CompressedPostingListIterator<'_, OW, TW>>,
        element_type: ElementType,
    ) -> (CompressedPostingList<TW>, Option<QuantizedParam>) {
        // TODO: Refine compressed posting merging design, currently we should finally sort the whole posting, it's too slow.
        let mut merged_compressed_posting_builder: CompressedPostingBuilder<OW, TW> =
            CompressedPostingBuilder::<OW, TW>::new(element_type, true, false);

        // TODO: 优化策略，对齐所有的 compressed posting iterators，从头选择 row_id 最小的那个开始逐个合并
        // TODO： 另外也需要照顾到 是否含有 max_next_weight 的场景
        for iterator in compressed_posting_iterators {
            while iterator.remains() != 0 {
                let element = iterator.next();
                if element.is_some() {
                    let element = element.unwrap();
                    merged_compressed_posting_builder
                        .add(element.row_id(), OW::to_f32(element.weight()));
                }
            }
        }

        let merged_compressed_posting_list: CompressedPostingList<TW> =
            merged_compressed_posting_builder.build();
        let quantized_param: Option<QuantizedParam> =
            merged_compressed_posting_list.quantization_params;

        return (merged_compressed_posting_list, quantized_param);
    }
}

#[cfg(test)]
mod tests {
    use core::f32;

    use crate::core::{ElementType, ExtendedElement, GenericElement, PostingList};

    /// mock 7 postings for the same dim-id.
    fn get_mocked_postings() -> (Vec<Vec<GenericElement<f32>>>, PostingList<f32>) {
        let lists: Vec<Vec<GenericElement<f32>>> = vec![
            vec![], // 0
            vec![
                // 1
                ExtendedElement { row_id: 0, weight: 2.3, max_next_weight: 2.8 }.into(),
                ExtendedElement { row_id: 4, weight: 1.4, max_next_weight: 2.8 }.into(),
                ExtendedElement { row_id: 5, weight: 2.1, max_next_weight: 2.8 }.into(),
                ExtendedElement { row_id: 9, weight: 2.8, max_next_weight: 1.2 }.into(),
                ExtendedElement { row_id: 12, weight: 1.2, max_next_weight: f32::NEG_INFINITY }
                    .into(),
            ],
            vec![], // 2
            vec![
                // 3
                ExtendedElement { row_id: 1, weight: 1.2, max_next_weight: 4.3 }.into(),
                ExtendedElement { row_id: 3, weight: 4.3, max_next_weight: 3.1 }.into(),
                ExtendedElement { row_id: 8, weight: 2.9, max_next_weight: 3.1 }.into(),
                ExtendedElement { row_id: 10, weight: 1.8, max_next_weight: 3.1 }.into(),
                ExtendedElement { row_id: 14, weight: 3.1, max_next_weight: f32::NEG_INFINITY }
                    .into(),
            ],
            vec![
                // 4
                ExtendedElement { row_id: 2, weight: 0.3, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 11, weight: 3.4, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 13, weight: 2.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 15, weight: 1.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 17, weight: 1.5, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 21, weight: 3.8, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 24, weight: 4.2, max_next_weight: f32::NEG_INFINITY }
                    .into(),
            ],
            vec![
                // 5
                ExtendedElement { row_id: 6, weight: 2.3, max_next_weight: 3.4 }.into(),
                ExtendedElement { row_id: 7, weight: 3.4, max_next_weight: 3.2 }.into(),
                ExtendedElement { row_id: 16, weight: 3.2, max_next_weight: 2.8 }.into(),
                ExtendedElement { row_id: 19, weight: 2.8, max_next_weight: 1.9 }.into(),
                ExtendedElement { row_id: 20, weight: 1.9, max_next_weight: f32::NEG_INFINITY }
                    .into(),
            ],
            vec![
                // 6
                ExtendedElement { row_id: 18, weight: 2.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 22, weight: 4.2, max_next_weight: 4.1 }.into(),
                ExtendedElement { row_id: 23, weight: 3.9, max_next_weight: 4.1 }.into(),
                ExtendedElement { row_id: 25, weight: 1.6, max_next_weight: 4.1 }.into(),
                ExtendedElement { row_id: 26, weight: 1.2, max_next_weight: 4.1 }.into(),
                ExtendedElement { row_id: 30, weight: 4.1, max_next_weight: f32::NEG_INFINITY }
                    .into(),
            ],
        ];

        let merged = PostingList {
            elements: vec![
                ExtendedElement { row_id: 0, weight: 2.3, max_next_weight: 4.3 }.into(),
                ExtendedElement { row_id: 1, weight: 1.2, max_next_weight: 4.3 }.into(),
                ExtendedElement { row_id: 2, weight: 0.3, max_next_weight: 4.3 }.into(),
                ExtendedElement { row_id: 3, weight: 4.3, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 4, weight: 1.4, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 5, weight: 2.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 6, weight: 2.3, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 7, weight: 3.4, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 8, weight: 2.9, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 9, weight: 2.8, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 10, weight: 1.8, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 11, weight: 3.4, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 12, weight: 1.2, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 13, weight: 2.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 14, weight: 3.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 15, weight: 1.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 16, weight: 3.2, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 17, weight: 1.5, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 18, weight: 2.1, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 19, weight: 2.8, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 20, weight: 1.9, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 21, weight: 3.8, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 22, weight: 4.2, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 23, weight: 3.9, max_next_weight: 4.2 }.into(),
                ExtendedElement { row_id: 24, weight: 4.2, max_next_weight: 4.1 }.into(),
                ExtendedElement { row_id: 25, weight: 1.6, max_next_weight: 4.1 }.into(),
                ExtendedElement { row_id: 26, weight: 1.2, max_next_weight: 4.1 }.into(),
                ExtendedElement { row_id: 30, weight: 4.1, max_next_weight: f32::NEG_INFINITY }
                    .into(),
            ],
            element_type: ElementType::EXTENDED,
        };
        return (lists, merged);
    }
    #[test]
    fn test_merge_posting_lists() {
        // let postings = get_mocked_postings();
        // let result = PostingListMerger::merge_posting_lists::<f32, f32>(&postings.0);
        // assert_eq!(result.0, postings.1);
    }
}
