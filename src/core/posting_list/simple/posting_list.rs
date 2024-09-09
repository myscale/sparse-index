use super::super::traits::{PostingElementEx, DEFAULT_MAX_NEXT_WEIGHT};
use super::{PostingListBuilder, PostingListIterator};
use crate::core::common::types::{DimWeight, ElementOffsetType};
use ordered_float::OrderedFloat;
use std::cmp::max;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct PostingList {
    pub elements: Vec<PostingElementEx>,
}

impl PostingList {
    pub fn from(records: Vec<(ElementOffsetType, DimWeight)>) -> PostingList {
        let mut posting_list = PostingListBuilder::new();
        for (row_id, weight) in records {
            posting_list.add(row_id, weight);
        }
        posting_list.build()
    }

    pub fn new_one(row_id: ElementOffsetType, dim_weight: DimWeight) -> PostingList {
        PostingList {
            elements: vec![PostingElementEx::new(row_id, dim_weight)],
        }
    }

    pub fn delete(&mut self, row_id: ElementOffsetType) {
        let index = self.elements.binary_search_by_key(&row_id, |e| e.row_id);
        if let Ok(found_index) = index {
            self.elements.remove(found_index);
            if let Some(last) = self.elements.last_mut() {
                last.max_next_weight = DEFAULT_MAX_NEXT_WEIGHT;
            }
            // 从发生删除的位置往前传播, 更新沿途所有 Elements 的 max_next_weight
            if found_index < self.elements.len() {
                self.propagate_max_next_weight_to_the_left(found_index);
            } else if !self.elements.is_empty() {
                self.propagate_max_next_weight_to_the_left(self.elements.len() - 1);
            }
        }
    }

    pub fn upsert(&mut self, posting_element: PostingElementEx) {
        let index = self
            .elements
            .binary_search_by_key(&posting_element.row_id, |e| e.row_id);

        let modified_index = match index {
            Ok(found_index) => {
                // 更新已经存在的 element
                let element = &mut self.elements[found_index];
                if element.weight == posting_element.weight {
                    None
                } else {
                    element.weight = posting_element.weight;
                    Some(found_index)
                }
            }
            Err(insert_index) => {
                // 插入不存在的 new element
                self.elements.insert(insert_index, posting_element);
                if insert_index == self.elements.len() - 1 {
                    Some(insert_index)
                } else {
                    Some(insert_index + 1)
                }
            }
        };

        // 前向传播, 更新 max_next_weight, 仅针对 Insert 与 Update成功的场景
        if let Some(modified_index) = modified_index {
            self.propagate_max_next_weight_to_the_left(modified_index)
        }
    }

    fn propagate_max_next_weight_to_the_left(&mut self, up_to_index: usize) {
        // used element at `up_to_index` as the starting point
        let starting_element = &self.elements[up_to_index];
        let mut max_next_weight = max(
            OrderedFloat(starting_element.max_next_weight),
            OrderedFloat(starting_element.weight), // 使用 OrderedFloat 包装，防止浮点数的比较出现 Nan 行为
        )
        .0;

        // 前向传播，修改所有 Element 的 max_next_weight 字段
        for element in self.elements[..up_to_index].iter_mut().rev() {
            // update max_next_weight for element
            element.max_next_weight = max_next_weight;
            max_next_weight = max_next_weight.max(element.weight);
        }
    }

    /// 构建 `PostingListIterator`
    /// `PostingListIterator` 内部 elements 生命周期与 PostingList 一致
    pub fn iter(&self) -> PostingListIterator {
        PostingListIterator::new(&self.elements)
    }
}
