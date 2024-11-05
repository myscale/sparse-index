use std::mem::size_of;

use log::{error, warn};

use super::super::traits::PostingElementEx;
use super::PostingList;
use crate::{
    core::{common::types::DimWeight, DEFAULT_MAX_NEXT_WEIGHT},
    RowId,
};

#[derive(Default)]
pub struct PostingListBuilder {
    posting: PostingList,
    propagate_while_upserting: bool,
    finally_sort: bool,
    finally_propagate: bool,
}

// Builder pattern
impl PostingListBuilder {
    pub fn new() -> Self {
        Self {
            posting: PostingList::new(),
            finally_sort: false,
            propagate_while_upserting: false,
            finally_propagate: true,
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

impl PostingListBuilder {
    /// ## brief
    /// add a new Element to the posting list.
    /// ## return
    /// bool: `ture` means the `insert` operation, `false` means `update`.
    pub fn add(&mut self, row_id: RowId, weight: DimWeight) -> bool {
        if self.propagate_while_upserting {
            self.posting
                .upsert_with_propagate(PostingElementEx::new(row_id, weight))
        } else {
            self.posting.upsert(PostingElementEx::new(row_id, weight)).1
        }
    }

    /// ## brief
    /// 返回 elements 占据的内存字节大小
    pub fn memory_usage(&self) -> usize {
        self.posting.len() * size_of::<PostingElementEx>()
    }

    /// ## brief
    /// 消费 self 并返回新的 PostingList 结构
    pub fn build(mut self) -> PostingList {
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
                error!("Duplicated row_id, or Posting is not sorted by row_id correctly, left: {:?}, right: {:?}.", res[0], res[1]);
                panic!("Duplicated row_id, or Posting is not sorted by row_id correctly, left: {:?}, right: {:?}.", res[0], res[1]);
            }
        }
        // 从后往前修改每个 element 的 max_next_weight
        if self.finally_propagate {
            let mut max_next_weight = DEFAULT_MAX_NEXT_WEIGHT;
            for element in self.posting.elements.iter_mut().rev() {
                element.max_next_weight = max_next_weight;
                max_next_weight = max_next_weight.max(element.weight);
            }
        } else {
            warn!("Skip propagating the Posting finally, please make sure it has already been propagated.")
        }

        self.posting
    }
}

#[cfg(test)]
mod tests {
    use crate::core::posting_list::traits::PostingElementEx;

    // TODO 这个测试应该放到 traits 里面，验证 Element 的排序
    #[test]
    fn test_sort_unstable_by() {
        let mut elements = vec![
            PostingElementEx::new(2, 0.9),
            PostingElementEx::new(1, 1.2),
            PostingElementEx::new(3, 0.2),
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
