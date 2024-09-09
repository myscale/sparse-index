use super::super::traits::PostingElementEx;
use super::PostingList;
use crate::core::common::types::{DimWeight, ElementOffsetType};

pub struct PostingListBuilder {
    elements: Vec<PostingElementEx>,
}

impl Default for PostingListBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PostingListBuilder {
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    // add a new Element to the posting list.
    pub fn add(&mut self, row_id: ElementOffsetType, weight: DimWeight) {
        self.elements.push(PostingElementEx::new(row_id, weight));
    }

    // 消费 self 并返回新的 PostingList 结构
    pub fn build(mut self) -> PostingList {
        // 根据 row_id 进行排序
        self.elements.sort_unstable_by_key(|e| e.row_id);
        // 检查在一个 PostingList 中是否存在重复的 row_id
        #[cfg(debug_assertions)]
        {
            if let Some(res) = self
                .elements
                .windows(2)
                .find(|e| e[0].row_id == e[1].row_id)
            {
                panic!("Duplicated row_id {} in posting list.", res[0].row_id);
            }
        }
        // 从后往前修改每个 element 的 max_next_weight
        let mut max_next_weight = f32::NEG_INFINITY;
        for element in self.elements.iter_mut().rev() {
            element.max_next_weight = max_next_weight;
            max_next_weight = max_next_weight.max(element.weight);
        }
        PostingList {
            elements: self.elements,
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
