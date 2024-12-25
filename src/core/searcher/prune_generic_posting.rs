use crate::{core::ElementRead, RowId};

use super::search_posting_iterator::SearchPostingIterator;

pub fn prune_longest_posting(longest_posting: &mut SearchPostingIterator, min_score: f32, right_postings: &mut [SearchPostingIterator]) -> bool {
    // 获得最左侧 longest posting iter 的首个未遍历的元素
    if let Some(element) = longest_posting.generic_posting.peek() {
        // 在 right iterators 中找到最小的 row_id
        let min_row_id_in_right = get_min_row_id(right_postings);
        match min_row_id_in_right {
            Some(min_row_id_in_right) => {
                match min_row_id_in_right.cmp(&element.row_id()) {
                    std::cmp::Ordering::Less => {
                        // 当 right set 中 min row_id 比当前 longest posting 首个 row_id 小的时候, 不可以剪枝
                        return false;
                    }
                    std::cmp::Ordering::Equal => {
                        // 当 right set 中 min row_id 和当前 longest posting 首个 row_id 一样的时候, 也不能剪枝
                        return false;
                    }
                    std::cmp::Ordering::Greater => {
                        // 当 right set 中 min row_id 比当前 longest posting 首个 row_id 大的时候, 可以剪枝
                        // 最好的情形是 longest posting 中最小的 row_id 一直到 right set 中最小的 row_id 这个区间都能够被 cut 掉

                        // 获得 longest posting 能够贡献的最大分数
                        let max_weight_in_longest = element.weight().max(element.max_next_weight());
                        let max_score_contribution = max_weight_in_longest * longest_posting.dim_weight;

                        // 根据贡献的最大分数判断是否能够剪枝
                        if max_score_contribution <= min_score {
                            let cursor_before_pruning = longest_posting.generic_posting.cursor();
                            longest_posting.generic_posting.skip_to(min_row_id_in_right);
                            let cursor_after_pruning = longest_posting.generic_posting.cursor();
                            return cursor_before_pruning != cursor_after_pruning;
                        }
                    }
                }
            }
            None => {
                // min_row_id_in_right 为 None 时, 表示仅剩余左侧 1 个 posting
                // 直接判断左侧 posting 是否能够全部剪掉就行
                let max_weight_in_longest = element.weight().max(element.max_next_weight());
                let max_score_contribution = max_weight_in_longest * longest_posting.dim_weight;
                if max_score_contribution <= min_score {
                    longest_posting.generic_posting.skip_to_end();
                    return true;
                }
            }
        }
    }
    false
}

pub fn get_min_row_id(postings: &mut [SearchPostingIterator]) -> Option<RowId> {
    postings.iter_mut().filter_map(|iter| iter.generic_posting.peek().map(|e| e.row_id())).min()
}
