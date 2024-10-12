use std::cmp::{max, min};

use log::{debug, error};

use crate::{ffi::ScoredPointOffset, RowId};

use super::{DimId, DimWeight, InvertedIndex, InvertedIndexMmap, PostingListIter, PostingListIterator, ScoreType, SparseVector, TopK};
const ADVANCE_BATCH_SIZE: usize = 10_000;


struct IndexedPostingListIterator<'a> {
    posting_list_iterator: PostingListIterator<'a>,
    query_index: DimId,
    query_weight: DimWeight,
}


pub struct SearchEnv<'a> {
    postings_iterators: Vec<IndexedPostingListIterator<'a>>,
    // query 涉及到的最小行号
    min_row_id: Option<RowId>,
    // query 涉及到的最大行号
    max_row_id: Option<RowId>,
    // 是否能够使用剪枝
    use_pruning: bool,
    // 存储候选结果
    top_k: TopK,
}


#[derive(Clone)]
pub struct IndexSearcher {
    inverted_index: InvertedIndexMmap,
}


fn get_min_row_id(posting_iterators: &mut [IndexedPostingListIterator<'_>]) -> Option<RowId> {
    let mut min_row_id = RowId::MAX;
    for iterator in posting_iterators {
        if let Some(element) = iterator.posting_list_iterator.peek() {
            min_row_id = min(element.row_id, min_row_id);
        }
    }
    if min_row_id == RowId::MAX {
        return None;
    }else {
        return Some(min_row_id);
    }
}


impl IndexSearcher {
    pub fn new(inverted_index: InvertedIndexMmap) -> Self {
        return Self {
            inverted_index,
        };
    }

    pub fn get_inverted_index(&self) -> &InvertedIndexMmap {
        return &self.inverted_index;
    }

    fn pre_search(&self, query: SparseVector, limits: u32) -> SearchEnv {
        let mut postings_iterators: Vec<IndexedPostingListIterator> = Vec::new();
        let mut max_row_id = 0;
        let mut min_row_id = u32::MAX;

        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(it) = self.inverted_index.iter(id) {
                if let (Some(first), Some(last_id)) = (it.peek(), it.last_id()) {
                    min_row_id = min(min_row_id, first.row_id);
                    max_row_id = max(max_row_id, last_id);

                    let query_index = *id;
                    let query_weight = query.values[query_weight_offset];

                    // 将 query（sparse vector）涉及到的 PostingListIterator 存储起来
                    postings_iterators.push(IndexedPostingListIterator {
                        posting_list_iterator: it,
                        query_index,
                        query_weight,
                    })
                }
            }
        }
        // 未压缩 mmap 能够保证 `max_next_weigh` 是 true
        let use_pruning = query.values.iter().all(|v| *v >= 0.0);

        let top_k = TopK::new(limits as usize);

        SearchEnv {
            postings_iterators,
            min_row_id: Some(min_row_id),
            max_row_id: Some(max_row_id),
            use_pruning,
            top_k,
        }
    }

    // TODO 应该将 index 中所有的 row_id 给存储起来 
    pub fn plain_search(&self, query: SparseVector, limits: u32) -> TopK {
        let mut search_env = self.pre_search(query.clone(), limits);

        // row_id 范围应该是整个 index 在索引过程中记录的最小和最大 row_id 区间
        for id in self.inverted_index.min_row_id()..self.inverted_index.max_row_id() {
            let mut indices = Vec::with_capacity(query.indices.len());
            let mut values = Vec::with_capacity(query.values.len());

            // 仅遍历 query 涉及到的 rows
            // collect indices and values for the current record id from the query's posting lists *only*
            for posting_iterator in search_env.postings_iterators.iter_mut() {
                // rely on underlying binary search as the posting lists are sorted by record id
                match posting_iterator.posting_list_iterator.skip_to(id) {
                    None => {} // no match for posting list
                    Some(element) => {
                        // match for posting list
                        indices.push(posting_iterator.query_index);
                        values.push(element.weight);
                    }
                }
            }
            // reconstruct sparse vector and score against query
            // TODO 是否有可能直接将每一行 sparse vector 的完整数据存储到一个文件中 .store ?
            let sparse_vector: SparseVector = SparseVector { indices, values };
            search_env.top_k.push(ScoredPointOffset {
                score: sparse_vector.score(&query).unwrap_or(0.0),
                row_id: id,
            });
        }
        
        search_env.top_k
    }

    /// 遍历 query 涉及到的所有 postings，在每个 postings 中遍历一个 batch 范围内的数据
    fn advance_batch(&self, batch_start_id: RowId, batch_end_id: RowId, search_env: &mut SearchEnv) {
        let batch_size = batch_end_id - batch_start_id + 1;
        let mut batch_scores: Vec<ScoreType> = vec![0.0; batch_size as usize];

        // debug!("[advance_batch] batch_scores len (batch_size):{}, batch_start_id:{}, batch_end_id:{}", batch_size, batch_start_id, batch_end_id);
        for posting in search_env.postings_iterators.iter_mut() {
            posting.posting_list_iterator.for_each_till_row_id(
                batch_end_id,
                |element| {

                    if element.row_id < batch_start_id || element.row_id > batch_end_id {
                        error!("row id range error when iter posting element till row id.");
                        return;
                    }

                    let score = element.weight * posting.query_weight;
                    let local_id = (element.row_id - batch_start_id) as usize;
                    // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                    batch_scores[local_id] += score;
                } 
            );
        }

        for (local_id, &score) in batch_scores.iter().enumerate() {
            if score > 0.0 && score > search_env.top_k.threshold() {
                // TOOD 判断 element.row_id 是否合法（未被过滤）

                let real_id = local_id + batch_start_id as usize;
                search_env.top_k.push(
                    ScoredPointOffset {
                        row_id: real_id as RowId,
                        score 
                    }
                );
            }
        }
    }


    // search env 仅存在 1 个 posting 的时候, 计算分数
    fn process_last_posting_list(&self, search_env: &mut SearchEnv) {
        debug_assert_eq!(search_env.postings_iterators.len(), 1);
        let posting = &mut search_env.postings_iterators[0];
        posting.posting_list_iterator.for_each_till_row_id(
            search_env.max_row_id.unwrap_or(RowId::MAX),
            |element| {
                // TODO 过滤掉不合法的 rowid
                let score = element.weight * posting.query_weight;
                search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
            },
        );
    }

    // 将当前剩余长度最长的 postings iter 放到 iterators 的最前面
    fn promote_longest_posting_lists_to_the_front(&self, search_env: &mut SearchEnv) {
        // find index of longest posting list
        // 这里找到的最长 posting list 是 len_to_end（posting list 的剩余长度）长度
        let posting_index = search_env
            .postings_iterators
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.posting_list_iterator
                    .len_to_end()
                    .cmp(&b.posting_list_iterator.len_to_end())
            })
            .map(|(index, _)| index);

        if let Some(posting_index) = posting_index {
            // make sure it is not already at the head
            if posting_index != 0 {
                // swap longest posting list to the head
                search_env.postings_iterators.swap(0, posting_index);
            }
        }
    }

    // 对最长长度的 posting 进行剪枝
    fn prune_longest_posting_list(&self, min_score: f32, search_env: &mut SearchEnv) -> bool {
        if search_env.postings_iterators.is_empty() {
            return false;
        }
        // 将 posting iters 切分为左右两个部分, 左半部分仅包含一个最长的 iter, 右半部分是其余所有的 iter
        let (left_iters, right_iters) = search_env.postings_iterators.split_at_mut(1);

        let longest_indexed_posting_iterator = &mut left_iters[0];
        let longest_posting_iterator = &mut longest_indexed_posting_iterator.posting_list_iterator;

        // 获得最左侧 longest posting iter 的首个未遍历的元素
        if let Some(element) = longest_posting_iterator.peek() {
            // 在 right iterators 中找到最小的 row_id
            let min_row_id_in_right = get_min_row_id(right_iters);
            match min_row_id_in_right {
                Some(min_row_id_in_right) => {
                    match min_row_id_in_right.cmp(&element.row_id) {
                        std::cmp::Ordering::Less => {
                            // 当 right set 中 min row_id 比当前 longest posting 首个 row_id 小的时候, 不可以剪枝
                            return false;
                        },
                        std::cmp::Ordering::Equal => {
                            // 当 right set 中 min row_id 和当前 longest posting 首个 row_id 一样的时候, 也不能剪枝
                            return false;
                        },
                        std::cmp::Ordering::Greater => {
                            // 当 right set 中 min row_id 比当前 longest posting 首个 row_id 大的时候, 可以剪枝
                            // 最好的情形是 longest posting 中最小的 row_id 一直到 right set 中最小的 row_id 这个区间都能够被 cut 掉

                            // 获得 longest posting 能够贡献的最大分数
                            let max_weight_in_longest = element.weight.max(element.max_next_weight);
                            let max_score_contribution = max_weight_in_longest * longest_indexed_posting_iterator.query_weight;

                            // 根据贡献的最大分数判断是否能够剪枝
                            if max_score_contribution <= min_score {
                                let cursor_before_pruning = longest_posting_iterator.current_index();
                                longest_posting_iterator.skip_to(min_row_id_in_right);
                                let cursor_after_pruning = longest_posting_iterator.current_index();
                                return cursor_before_pruning != cursor_after_pruning;
                            }
                        },
                    }
                },
                None => {
                    // min_row_id_in_right 为 None 时, 表示仅剩余左侧 1 个 posting
                    // 直接判断左侧 posting 是否能够全部剪掉就行
                    let max_weight_in_longest = element.weight.max(element.max_next_weight);
                    let max_score_contribution = max_weight_in_longest * longest_indexed_posting_iterator.query_weight;
                    if max_score_contribution <= min_score {
                        longest_posting_iterator.skip_to_end();
                        return true;
                    }
                },
            }
        }
        false
    }

    pub fn search(&self, query: SparseVector, limits: u32) -> TopK {
        let mut search_env = self.pre_search(query.clone(), limits);

        if search_env.postings_iterators.is_empty() {
            return TopK::default();
        }

        let mut best_min_score = f32::MIN;


        // 循环处理每个批次
        loop {
            if search_env.min_row_id.is_none() {
                break;
            }

            let last_batch_id = min(
                search_env.min_row_id.unwrap() + ADVANCE_BATCH_SIZE as RowId,
                search_env.max_row_id.unwrap_or(RowId::MAX),
            );
            self.advance_batch(search_env.min_row_id.unwrap(), last_batch_id, &mut search_env);

            // 剔除已经遍历完成的 posting
            search_env.postings_iterators.retain(|posting_iterator|{
                posting_iterator.posting_list_iterator.len_to_end()!=0
            });

            // 是否所有的 posting 均被消耗
            if search_env.postings_iterators.is_empty() {
                break;
            }

            // 更新 min_row_id
            search_env.min_row_id = get_min_row_id(&mut search_env.postings_iterators);

            if search_env.postings_iterators.len() == 1 {
                self.process_last_posting_list(&mut search_env);
                break;
            }

            // 可能发生剪枝
            if search_env.use_pruning && search_env.top_k.len() >= limits as usize {
                let new_min_score = search_env.top_k.threshold();
                if new_min_score == best_min_score {
                    continue;
                } else {
                    best_min_score = new_min_score;
                }
                // 准备剪枝
                self.promote_longest_posting_lists_to_the_front(&mut search_env);
                // 执行剪枝
                let pruned = self.prune_longest_posting_list(new_min_score, &mut search_env);
                // 剪枝后更新 row id 范围
                if pruned {
                    search_env.min_row_id = get_min_row_id(&mut search_env.postings_iterators);
                }
            }

        }
        search_env.top_k
    }

}


