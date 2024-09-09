use crate::core::common::types::{DimId, DimWeight, ElementOffsetType};
use crate::core::inverted_index::InvertedIndex;
use crate::core::posting_list::{PostingListIter, PostingListIterator};
use crate::core::scores::PooledScoresHandle;
use crate::core::sparse_vector::RemappedSparseVector;
use crate::ffi::ScoredPointOffset;
use std::cmp::{max, min, Ordering};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use super::scores::TopK;


/// Iterator over posting lists with a reference to the corresponding query index and weight
pub struct IndexedPostingListIterator<T: PostingListIter> {
    posting_list_iterator: T,
    query_index: DimId,
    query_weight: DimWeight,
}

/// Making this larger makes the search faster but uses more (pooled) memory
const ADVANCE_BATCH_SIZE: usize = 10_000;

pub struct SearchContext<'a, 'b, T: PostingListIter = PostingListIterator<'a>> {
    postings_iterators: Vec<IndexedPostingListIterator<T>>,
    query: RemappedSparseVector,
    top: usize,
    is_stopped: &'a AtomicBool,
    top_results: TopK,
    min_record_id: Option<ElementOffsetType>, // min_record_id ids across all posting lists
    max_record_id: ElementOffsetType,         // max_record_id ids across all posting lists
    pooled: PooledScoresHandle<'b>,           // handle to pooled scores
    use_pruning: bool,
}

impl<'a, 'b, T: PostingListIter> SearchContext<'a, 'b, T> {
    pub fn new(
        query: RemappedSparseVector,
        top: usize,
        inverted_index: &'a impl InvertedIndex<Iter<'a> = T>,
        pooled: PooledScoresHandle<'b>,
        is_stopped: &'a AtomicBool,
    ) -> SearchContext<'a, 'b, T> {
        let mut postings_iterators: Vec<IndexedPostingListIterator<T>> = Vec::new();
        let mut max_record_id = 0;
        let mut min_record_id = u32::MAX;

        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            if let Some(mut it) = inverted_index.get(id) {
                if let (Some(first), Some(last_id)) = (it.peek(), it.last_id()) {
                    let min_record_id_posting = first.row_id;
                    min_record_id = min(min_record_id, min_record_id_posting);

                    let max_record_id_posting = last_id;
                    max_record_id = max(max_record_id, max_record_id_posting);

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

        let top_results = TopK::new(top);
        // Query vectors with negative values can NOT use the pruning mechanism which relies on the pre-computed `max_next_weight`.
        // The max contribution per posting list that we calculate is not made to compute the max value of two negative numbers.
        // This is a limitation of the current pruning implementation.
        let use_pruning = T::reliable_max_next_weight() && query.values.iter().all(|v| *v >= 0.0);
        let min_record_id = Some(min_record_id);
        SearchContext {
            postings_iterators, // 根据 query(sparse-vector) 收集到的所有相关的 posting lists iterator
            query,              // query sparse-vector
            top,                // top 个候选值
            is_stopped,         // 暂时不清楚什么作用
            top_results,        // K 个候选值
            min_record_id,      // query(sparse-vector) 涉及到的最小 row_id
            max_record_id,      // query(sparse-vector) 涉及到的最大 row_id
            pooled,             // 分数池
            use_pruning,        // 是否进行加速剪枝
        }
    }

    /// Plain search against the given ids without any pruning
    pub fn plain_search(&mut self, ids: &[ElementOffsetType]) -> Vec<ScoredPointOffset> {
        // sort ids to fully leverage posting list iterator traversal
        let mut sorted_ids = ids.to_vec();
        sorted_ids.sort_unstable();

        for id in sorted_ids {
            // check for cancellation
            // TODO: 这个 stop 应该可以给删除掉，感觉没有这样的需求（或者 CK 可以传递一个 time_out 时间，超过了就 break？）
            if self.is_stopped.load(Relaxed) {
                break;
            }

            let mut indices = Vec::with_capacity(self.query.indices.len());
            let mut values = Vec::with_capacity(self.query.values.len());
            // collect indices and values for the current record id from the query's posting lists *only*
            for posting_iterator in self.postings_iterators.iter_mut() {
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
            let sparse_vector = RemappedSparseVector { indices, values };
            self.top_results.push(ScoredPointOffset {
                score: sparse_vector.score(&self.query).unwrap_or(0.0),
                row_id: id,
            });
        }
        let top = std::mem::take(&mut self.top_results);
        top.into_vec()
    }

    /// 在一个 batch 范围中进行分数计算
    /// TODO：可以考虑多线程 batch 访问 PostingList 进行加速？
    pub fn advance_batch<F: Fn(ElementOffsetType) -> bool>(
        &mut self,
        batch_start_id: ElementOffsetType,
        batch_last_id: ElementOffsetType,
        filter_condition: &F, // 符合 filter_condition 的才会被纳入计算
    ) {
        // init batch scores
        let batch_len = batch_last_id - batch_start_id + 1;
        // pooled.scores 就是一个数组
        self.pooled.scores.clear();
        self.pooled.scores.resize(batch_len as usize, 0.0);

        // 存储一个 batch 中 row_id 对应的 weight 权重之和
        // TODO: 这块儿设计的也太不清晰了，写点儿人能看懂的
        for posting in self.postings_iterators.iter_mut() {
            posting.posting_list_iterator.for_each_till_id(
                batch_last_id,                     // id
                self.pooled.scores.as_mut_slice(), // Ctx
                |scores, id, weight| {
                    // impl FnMut(&mut Ctx, ElementOffsetType, DimWeight)
                    let element_score = weight * posting.query_weight;
                    let local_id = (id - batch_start_id) as usize;
                    // SAFETY: `id` is within `batch_start_id..=batch_last_id`
                    // Thus, `local_id` is within `0..batch_len`.
                    *unsafe { scores.get_unchecked_mut(local_id) } += element_score;
                },
            );
        }

        for (local_index, &score) in self.pooled.scores.iter().enumerate() {
            // publish only the non-zero scores above the current min to beat
            if score != 0.0 && score > self.top_results.threshold() {
                let real_id = batch_start_id + local_index as ElementOffsetType;
                // do not score if filter condition is not satisfied
                if !filter_condition(real_id) {
                    continue;
                }
                let score_point_offset = ScoredPointOffset {
                    row_id: real_id,
                    score,
                };
                self.top_results.push(score_point_offset);
            }
        }
    }

    /// Compute scores for the last posting list quickly
    fn process_last_posting_list<F: Fn(ElementOffsetType) -> bool>(
        &mut self,
        filter_condition: &F,
    ) {
        // TODO 怎么觉得这个 self.postings_iterators 的长度不一定是 1 呢？
        debug_assert_eq!(self.postings_iterators.len(), 1);
        let posting = &mut self.postings_iterators[0];
        posting.posting_list_iterator.for_each_till_id(
            ElementOffsetType::MAX,
            &mut (),
            |_, id, weight| {
                // do not score if filter condition is not satisfied
                if !filter_condition(id) {
                    return;
                }
                let score = weight * posting.query_weight;
                self.top_results
                    .push(ScoredPointOffset { score, row_id: id });
            },
        );
    }

    /// 返回 to_inspect 所有的 Iterator 中最小的 record_id
    fn next_min_id(to_inspect: &mut [IndexedPostingListIterator<T>]) -> Option<ElementOffsetType> {
        let mut min_record_id = None;

        // Iterate to find min record id at the head of the posting lists
        for posting_iterator in to_inspect.iter_mut() {
            if let Some(next_element) = posting_iterator.posting_list_iterator.peek() {
                match min_record_id {
                    None => min_record_id = Some(next_element.row_id), // first record with matching id
                    Some(min_id_seen) => {
                        // update min record id if smaller
                        if next_element.row_id < min_id_seen {
                            min_record_id = Some(next_element.row_id);
                        }
                    }
                }
            }
        }

        min_record_id
    }

    /// Make sure the longest posting list is at the head of the posting list iterators
    /// 将当前剩余最长的 posting iterator 放置到 iterators 最前面
    fn promote_longest_posting_lists_to_the_front(&mut self) {
        // find index of longest posting list
        // 这里找到的最长 posting list 是 len_to_end（posting list 的剩余长度）长度
        let posting_index = self
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
                self.postings_iterators.swap(0, posting_index);
            }
        }
    }

    /// Search for the top k results that satisfy the filter condition
    pub fn search<F: Fn(ElementOffsetType) -> bool>(
        &mut self,
        filter_condition: &F,
    ) -> Vec<ScoredPointOffset> {
        if self.postings_iterators.is_empty() {
            return Vec::new();
        }
        let mut best_min_score = f32::MIN;
        // loop 用来循环处理每个批次搜索
        loop {
            // check for cancellation (atomic amortized by batch)
            // TODO: 支持 CK 传递一个 timeout 的时间？超过时间返回一个 Error？
            if self.is_stopped.load(Relaxed) {
                break;
            }

            // prepare next iterator of batched ids
            let Some(start_batch_id) = self.min_record_id else {
                break;
            };

            // 批次结束 compute batch range of contiguous ids for the next batch
            let last_batch_id = min(
                start_batch_id + ADVANCE_BATCH_SIZE as u32,
                self.max_record_id,
            );

            // advance and score posting lists iterators
            // 在当前的 batch 范围计算分数
            self.advance_batch(start_batch_id, last_batch_id, filter_condition);

            // remove empty posting lists if necessary
            // 如果倒排列表已经被完全遍历（所有的 row_id 都计算过分数），将它从迭代器中移除
            self.postings_iterators.retain(|posting_iterator| {
                posting_iterator.posting_list_iterator.len_to_end() != 0
            });

            // update min_record_id
            self.min_record_id = Self::next_min_id(&mut self.postings_iterators);

            // check if all posting lists are exhausted
            if self.postings_iterators.is_empty() {
                break;
            }

            // if only one posting list left, we can score it quickly
            if self.postings_iterators.len() == 1 {
                self.process_last_posting_list(filter_condition);
                break;
            }

            // we potentially have enough results to prune low performing posting lists
            if self.use_pruning && self.top_results.len() >= self.top {
                // current min score
                let new_min_score = self.top_results.threshold();
                if new_min_score == best_min_score {
                    // no improvement in lowest best score since last pruning - skip pruning
                    continue;
                } else {
                    best_min_score = new_min_score;
                }
                // make sure the first posting list is the longest for pruning
                self.promote_longest_posting_lists_to_the_front();

                // prune posting list that cannot possibly contribute to the top results
                let pruned = self.prune_longest_posting_list(new_min_score);
                if pruned {
                    // update min_record_id
                    // 在剪枝之后，min_id 可能会发生变化，需要更新
                    self.min_record_id = Self::next_min_id(&mut self.postings_iterators);
                }
            }
        }
        // posting iterators exhausted, return result queue
        let queue = std::mem::take(&mut self.top_results);
        queue.into_vec()
    }

    /// Prune posting lists that cannot possibly contribute to the top results
    /// Assumes longest posting list is at the head of the posting list iterators
    /// Returns true if the longest posting list was pruned
    pub fn prune_longest_posting_list(&mut self, min_score: f32) -> bool {
        if self.postings_iterators.is_empty() {
            return false;
        }
        // peek first element of longest posting list
        // 将 postings iterators 切分为两个部分，左半部分只包含一个最长的 iterator，右半部分是剩下的所有 iterator
        let (longest_posting_iterator, rest_iterators) = self.postings_iterators.split_at_mut(1);
        // 最长的 iterator
        let longest_posting_iterator = &mut longest_posting_iterator[0];
        // 获取 longest iterator 首个元素
        if let Some(element) = longest_posting_iterator.posting_list_iterator.peek() {
            // 在 rest_iterators 中找到最小的 row_id
            let next_min_id_in_others = Self::next_min_id(rest_iterators);
            match next_min_id_in_others {
                Some(next_min_id) => {
                    match next_min_id.cmp(&element.row_id) {
                        Ordering::Equal => {
                            // if the next min id in the other posting lists is the same as the current one,
                            // we can't prune the current element as it needs to be scored properly across posting lists
                            // 如果在 rest posting iterators 中，nex min id 与 longest_iterator.peek 的 row_id 一样，那么不能剪枝
                            return false;
                        }
                        Ordering::Less => {
                            // we can't prune as there the other posting lists contains smaller smaller ids that need to scored first
                            // 当别的 rest min id 比 longest_iterator.peek 的 row_id 小的时候，也不能剪枝
                            return false;
                        }
                        Ordering::Greater => {
                            // next_min_id is > element.record_id there is a chance to prune up to `next_min_id`
                            // check against the max possible score using the `max_next_weight`
                            // we can under prune as we should actually check the best score up to `next_min_id` - 1 only
                            // instead of the max possible score but it is not possible to know the best score up to `next_min_id` - 1
                            // rest 中最小的 row_id 比当前的 longest row_id 更大，说明可以剪枝

                            // longest_posting 中最大的 weight
                            let max_weight_from_list = element.weight.max(element.max_next_weight);

                            // longest_posting 只是对应了一个维度, 也就是对应 query 的一个 index, 这里计算两个点的内积
                            let max_score_contribution =
                                max_weight_from_list * longest_posting_iterator.query_weight;

                            // 点积结果出发了剪枝
                            if max_score_contribution <= min_score {
                                // prune to next_min_id
                                let longest_posting_iterator =
                                    &mut self.postings_iterators[0].posting_list_iterator;
                                let position_before_pruning =
                                    longest_posting_iterator.current_index();
                                longest_posting_iterator.skip_to(next_min_id);
                                let position_after_pruning =
                                    longest_posting_iterator.current_index();
                                // check if pruning took place
                                return position_before_pruning != position_after_pruning;
                            }
                        }
                    }
                }
                None => {
                    // the current posting list is the only one left, we can potentially skip it to the end
                    // check against the max possible score using the `max_next_weight`
                    let max_weight_from_list = element.weight.max(element.max_next_weight);
                    let max_score_contribution =
                        max_weight_from_list * longest_posting_iterator.query_weight;
                    if max_score_contribution <= min_score {
                        // prune to the end!
                        let longest_posting_iterator = &mut self.postings_iterators[0];
                        longest_posting_iterator.posting_list_iterator.skip_to_end();
                        return true;
                    }
                }
            }
        }
        // no pruning took place
        false
    }
}

#[cfg(test)]
mod test2 {
    use crate::core::inverted_index::InvertedIndexBuilder;
    use crate::core::inverted_index::InvertedIndexRam;
    use crate::core::scores::{PooledScoresHandle, ScoresMemoryPool};
    use crate::core::search_context::SearchContext;
    use crate::core::sparse_vector::RemappedSparseVector;
    use std::sync::atomic::AtomicBool;
    use std::sync::OnceLock;

    static TEST_SCORES_POOL: OnceLock<ScoresMemoryPool> = OnceLock::new();

    fn get_pooled_scores() -> PooledScoresHandle<'static> {
        TEST_SCORES_POOL
            .get_or_init(ScoresMemoryPool::default)
            .get()
    }

    #[test]
    fn plain_search_all_test() {
        let mut builder = InvertedIndexBuilder::new();
        // id 是 record id
        // builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
        // builder.add(2, [(1, 20.0), (3, 20.0)].into());
        // builder.add(3, [(1, 30.0), (3, 30.0)].into());
        builder.add(1, [(1, 7.01), (2, 3.02), (3, 8.03)].into());
        builder.add(2, [(1, 4.01), (3, 0.02), (5, 9.03)].into());
        builder.add(3, [(4, 2.11), (7, 0.82)].into());
        let inverted_index_ram: InvertedIndexRam = builder.build();

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 3, 5],
                values: vec![0.4, 1.6, 2.1],
            },
            5,
            &inverted_index_ram,
            get_pooled_scores(),
            &is_stopped,
        );

        let scores = search_context.plain_search(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        println!("{:?}", scores);
    }
}

#[cfg(test)]
// 允许创建可以接受泛型参数的测试模块
#[generic_tests::define]
mod tests {
    use std::any::TypeId;
    use std::borrow::Cow;
    use std::sync::OnceLock;

    use rand::Rng;
    use tempfile::TempDir;

    use super::*;
    use crate::core::common::*;
    use crate::core::inverted_index::*;
    use crate::core::scores::ScoresMemoryPool;
    use crate::core::sparse_vector::utils::random_sparse_vector;
    use crate::core::sparse_vector::SparseVector;
    // ---- Test instantiations ----

    #[instantiate_tests(<InvertedIndexRam>)]
    mod ram {}

    #[instantiate_tests(<InvertedIndexMmap>)]
    mod mmap {}

    #[instantiate_tests(<InvertedIndexImmutableRam>)]
    mod iram {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<f32>>)]
    mod iram_f32 {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<half::f16>>)]
    mod iram_f16 {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<u8>>)]
    mod iram_u8 {}

    #[instantiate_tests(<InvertedIndexCompressedImmutableRam<QuantizedU8>>)]
    mod iram_q8 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<f32>>)]
    mod mmap_f32 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<half::f16>>)]
    mod mmap_f16 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<u8>>)]
    mod mmap_u8 {}

    #[instantiate_tests(<InvertedIndexCompressedMmap<QuantizedU8>>)]
    mod mmap_q8 {}

    // --- End of test instantiations ---

    static TEST_SCORES_POOL: OnceLock<ScoresMemoryPool> = OnceLock::new();

    fn get_pooled_scores() -> PooledScoresHandle<'static> {
        TEST_SCORES_POOL
            .get_or_init(ScoresMemoryPool::default)
            .get()
    }

    /// Match all filter condition for testing
    fn match_all(_p: ElementOffsetType) -> bool {
        true
    }

    /// Helper struct to store both an index and a temporary directory
    struct TestIndex<I: InvertedIndex> {
        index: I,
        temp_dir: TempDir,
    }

    impl<I: InvertedIndex> TestIndex<I> {
        fn from_ram(ram_index: InvertedIndexRam) -> Self {
            let temp_dir = tempfile::Builder::new()
                .prefix("test_index_dir")
                .tempdir()
                .unwrap();
            TestIndex {
                index: I::from_ram_index(Cow::Owned(ram_index), &temp_dir).unwrap(),
                temp_dir,
            }
        }
    }

    /// Round scores to allow some quantization errors
    fn round_scores<I: 'static>(mut scores: Vec<ScoredPointOffset>) -> Vec<ScoredPointOffset> {
        let errors_allowed_for = [
            TypeId::of::<InvertedIndexCompressedImmutableRam<QuantizedU8>>(),
            TypeId::of::<InvertedIndexCompressedMmap<QuantizedU8>>(),
        ];
        if errors_allowed_for.contains(&TypeId::of::<I>()) {
            let precision = 0.25;
            scores.iter_mut().for_each(|score| {
                score.score = (score.score / precision).round() * precision;
            });
            scores
        } else {
            scores
        }
    }

    #[test]
    fn test_empty_query<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram(InvertedIndexRam::empty());

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector::default(), // empty query vector
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );
        assert_eq!(search_context.search(&match_all), Vec::new());
    }

    #[test]
    fn search_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    row_id: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    row_id: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    row_id: 1
                },
            ]
        );
    }

    #[test]
    fn search_with_update_test<I: InvertedIndex + 'static>() {
        if TypeId::of::<I>() != TypeId::of::<InvertedIndexRam>() {
            // Only InvertedIndexRam supports upserts
            return;
        }

        let mut index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    row_id: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    row_id: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    row_id: 1
                },
            ]
        );
        drop(search_context);

        // update index with new point
        index.index.upsert(
            4,
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![40.0, 40.0, 40.0],
            },
            None,
        );
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            10,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.search(&match_all),
            vec![
                ScoredPointOffset {
                    score: 120.0,
                    row_id: 4
                },
                ScoredPointOffset {
                    score: 90.0,
                    row_id: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    row_id: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    row_id: 1
                },
            ]
        );
    }

    #[test]
    fn search_with_hot_key_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
            builder.add(4, [(1, 1.0)].into());
            builder.add(5, [(1, 2.0)].into());
            builder.add(6, [(1, 3.0)].into());
            builder.add(7, [(1, 4.0)].into());
            builder.add(8, [(1, 5.0)].into());
            builder.add(9, [(1, 6.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    row_id: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    row_id: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    row_id: 1
                },
            ]
        );

        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            4,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            round_scores::<I>(search_context.search(&match_all)),
            vec![
                ScoredPointOffset {
                    score: 90.0,
                    row_id: 3
                },
                ScoredPointOffset {
                    score: 60.0,
                    row_id: 2
                },
                ScoredPointOffset {
                    score: 30.0,
                    row_id: 1
                },
                ScoredPointOffset {
                    score: 6.0,
                    row_id: 9
                },
            ]
        );
    }

    #[test]
    fn pruning_single_to_end_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0)].into());
            builder.add(2, [(1, 20.0)].into());
            builder.add(3, [(1, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            0
        );
    }

    #[test]
    fn pruning_multi_to_end_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0)].into());
            builder.add(2, [(1, 20.0)].into());
            builder.add(3, [(1, 30.0)].into());
            builder.add(5, [(3, 10.0)].into());
            builder.add(6, [(2, 20.0), (3, 20.0)].into());
            builder.add(7, [(2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        // assuming we have gathered enough results and want to prune the longest posting list
        assert!(search_context.prune_longest_posting_list(30.0));
        // the longest posting list was pruned to the end
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            0
        );
    }

    #[test]
    fn pruning_multi_under_prune_test<I: InvertedIndex>() {
        if !I::Iter::reliable_max_next_weight() {
            return;
        }

        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0)].into());
            builder.add(2, [(1, 20.0)].into());
            builder.add(3, [(1, 20.0)].into());
            builder.add(4, [(1, 10.0)].into());
            builder.add(5, [(3, 10.0)].into());
            builder.add(6, [(1, 20.0), (2, 20.0), (3, 20.0)].into());
            builder.add(7, [(1, 40.0), (2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            1,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        // one would expect this to prune up to `6` but it does not happen it practice because we are under pruning by design
        // we should actually check the best score up to `6` - 1 only instead of the max possible score (40.0)
        assert!(!search_context.prune_longest_posting_list(30.0));

        assert!(search_context.prune_longest_posting_list(40.0));
        // the longest posting list was pruned to the end
        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            2 // 6, 7
        );
    }

    /// Generates a random inverted index with `num_vectors` vectors
    fn random_inverted_index<R: Rng + ?Sized>(
        rnd_gen: &mut R,
        num_vectors: u32,
        max_sparse_dimension: usize,
    ) -> InvertedIndexRam {
        let mut inverted_index_ram = InvertedIndexRam::empty();

        for i in 1..=num_vectors {
            let SparseVector { indices, values } =
                random_sparse_vector(rnd_gen, max_sparse_dimension);
            let vector = RemappedSparseVector::new(indices, values).unwrap();
            inverted_index_ram.upsert(i, vector, None);
        }
        inverted_index_ram
    }

    #[test]
    fn promote_longest_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (3, 20.0)].into());
            builder.add(3, [(2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0, 1.0, 1.0],
            },
            3,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            2
        );

        search_context.promote_longest_posting_lists_to_the_front();

        assert_eq!(
            search_context.postings_iterators[0]
                .posting_list_iterator
                .len_to_end(),
            3
        );
    }

    // #[test]
    // fn plain_search_all_test<I: InvertedIndex>() {
    //     let index = TestIndex::<I>::from_ram({
    //         let mut builder = InvertedIndexBuilder::new();
    //         // id 是 record id
    //         // builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
    //         // builder.add(2, [(1, 20.0), (3, 20.0)].into());
    //         // builder.add(3, [(1, 30.0), (3, 30.0)].into());
    //         builder.add(1, [(1, 7.01), (2, 3.02), (3, 8.03)].into());
    //         builder.add(2, [(1, 4.01), (3, 0.02), (5, 9.03)].into());
    //         builder.add(3, [(4, 2.11), (7, 0.82)].into());
    //         builder.build()
    //     });
    //
    //     let is_stopped = AtomicBool::new(false);
    //     let mut search_context = SearchContext::new(
    //         RemappedSparseVector {
    //             indices: vec![1, 3, 5],
    //             values: vec![0.4, 1.6, 2.1],
    //         },
    //         10,
    //         &index.index,
    //         get_pooled_scores(),
    //         &is_stopped,
    //     );
    //
    //     let scores = search_context.plain_search(&[0,1,2,3,4,5,6,7,8,9,10]);
    //     assert_eq!(
    //         round_scores::<I>(scores),
    //         vec![
    //             ScoredPointOffset {
    //                 row_id: 3,
    //                 score: 60.0
    //             },
    //             ScoredPointOffset {
    //                 row_id: 2,
    //                 score: 40.0
    //             },
    //             ScoredPointOffset {
    //                 row_id: 1,
    //                 score: 30.0
    //             },
    //         ]
    //     );
    // }

    #[test]
    fn plain_search_gap_test<I: InvertedIndex>() {
        let index = TestIndex::<I>::from_ram({
            let mut builder = InvertedIndexBuilder::new();
            builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0)].into());
            builder.add(2, [(1, 20.0), (3, 20.0)].into());
            builder.add(3, [(2, 30.0), (3, 30.0)].into());
            builder.build()
        });

        // query vector has a gap for dimension 2
        let is_stopped = AtomicBool::new(false);
        let mut search_context = SearchContext::new(
            RemappedSparseVector {
                indices: vec![1, 3],
                values: vec![1.0, 1.0],
            },
            3,
            &index.index,
            get_pooled_scores(),
            &is_stopped,
        );

        let scores = search_context.plain_search(&[1, 2, 3]);
        assert_eq!(
            round_scores::<I>(scores),
            vec![
                ScoredPointOffset {
                    row_id: 2,
                    score: 40.0
                },
                ScoredPointOffset {
                    row_id: 3,
                    score: 30.0 // the dimension 2 did not contribute to the score
                },
                ScoredPointOffset {
                    row_id: 1,
                    score: 20.0 // the dimension 2 did not contribute to the score
                },
            ]
        );
    }
}
