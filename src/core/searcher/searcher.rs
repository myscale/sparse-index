use std::cmp::min;

use log::trace;

use crate::{
    core::{
        dispatch::GenericInvertedIndex, DimId, DimWeight, ElementRead, ScoreType, SparseBitmap, SparseVector, TopK
    },
    ffi::ScoredPointOffset,
    RowId,
};

use super::{
    prune_generic_posting::{get_min_row_id, prune_longest_posting},
    search_env::SearchEnv,
    search_posting_iterator::SearchPostingIterator,
};

const ADVANCE_BATCH_SIZE: usize = 10_000;

#[derive(Debug, Clone)]
pub struct Searcher {
    inverted_index: GenericInvertedIndex,
}

impl Searcher {
    pub fn new(inverted_index: GenericInvertedIndex) -> Self {
        return Self { inverted_index };
    }

    pub fn get_inverted_index(&self) -> &GenericInvertedIndex {
        return &self.inverted_index;
    }

    // Bind SearchEnv inner iterator's lifetime annotation into IndexSearcher Self-Object.
    fn pre_search<'a>(&'a self, sparse_vector: &SparseVector, sparse_bitmap: &Option<SparseBitmap>, limits: u32) -> SearchEnv<'a> {
        let mut postings: Vec<SearchPostingIterator<'a>> = Vec::new();

        // The min and max row_id indicate the range of row IDs that may be used in this query.
        let mut max_row_id = 0;
        let mut min_row_id = u32::MAX;

        for (i, dim_id) in sparse_vector.indices.iter().enumerate() {
            if let Some(generic_posting) =
                self.inverted_index.get_posting_opt(*dim_id, &mut min_row_id, &mut max_row_id)
            {
                postings.push(SearchPostingIterator {
                    generic_posting,
                    dim_id: *dim_id,
                    dim_weight: sparse_vector.values[i],
                });
            }
        }
        // TODO: if enable quantized, we will not use `max_next_weight`, that is to say we should not use pruning.
        let use_pruning =
        sparse_vector.values.iter().all(|v| *v >= 0.0) && self.inverted_index.support_pruning();

        let top_k = TopK::new(limits as usize);

        SearchEnv {
            postings,
            min_row_id: Some(min_row_id),
            max_row_id: Some(max_row_id),
            use_pruning,
            top_k,
            sparse_bitmap: sparse_bitmap.clone(),
        }
    }

    // TODO 应该将 index 中所有的 row_id 给存储起来
    pub fn plain_search(&self, sparse_vector: &SparseVector, sparse_bitmap: &Option<SparseBitmap>, limits: u32) -> TopK {
        let mut search_env: SearchEnv<'_> = self.pre_search(sparse_vector, sparse_bitmap, limits);

        let metrics = self.inverted_index.metrics();

        // iter all rows stored in self.inverted_index.
        for row_id in metrics.min_row_id..=metrics.max_row_id {
            // filter row_id which is already deleted.
            if let Some(bitmap) = &search_env.sparse_bitmap {
                if !bitmap.is_alive(row_id) {
                    continue;
                }
            }
            let mut dim_ids: Vec<DimId> = Vec::with_capacity(sparse_vector.indices.len());
            let mut dim_weights: Vec<DimWeight> = Vec::with_capacity(sparse_vector.values.len());
            for posting in search_env.postings.iter_mut() {
                let generic_posting_ref = &mut posting.generic_posting;
                match generic_posting_ref.get_element_opt(row_id) {
                    Some(element) => {
                        dim_ids.push(posting.dim_id);
                        dim_weights.push(element.weight());
                    }
                    None => {}
                }
            }
            // reconstruct sparse vector and score against query
            let sparse_vector: SparseVector =
                SparseVector { indices: dim_ids, values: dim_weights };
            search_env.top_k.push(ScoredPointOffset {
                score: sparse_vector.score(&sparse_vector).unwrap_or(0.0),
                row_id,
            });
        }
        search_env.top_k
    }

    /// Iterate through all postings involved in the query(sparse-vector).
    /// And for each `Posting`, processing elements within a specified batch range(batch_start_id ~ batch_end_id).
    fn advance_batch(
        &self,
        batch_start_row_id: RowId,
        batch_end_row_id: RowId,
        search_env: &mut SearchEnv,
    ) {
        let batch_size = batch_end_row_id - batch_start_row_id + 1;
        let mut batch_scores: Vec<ScoreType> = vec![0.0; batch_size as usize];

        trace!("[advance_batch] batch_scores len (batch_size):{}, batch_start_row_id:{}, batch_end_row_id:{}", batch_size, batch_start_row_id, batch_end_row_id);
        for posting in search_env.postings.iter_mut() {
            posting.generic_posting.batch_compute(
                &mut batch_scores,
                posting.dim_weight,
                batch_start_row_id,
                batch_end_row_id,
            );
        }

        for (local_id, &score) in batch_scores.iter().enumerate() {
            if score > 0.0 && score > search_env.top_k.threshold() {
                let mut is_alive = true;
                let real_row_id = local_id as RowId + batch_start_row_id;
                if let Some(bitmap) = &search_env.sparse_bitmap {
                    is_alive = bitmap.is_alive(real_row_id)
                }
                if is_alive {
                    search_env.top_k.push(ScoredPointOffset { row_id: real_row_id as RowId, score });
                }
            }
        }
    }

    // only remains one posting.
    fn process_last_posting_list(&self, search_env: &mut SearchEnv) {
        debug_assert_eq!(search_env.postings.len(), 1);
        let posting = &mut search_env.postings[0];
        let query_dim_weight = posting.dim_weight;

        posting.generic_posting.full_compute(
            search_env.max_row_id.unwrap_or(RowId::MAX),
            query_dim_weight,
            &search_env.sparse_bitmap,
            &mut search_env.top_k,
        );
    }

    // move the posting which has longest remain size to the front of iterators.
    fn promote_longest_posting_lists_to_the_front(&self, search_env: &mut SearchEnv) {
        // find index of longest posting list (remain size)
        let posting_index = search_env
            .postings
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.generic_posting.remains().cmp(&b.generic_posting.remains()))
            .map(|(index, _)| index);

        if let Some(posting_index) = posting_index {
            // make sure it is not already at the head
            if posting_index != 0 {
                // swap longest posting list to the head
                search_env.postings.swap(0, posting_index);
            }
        }
    }

    // cut the longest posting.
    fn prune_longest_posting_list(&self, min_score: f32, search_env: &mut SearchEnv) -> bool {
        if search_env.postings.is_empty() {
            return false;
        }
        // split posting iterators into two parts, left contains single longest posting, right contains the others.
        let (left_iters, right_postings) = search_env.postings.split_at_mut(1);

        prune_longest_posting(&mut left_iters[0], min_score, right_postings)
    }

    pub fn search(&self, query: &SparseVector, sparse_bitmap: &Option<SparseBitmap>, limits: u32) -> TopK {
        let mut search_env = self.pre_search(query, sparse_bitmap, limits);

        if search_env.postings.is_empty() {
            return TopK::default();
        }

        let mut best_min_score = f32::MIN;

        // loop process each batch.
        loop {
            if search_env.min_row_id.is_none() {
                break;
            }

            let last_batch_id = min(
                search_env.min_row_id.unwrap_or(0) + ADVANCE_BATCH_SIZE as RowId,
                search_env.max_row_id.unwrap_or(RowId::MAX),
            );
            self.advance_batch(search_env.min_row_id.unwrap_or(0), last_batch_id, &mut search_env);

            // remove the posting already finished iter.
            search_env
                .postings
                .retain(|posting_iterator| posting_iterator.generic_posting.remains() != 0);

            if search_env.postings.is_empty() {
                break;
            }

            // update min_row_id in search_env.
            search_env.min_row_id = get_min_row_id(&mut search_env.postings);

            if search_env.postings.len() == 1 {
                self.process_last_posting_list(&mut search_env);
                break;
            }

            // cut posting.
            if search_env.use_pruning && search_env.top_k.len() >= limits as usize {
                let new_min_score = search_env.top_k.threshold();
                if new_min_score == best_min_score {
                    continue;
                } else {
                    best_min_score = new_min_score;
                }
                // prepare for posting cut.
                self.promote_longest_posting_lists_to_the_front(&mut search_env);
                // execute posting cut.
                let pruned = self.prune_longest_posting_list(new_min_score, &mut search_env);
                // update row_ids range after posting cut.
                if pruned {
                    search_env.min_row_id = get_min_row_id(&mut search_env.postings);
                }
            }
        }
        search_env.top_k
    }
}
