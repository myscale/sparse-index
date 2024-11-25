use std::{cmp::min, marker::PhantomData};

use log::{error, trace};

use crate::{ffi::ScoredPointOffset, RowId};

use super::{
    get_min_row_id, DimId, DimWeight, GenericInvertedIndexMmapType, GenericPostingsIterator,
    PostingListIteratorTrait, QuantizedWeight, ScoreType, SparseVector, TopK,
};
const ADVANCE_BATCH_SIZE: usize = 10_000;

pub struct IndexedPostingListIterator<OW: QuantizedWeight, TW: QuantizedWeight, PI>
where
    PI: PostingListIteratorTrait<TW, OW>,
{
    pub posting_list_iterator: PI,
    pub query_index: DimId,
    pub query_weight: DimWeight,
    pub _ow: PhantomData<OW>,
    pub _tw: PhantomData<TW>,
}

/// Each query(sparse_vector) will generate a `SearchEnv`.
pub struct SearchEnv<'a> {
    // single query(sparse_vector) will use these iterators.
    pub postings_iterators: Vec<GenericPostingsIterator<'a>>,
    // single query(sparse_vector) will use `min_row_id` during search
    pub min_row_id: Option<RowId>,
    pub max_row_id: Option<RowId>,
    pub use_pruning: bool,
    pub top_k: TopK,
}

#[derive(Debug, Clone)]
pub struct IndexSearcher {
    inverted_index: GenericInvertedIndexMmapType,
}

impl IndexSearcher {
    pub fn new(inverted_index: GenericInvertedIndexMmapType) -> Self {
        return Self { inverted_index };
    }

    pub fn get_inverted_index(&self) -> &GenericInvertedIndexMmapType {
        return &self.inverted_index;
    }
    // Bind SearchEnv inner iterator's lifetime annotation into IndexSearcher Self-Object.
    fn pre_search<'a>(&'a self, query: SparseVector, limits: u32) -> SearchEnv<'a> {
        let mut postings_iterators: Option<Vec<GenericPostingsIterator<'a>>> = None;

        // The min and max row_id indicate the range of row IDs that may be used in this query.
        let mut max_row_id = 0;
        let mut min_row_id = u32::MAX;

        for (query_weight_offset, id) in query.indices.iter().enumerate() {
            postings_iterators = self.inverted_index.generate_postings_iterators(
                id,
                query_weight_offset,
                &query.values,
                &mut min_row_id,
                &mut max_row_id,
            );
        }
        // TODO: if enable quantized, we will not use `max_next_weight`, that is to say we should not use pruning.
        let use_pruning = query.values.iter().all(|v| *v >= 0.0);

        let top_k = TopK::new(limits as usize);

        SearchEnv {
            postings_iterators: postings_iterators.unwrap_or(vec![]),
            min_row_id: Some(min_row_id),
            max_row_id: Some(max_row_id),
            use_pruning,
            top_k,
        }
    }

    // TODO 应该将 index 中所有的 row_id 给存储起来
    pub fn plain_search(&self, query: SparseVector, limits: u32) -> TopK {
        let mut search_env: SearchEnv<'_> = self.pre_search(query.clone(), limits);

        let metrics = self.inverted_index.metrics();

        // iter all rows stored in self.inverted_index.
        for row_id in metrics.min_row_id..(metrics.max_row_id+1) {
            let mut indices = Vec::with_capacity(query.indices.len());
            let mut values = Vec::with_capacity(query.values.len());

            // collect indices and values for the current record id from the query's posting lists *only*
            for posting_iterator in search_env.postings_iterators.iter_mut() {
                // rely on underlying binary search as the posting lists are sorted by record id
                match posting_iterator {
                    GenericPostingsIterator::F32NoQuantized(indexed_posting_list_iterator) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(element.weight);
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::F32Quantized(indexed_posting_list_iterator) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(element.weight);
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::F16NoQuantized(indexed_posting_list_iterator) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(half::f16::to_f32(element.weight));
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::F16Quantized(indexed_posting_list_iterator) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(half::f16::to_f32(element.weight));
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::U8NoQuantized(indexed_posting_list_iterator) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(u8::to_f32(element.weight));
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::CompressedF32NoQuantized(
                        indexed_posting_list_iterator,
                    ) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(element.weight);
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::CompressedF32Quantized(
                        indexed_posting_list_iterator,
                    ) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(element.weight);
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::CompressedF16NoQuantized(
                        indexed_posting_list_iterator,
                    ) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(half::f16::to_f32(element.weight));
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::CompressedF16Quantized(
                        indexed_posting_list_iterator,
                    ) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(half::f16::to_f32(element.weight));
                            }
                            None => {} // no match for posting list
                        }
                    }
                    GenericPostingsIterator::CompressedU8NoQuantized(
                        indexed_posting_list_iterator,
                    ) => {
                        match indexed_posting_list_iterator.posting_list_iterator.skip_to(row_id) {
                            Some(element) => {
                                // match for posting list
                                indices.push(indexed_posting_list_iterator.query_index);
                                values.push(u8::to_f32(element.weight));
                            }
                            None => {} // no match for posting list
                        }
                    }
                }
            }
            // reconstruct sparse vector and score against query
            let sparse_vector: SparseVector = SparseVector { indices, values };
            search_env.top_k.push(ScoredPointOffset {
                score: sparse_vector.score(&query).unwrap_or(0.0),
                row_id,
            });
        }

        search_env.top_k
    }

    /// Iterate through all postings involved in the query(sparse-vector).
    /// And for each `Posting`, processing elements within a specified batch range(batch_start_id ~ batch_end_id).
    fn advance_batch(
        &self,
        batch_start_id: RowId,
        batch_end_id: RowId,
        search_env: &mut SearchEnv,
    ) {
        let batch_size = batch_end_id - batch_start_id + 1;
        let mut batch_scores: Vec<ScoreType> = vec![0.0; batch_size as usize];

        trace!("[advance_batch] batch_scores len (batch_size):{}, batch_start_id:{}, batch_end_id:{}", batch_size, batch_start_id, batch_end_id);
        for posting in search_env.postings_iterators.iter_mut() {
            match posting {
                GenericPostingsIterator::F32NoQuantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = element.weight * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            trace!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::F32Quantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = element.weight * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::F16NoQuantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = half::f16::to_f32(element.weight)
                                * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::F16Quantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = half::f16::to_f32(element.weight)
                                * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::U8NoQuantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = u8::to_f32(element.weight)
                                * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::CompressedF32NoQuantized(
                    indexed_posting_list_iterator,
                ) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = element.weight * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::CompressedF32Quantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = element.weight * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::CompressedF16NoQuantized(
                    indexed_posting_list_iterator,
                ) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = half::f16::to_f32(element.weight)
                                * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::CompressedF16Quantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = half::f16::to_f32(element.weight)
                                * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
                GenericPostingsIterator::CompressedU8NoQuantized(indexed_posting_list_iterator) => {
                    indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                        batch_end_id,
                        |element| {
                            if element.row_id < batch_start_id || element.row_id > batch_end_id {
                                error!("row id range error when iter posting element till row id.");
                                return;
                            }

                            let score = u8::to_f32(element.weight)
                                * indexed_posting_list_iterator.query_weight;
                            let local_id = (element.row_id - batch_start_id) as usize;
                            // debug!("[advance_batch] local_id:{}, element_row_id:{}", local_id, element.row_id);
                            batch_scores[local_id] += score;
                        },
                    );
                }
            }
        }

        for (local_id, &score) in batch_scores.iter().enumerate() {
            if score > 0.0 && score > search_env.top_k.threshold() {
                // TOOD: if real_id is filtered, skip it.

                let real_id = local_id + batch_start_id as usize;
                search_env.top_k.push(ScoredPointOffset { row_id: real_id as RowId, score });
            }
        }
    }

    // only remains one posting.
    fn process_last_posting_list(&self, search_env: &mut SearchEnv) {
        debug_assert_eq!(search_env.postings_iterators.len(), 1);
        let posting = &mut search_env.postings_iterators[0];

        // TODO: filter row_id for LWD.
        match posting {
            GenericPostingsIterator::F32NoQuantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = element.weight * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::F32Quantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = element.weight * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::F16NoQuantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = half::f16::to_f32(element.weight)
                            * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::F16Quantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = half::f16::to_f32(element.weight)
                            * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::U8NoQuantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score =
                            u8::to_f32(element.weight) * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::CompressedF32NoQuantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = element.weight * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::CompressedF32Quantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = element.weight * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::CompressedF16NoQuantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = half::f16::to_f32(element.weight)
                            * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::CompressedF16Quantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score = half::f16::to_f32(element.weight)
                            * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
            GenericPostingsIterator::CompressedU8NoQuantized(indexed_posting_list_iterator) => {
                indexed_posting_list_iterator.posting_list_iterator.for_each_till_row_id(
                    search_env.max_row_id.unwrap_or(RowId::MAX),
                    |element| {
                        let score =
                            u8::to_f32(element.weight) * indexed_posting_list_iterator.query_weight;
                        search_env.top_k.push(ScoredPointOffset { score, row_id: element.row_id });
                    },
                );
            }
        }
    }

    // move the posting which has longest remain size to the front of iterators.
    fn promote_longest_posting_lists_to_the_front(&self, search_env: &mut SearchEnv) {
        // find index of longest posting list (remain size)
        let posting_index = search_env
            .postings_iterators
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.remains().cmp(&b.remains()))
            .map(|(index, _)| index);

        if let Some(posting_index) = posting_index {
            // make sure it is not already at the head
            if posting_index != 0 {
                // swap longest posting list to the head
                search_env.postings_iterators.swap(0, posting_index);
            }
        }
    }

    // cut the longest posting.
    fn prune_longest_posting_list(&self, min_score: f32, search_env: &mut SearchEnv) -> bool {
        if search_env.postings_iterators.is_empty() {
            return false;
        }
        // split posting iterators into two parts, left contains single longest posting, right contains the others.
        let (left_iters, right_iters) = search_env.postings_iterators.split_at_mut(1);

        let longest_indexed_posting_iterator: &mut GenericPostingsIterator<'_> = &mut left_iters[0];

        longest_indexed_posting_iterator.prune_longest_posting_list(min_score, right_iters)
    }

    pub fn search(&self, query: SparseVector, limits: u32) -> TopK {
        let mut search_env = self.pre_search(query.clone(), limits);

        if search_env.postings_iterators.is_empty() {
            return TopK::default();
        }

        let mut best_min_score = f32::MIN;

        // loop process each batch.
        loop {
            if search_env.min_row_id.is_none() {
                break;
            }

            let last_batch_id = min(
                search_env.min_row_id.unwrap() + ADVANCE_BATCH_SIZE as RowId,
                search_env.max_row_id.unwrap_or(RowId::MAX),
            );
            self.advance_batch(search_env.min_row_id.unwrap(), last_batch_id, &mut search_env);

            // remove the posting already finished iter.
            search_env.postings_iterators.retain(|posting_iterator| {
                posting_iterator.remains() != 0
            });

            if search_env.postings_iterators.is_empty() {
                break;
            }

            // update min_row_id in search_env.
            search_env.min_row_id = get_min_row_id(&mut search_env.postings_iterators);

            if search_env.postings_iterators.len() == 1 {
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
                    search_env.min_row_id = get_min_row_id(&mut search_env.postings_iterators);
                }
            }
        }
        search_env.top_k
    }
}
