use crate::{core::{SparseBitmap, TopK}, RowId};

use super::search_posting_iterator::SearchPostingIterator;

pub struct SearchEnv<'a> {
    // single query(sparse_vector) will use these iterators.
    pub postings: Vec<SearchPostingIterator<'a>>,
    // single query(sparse_vector) will use `min_row_id` during search
    pub min_row_id: Option<RowId>,
    pub max_row_id: Option<RowId>,
    pub sparse_bitmap: Option<SparseBitmap>,
    pub use_pruning: bool,
    pub top_k: TopK,
}
