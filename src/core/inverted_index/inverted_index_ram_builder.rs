use crate::core::posting_list::PostingListBuilder;
use crate::core::sparse_vector::SparseVector;
use crate::core::{inverted_index::inverted_index_ram::InvertedIndexRam, DimId};
use crate::RowId;
use std::cmp::{max, min};

/// Builder for InvertedIndexRam
pub struct InvertedIndexBuilder {
    posting_builders: Vec<PostingListBuilder>,
    vector_count: usize,
    memory: usize,
    min_row_id: RowId,
    max_row_id: RowId,
    min_dim_id: DimId,
    max_dim_id: DimId,

    propagate_while_upserting: bool,
    finally_sort: bool,
    finally_propagate: bool,
}

impl Default for InvertedIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndexBuilder {
    /// ## brief
    /// create an InvertedIndexBuilder
    /// ## config
    /// - `propagate_while_upserting`: false
    /// - `finally_sort`: false
    /// - `finally_progagate`: true
    pub fn new() -> InvertedIndexBuilder {
        InvertedIndexBuilder {
            posting_builders: Vec::new(),
            vector_count: 0,
            memory: 0,
            min_row_id: RowId::MAX,
            max_row_id: RowId::MIN,

            min_dim_id: 0,
            max_dim_id: 0,

            propagate_while_upserting: false,
            finally_sort: false,
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

    /// Returns the total memory usage of the InvertedIndexBuilder in bytes.
    pub fn memory_usage(&self) -> usize {
        self.memory
    }

    pub fn vector_count(&self) -> usize {
        self.vector_count
    }

    pub fn min_row_id(&self) -> RowId {
        self.min_row_id
    }

    pub fn max_row_id(&self) -> RowId {
        self.max_row_id
    }

    pub fn min_dim_id(&self) -> DimId {
        self.min_dim_id
    }

    pub fn max_dim_id(&self) -> DimId {
        self.max_dim_id
    }
}

/// Operation
impl InvertedIndexBuilder {
    /// ## brief
    /// add one row into inverted_index_ram
    /// ## return
    /// bool: `true` if operation is `insert`, otherwise is `update`
    pub fn add(&mut self, row_id: RowId, vector: SparseVector) -> bool {
        let mut is_insert_operation = true;
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            // boundary resize.
            if dim_id >= self.posting_builders.len() {
                self.posting_builders.resize_with(dim_id + 1, || {
                    PostingListBuilder::new()
                        .with_propagate_while_upserting(self.propagate_while_upserting)
                        .with_finally_sort(self.finally_sort)
                        .with_finally_propagate(self.finally_propagate)
                });
            }
            // insert new sparse_vector into postings.
            let memory_before = self.posting_builders[dim_id].memory_usage();
            let operation = self.posting_builders[dim_id].add(row_id, weight);
            is_insert_operation &= operation;
            let memory_after = self.posting_builders[dim_id].memory_usage();

            self.memory = self.memory.saturating_add(memory_after - memory_before);
            self.max_dim_id = max(self.max_dim_id, dim_id as u32);
        }
        // update metrics
        if is_insert_operation {
            self.vector_count = self.vector_count.saturating_add(1);
        }
        self.min_row_id = min(self.min_row_id, row_id);
        self.max_row_id = max(self.max_row_id, row_id);

        is_insert_operation
    }

    /// Consumes the builder and returns an InvertedIndexRam
    pub fn build(self) -> InvertedIndexRam {
        InvertedIndexRam {
            postings: self
                .posting_builders
                .into_iter()
                .map(|builder| builder.build())
                .collect(),
            vector_count: self.vector_count,
            min_row_id: self.min_row_id,
            max_row_id: self.max_row_id,
            min_dim_id: self.min_dim_id,
            max_dim_id: self.max_dim_id,
        }
    }
}
