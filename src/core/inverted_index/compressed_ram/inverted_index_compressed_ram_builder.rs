use crate::core::sparse_vector::SparseVector;
use crate::core::CompressedPostingBuilder;
use crate::core::CompressedPostingList;
use crate::core::DimId;
use crate::core::InvertedIndexMetrics;
use crate::core::QuantizedWeight;
use crate::RowId;

use super::CompressedInvertedIndexRam;

/// Builder for InvertedIndexRam
pub struct InvertedIndexCompressedRamBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    posting_builders: Vec<CompressedPostingBuilder<OW, TW>>,
    memory: usize,
    metrics: InvertedIndexMetrics,

    propagate_while_upserting: bool,
    finally_sort: bool,
    finally_propagate: bool,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> Default
    for InvertedIndexCompressedRamBuilder<OW, TW>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexCompressedRamBuilder<OW, TW> {
    /// ## brief
    /// create an InvertedIndexCompressedRamBuilder
    /// ## config
    /// - `propagate_while_upserting`: false
    /// - `finally_sort`: false
    /// - `finally_progagate`: true
    pub fn new() -> InvertedIndexCompressedRamBuilder<OW, TW> {
        InvertedIndexCompressedRamBuilder::<OW, TW> {
            posting_builders: Vec::new(),
            memory: 0,
            metrics: InvertedIndexMetrics::default(),

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

    /// Returns the total memory usage of the InvertedIndexCompressedRamBuilder in bytes.
    pub fn memory_usage(&self) -> usize {
        self.memory
    }

    pub fn vector_count(&self) -> usize {
        self.metrics.vector_count
    }

    pub fn min_row_id(&self) -> RowId {
        self.metrics.min_row_id
    }

    pub fn max_row_id(&self) -> RowId {
        self.metrics.max_row_id
    }

    pub fn min_dim_id(&self) -> DimId {
        self.metrics.min_dim_id
    }

    pub fn max_dim_id(&self) -> DimId {
        self.metrics.max_dim_id
    }
}

// TODO 重构 add 函数
/// Operation
impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexCompressedRamBuilder<OW, TW> {
    /// ## brief
    /// add one row into inverted_index_ram
    /// ## return
    /// bool: `true` if operation is `insert`, otherwise is `update`
    pub fn add(&mut self, row_id: RowId, vector: SparseVector) -> bool {
        let mut is_insert_operation = true;
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            // resize postings.
            if dim_id >= self.posting_builders.len() {
                self.posting_builders.resize_with(dim_id + 1, || {
                    CompressedPostingBuilder::new()
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
            self.metrics.compare_and_update_dim_id(dim_id as DimId);
        }
        // update metrics
        if is_insert_operation {
            self.metrics.increase_vector_count();
        }

        self.metrics.compare_and_update_row_id(row_id);

        is_insert_operation
    }

    /// Consumes the builder and returns an InvertedIndexRam
    pub fn build(self) -> CompressedInvertedIndexRam<TW> {
        let posting_list: Vec<CompressedPostingList<TW>> = self
            .posting_builders
            .into_iter()
            .map(|builder| builder.build())
            .collect();
        CompressedInvertedIndexRam {
            postings: posting_list,
            metrics: self.metrics,
        }
    }
}
