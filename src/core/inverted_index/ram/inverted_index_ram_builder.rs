use super::InvertedIndexRam;
use crate::core::sparse_vector::SparseVector;
use crate::core::{posting_list::PostingListBuilder, QuantizedWeight};
use crate::core::{
    DimId, InvertedIndexMetrics, InvertedIndexRamBuilderTrait, PostingList, QuantizedParam,
};
use crate::RowId;

/// Builder for InvertedIndexRam
/// OW: RamBuilder 在构建过程中，逐个 add vector 的时候，使用到的 weight 数据类型
/// TW: RamBuilder 在执行 build 时，需要逐个的量化 Posting，每个 Posting 在量化之后会产生 1 个 u8 Posting，这就是量化后的数据类型，也可能没有用到量化
pub struct InvertedIndexRamBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    posting_builders: Vec<PostingListBuilder<OW, TW>>,
    memory: usize,
    metrics: InvertedIndexMetrics,

    propagate_while_upserting: bool,
    finally_sort: bool,
    finally_propagate: bool,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> Default for InvertedIndexRamBuilder<OW, TW> {
    fn default() -> Self {
        Self::new()
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexRamBuilder<OW, TW> {
    /// ## brief
    /// create an InvertedIndexRamBuilder
    /// ## config
    /// - `propagate_while_upserting`: false
    /// - `finally_sort`: false
    /// - `finally_progagate`: true
    pub fn new() -> InvertedIndexRamBuilder<OW, TW> {
        InvertedIndexRamBuilder {
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

    /// Returns the total memory usage of the InvertedIndexRamBuilder in bytes.

    pub fn metrics(&self) -> &InvertedIndexMetrics {
        &self.metrics
    }
}

/// Operation
impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexRamBuilderTrait<TW>
    for InvertedIndexRamBuilder<OW, TW>
{
    fn memory_usage(&self) -> usize {
        self.memory
    }

    /// ## brief
    /// add one row into inverted_index_ram
    /// ## return
    /// bool: `true` if operation is `insert`, otherwise is `update`
    fn add(&mut self, row_id: RowId, vector: SparseVector) -> bool {
        let mut is_insert_operation = true;
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            // resize postings.
            if dim_id >= self.posting_builders.len() {
                self.posting_builders.resize_with(dim_id + 1, || {
                    PostingListBuilder::<OW, TW>::new()
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
    fn build(self) -> InvertedIndexRam<TW> {
        let (postings, quantized_params): (Vec<PostingList<TW>>, Vec<Option<QuantizedParam>>) =
            self.posting_builders
                .into_iter()
                .map(|builder| builder.build())
                .unzip();

        InvertedIndexRam::<TW> {
            postings,
            quantized_params,
            metrics: self.metrics,
        }
    }
}
