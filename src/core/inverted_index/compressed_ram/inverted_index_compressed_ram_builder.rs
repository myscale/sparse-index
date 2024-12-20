use typed_builder::TypedBuilder;

use crate::core::inverted_index::common::InvertedIndexMetrics;
use crate::core::sparse_vector::SparseVector;
use crate::core::CompressedPostingBuilder;
use crate::core::CompressedPostingList;
use crate::core::DimId;
use crate::core::ElementType;
use crate::core::InvertedIndexRamBuilder;
use crate::core::InvertedIndexRamBuilderTrait;
use crate::core::QuantizedWeight;
use crate::RowId;

use super::CompressedInvertedIndexRam;

/// Builder for InvertedIndexRam
#[derive(TypedBuilder)]
pub struct InvertedIndexCompressedRamBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    #[builder(default=vec![])]
    posting_builders: Vec<CompressedPostingBuilder<OW, TW>>,

    #[builder(default=ElementType::SIMPLE)]
    element_type: ElementType,
    
    #[builder(default=0)]
    memory_consumed: usize,

    #[builder(default=InvertedIndexMetrics::default())]
    metrics: InvertedIndexMetrics,

    #[builder(default=false)]
    propagate_while_upserting: bool,

    #[builder(default=false)]
    finally_sort: bool,

    #[builder(default=false)]
    finally_propagate: bool,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight>  InvertedIndexRamBuilderTrait<TW> for InvertedIndexCompressedRamBuilder<OW, TW> {
    fn new(element_type: ElementType) -> Self {
        InvertedIndexRamBuilder::builder()
            .posting_builders(vec![])
            .element_type(element_type)
            .memory_consumed(0)
            .metrics(InvertedIndexMetrics::default())
            .propagate_while_upserting(false)
            .finally_sort(false)
            .finally_propagate(element_type == ElementType::EXTENDED)
            .build()
    }

    fn memory_usage(&self) -> usize {
        self.memory_consumed
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
                    CompressedPostingBuilder::<OW, TW>::new(self.element_type, self.finally_sort, self.propagate_while_upserting)
                });
            }
            // insert new sparse_vector into postings.
            let memory_before = self.posting_builders[dim_id].memory_usage().0;
            let operation = self.posting_builders[dim_id].add(row_id, weight);
            is_insert_operation &= operation;
            let memory_after = self.posting_builders[dim_id].memory_usage().0;

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
    fn build(self) -> CompressedInvertedIndexRam<TW> {
        let posting_list: Vec<CompressedPostingList<TW>> =
            self.posting_builders.into_iter().map(|builder| builder.build()).collect();
        CompressedInvertedIndexRam { postings: posting_list, metrics: self.metrics, element_type:  self.element_type}
    }


}
