use typed_builder::TypedBuilder;

use super::InvertedIndexRam;
use crate::core::inverted_index::common::InvertedIndexMetrics;
use crate::core::sparse_vector::SparseVector;
use crate::core::{posting_list::PostingListBuilder, QuantizedWeight};
use crate::core::{DimId, ElementType, InvertedIndexRamBuilderTrait, PostingList, QuantizedParam, WeightType};
use crate::RowId;

#[derive(TypedBuilder)]
pub struct InvertedIndexRamBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    #[builder(default=vec![])]
    posting_builders: Vec<PostingListBuilder<OW, TW>>,

    #[builder(default=ElementType::SIMPLE)]
    element_type: ElementType,

    #[builder(default = 0)]
    memory_consumed: usize,

    #[builder(default=InvertedIndexMetrics::default())]
    metrics: InvertedIndexMetrics,

    #[builder(default = false)]
    propagate_while_upserting: bool,

    #[builder(default = false)]
    finally_sort: bool,

    #[builder(default = false)]
    finally_propagate: bool,
}

/// Operation
impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexRamBuilderTrait<TW> for InvertedIndexRamBuilder<OW, TW> {
    fn new(element_type: ElementType) -> InvertedIndexRamBuilder<OW, TW> {
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
                // TODO: 优化错误传递， finally sort 也可以忽略掉
                self.posting_builders.resize_with(dim_id + 1, || PostingListBuilder::<OW, TW>::new(self.element_type, self.propagate_while_upserting).expect(""));
            }
            // insert new sparse_vector into postings.
            let memory_before = self.posting_builders[dim_id].memory_usage().0;
            let operation = self.posting_builders[dim_id].add(row_id, weight);
            is_insert_operation &= operation;
            let memory_after = self.posting_builders[dim_id].memory_usage().0;

            self.memory_consumed = self.memory_consumed.saturating_add(memory_after - memory_before);
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
        // TODO: 优化错误传递
        let (postings, quantized_params): (Vec<PostingList<TW>>, Vec<Option<QuantizedParam>>) = self.posting_builders.into_iter().map(|builder| builder.build().expect("")).unzip();

        let need_quantized = TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if !need_quantized {
            assert_eq!(TW::weight_type(), OW::weight_type());
        }
        InvertedIndexRam::<TW> { postings, quantized_params, metrics: self.metrics, element_type: self.element_type, need_quantized }
    }
}
