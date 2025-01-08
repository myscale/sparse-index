use log::error;
use typed_builder::TypedBuilder;

use super::InvertedIndexRam;
use crate::core::inverted_index::common::InvertedIndexMetrics;
use crate::core::sparse_vector::SparseVector;
use crate::core::{posting_list::PostingListBuilder, QuantizedWeight};
use crate::core::{DimId, ElementType, InvertedIndexError, InvertedIndexRamBuilderTrait, WeightType};
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
            .build()
    }

    fn memory_usage(&self) -> Result<usize, InvertedIndexError> {
        Ok(self.memory_consumed)
    }

    /// ## brief
    /// add one row into inverted_index_ram
    /// ## return
    /// bool: `true` if operation is `insert`, otherwise is `update`
    fn add(&mut self, row_id: RowId, vector: SparseVector) -> Result<bool, InvertedIndexError> {
        let mut is_insert_operation = true;
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            // resize postings.
            if dim_id >= self.posting_builders.len() {
                let empty_builder = PostingListBuilder::<OW, TW>::new(self.element_type, self.propagate_while_upserting).map_err(|e| InvertedIndexError::from(e))?;
                self.posting_builders.resize_with(dim_id + 1, || empty_builder.clone());
            }
            // insert new sparse_vector into postings.
            let actual_memory_before = self.posting_builders[dim_id].memory_usage().0;
            let operation = self.posting_builders[dim_id].add(row_id, weight);
            is_insert_operation &= operation;
            let actual_memory_after = self.posting_builders[dim_id].memory_usage().0;

            self.memory_consumed = self.memory_consumed.saturating_add(actual_memory_after - actual_memory_before);
            self.metrics.compare_and_update_dim_id(dim_id as DimId);
        }
        // update metrics
        if is_insert_operation {
            self.metrics.increase_vector_count();
        }

        self.metrics.compare_and_update_row_id(row_id);

        Ok(is_insert_operation)
    }

    /// Consumes the builder and returns an InvertedIndexRam
    fn build(self) -> Result<InvertedIndexRam<TW>, InvertedIndexError> {
        let need_quantized = TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if !need_quantized && TW::weight_type() != OW::weight_type() {
            let error_msg = "[InvertedIndexRam] WeightType should keep same, while quantized is disabled.";
            error!("{}", error_msg);
            return Err(InvertedIndexError::InvalidParameter(error_msg.to_string()));
        }

        let mut postings = Vec::with_capacity(self.posting_builders.len());
        let mut quantized_params = Vec::with_capacity(self.posting_builders.len());

        for builder in self.posting_builders.into_iter() {
            let (posting, quantized_param) = builder.build().map_err(|e| InvertedIndexError::from(e))?;
            postings.push(posting);
            quantized_params.push(quantized_param);
        }

        Ok(InvertedIndexRam::<TW> { postings, quantized_params, metrics: self.metrics, element_type: self.element_type, need_quantized })
    }
}
