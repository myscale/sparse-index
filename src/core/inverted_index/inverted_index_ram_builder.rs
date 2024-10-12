use crate::core::common::types::ElementOffsetType;
use crate::core::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::core::posting_list::PostingListBuilder;
use crate::core::sparse_vector::SparseVector;
use crate::RowId;
use std::cmp::{max, min};
use std::mem::size_of;

/// Builder for InvertedIndexRam
pub struct InvertedIndexBuilder {
    pub posting_builders: Vec<PostingListBuilder>,
    pub vector_count: usize,
    pub memory: usize,
    pub min_row_id: RowId,
    pub max_row_id: RowId,
}

impl Default for InvertedIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndexBuilder {
    pub fn new() -> InvertedIndexBuilder {
        InvertedIndexBuilder {
            posting_builders: Vec::new(),
            vector_count: 0,
            memory: 0,
            min_row_id: RowId::MAX,
            max_row_id: RowId::MIN,
        }
    }

    /// Returns the total memory usage of the InvertedIndexBuilder in bytes.
    pub fn memory_usage(&self) -> usize {
        self.memory
    }

    pub fn add(&mut self, id: ElementOffsetType, vector: SparseVector) {
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            if dim_id >= self.posting_builders.len() {
                self.posting_builders.resize_with(dim_id + 1, PostingListBuilder::new);
                self.memory += (dim_id + 1 - self.posting_builders.len()) * size_of::<PostingListBuilder>();
            }
            let memory_before = self.posting_builders[dim_id].memory_usage();
            self.posting_builders[dim_id].add(id, weight);
            let memory_after = self.posting_builders[dim_id].memory_usage();
            self.memory += memory_after - memory_before;
        }
        self.vector_count += 1;
        self.min_row_id = min(self.min_row_id, id);
        self.max_row_id = max(self.max_row_id, id);
    }

    /// Consumes the builder and returns an InvertedIndexRam
    pub fn build(self) -> InvertedIndexRam {
        let mut postings = Vec::with_capacity(self.posting_builders.len());
        for posting_builder in self.posting_builders {
            postings.push(posting_builder.build());
        }

        let vector_count = self.vector_count;
        InvertedIndexRam {
            postings,
            vector_count,
            min_row_id: self.min_row_id,
            max_row_id: self.max_row_id
        }
    }

    // Creates an [InvertedIndexRam] from an iterator of (id, vector) pairs.
    // pub fn build_from_iterator(
    //     iter: impl Iterator<Item = (ElementOffsetType, SparseVector)>,
    // ) -> InvertedIndexRam {
    //     let mut builder = InvertedIndexBuilder::new();
    //     for (id, vector) in iter {
    //         builder.add(id, vector);
    //     }
    //     builder.build()
    // }
}
