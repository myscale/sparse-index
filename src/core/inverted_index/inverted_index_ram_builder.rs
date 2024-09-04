use crate::core::common::types::ElementOffsetType;
use crate::core::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::core::posting_list::PostingListBuilder;
use crate::core::sparse_vector::RemappedSparseVector;
use std::cmp::max;

/// Builder for InvertedIndexRam
pub struct InvertedIndexBuilder {
    pub posting_builders: Vec<PostingListBuilder>,
    pub vector_count: usize,
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
        }
    }

    /// Add a vect        builder.add(row_id, sparse_vector.into());or to the inverted core builder
    pub fn add(&mut self, id: ElementOffsetType, vector: RemappedSparseVector) {
        for (dim_id, weight) in vector.indices.into_iter().zip(vector.values.into_iter()) {
            let dim_id = dim_id as usize;
            self.posting_builders.resize_with(
                max(dim_id + 1, self.posting_builders.len()),
                PostingListBuilder::new,
            );
            self.posting_builders[dim_id].add(id, weight);
        }
        self.vector_count += 1;
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
        }
    }

    /// Creates an [InvertedIndexRam] from an iterator of (id, vector) pairs.
    pub fn build_from_iterator(
        iter: impl Iterator<Item = (ElementOffsetType, RemappedSparseVector)>,
    ) -> InvertedIndexRam {
        let mut builder = InvertedIndexBuilder::new();
        for (id, vector) in iter {
            builder.add(id, vector);
        }
        builder.build()
    }
}
