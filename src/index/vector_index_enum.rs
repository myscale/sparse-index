// use std::path::PathBuf;
// use half::f16;
// use crate::common::ScoredPointOffset;
// use crate::core::common::QuantizedU8;
// use crate::core::inverted_index::{InvertedIndexCompressedImmutableRam, InvertedIndexCompressedMmap, InvertedIndexImmutableRam, InvertedIndexMmap, InvertedIndexRam};
// use crate::core::sparse_vector::SparseVector;
// use crate::index::sparse_vector_index::SparseVectorIndex;
//
// /// Trait for vector searching
// pub trait VectorIndex {
//     /// Return list of Ids with fitting
//     fn search(
//         &self,
//         vector: &SparseVector,
//         filter: Option<&Vec<u8>>,
//         top: usize,
//         // params: Option<&SearchParams>,
//         // query_context: &VectorQueryContext,
//     ) -> Result<Vec<ScoredPointOffset>, String>;
//
//     fn files(&self) -> Vec<PathBuf>;
//
//     /// The number of indexed vectors, currently accessible
//     fn indexed_vector_count(&self) -> usize;
// }


// #[derive(Debug)]
// pub enum VectorIndexEnum {
//     SparseRam(SparseVectorIndex<InvertedIndexRam>),
//     SparseImmutableRam(SparseVectorIndex<InvertedIndexImmutableRam>),
//     SparseMmap(SparseVectorIndex<InvertedIndexMmap>),
//
//     SparseCompressedImmutableRamF32(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f32>>),
//     SparseCompressedImmutableRamF16(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f16>>),
//     SparseCompressedImmutableRamU8(SparseVectorIndex<InvertedIndexCompressedImmutableRam<QuantizedU8>>),
//
//     SparseCompressedMmapF32(SparseVectorIndex<InvertedIndexCompressedMmap<f32>>),
//     SparseCompressedMmapF16(SparseVectorIndex<InvertedIndexCompressedMmap<f16>>),
//     SparseCompressedMmapU8(SparseVectorIndex<InvertedIndexCompressedMmap<QuantizedU8>>),
// }

// impl VectorIndex for VectorIndexEnum{
//     fn search(&self, vector: &SparseVector, filter: Option<&Vec<u8>>, top: usize) -> Result<Vec<ScoredPointOffset>, String> {
//         match self {
//             VectorIndexEnum::SparseRam(index) => {
//                 index.search(vector, filter, top)
//             }
//             VectorIndexEnum::SparseImmutableRam(index) => {
//                 index.search(vector, filter, top)
//             }
//             VectorIndexEnum::SparseMmap(index) => {
//                 index.search(vector, filter, top)
//             }
//
//
//             VectorIndexEnum::SparseCompressedImmutableRamF32(index) => {
//                 index.search(vector, filter, top)
//             }
//             VectorIndexEnum::SparseCompressedImmutableRamF16(index) => {
//                 index.search(vector, filter, top)
//             }
//             VectorIndexEnum::SparseCompressedImmutableRamU8(index) => {
//                 index.search(vector, filter, top)
//             }
//
//
//             VectorIndexEnum::SparseCompressedMmapF32(index) => {
//                 index.search(vector, filter, top)
//             }
//             VectorIndexEnum::SparseCompressedMmapF16(index) => {
//                 index.search(vector, filter, top)
//             }
//             VectorIndexEnum::SparseCompressedMmapU8(index) => {
//                 index.search(vector, filter, top)
//             }
//         }
//     }
//
//     fn files(&self) -> Vec<PathBuf> {
//         todo!()
//     }
//
//     fn indexed_vector_count(&self) -> usize {
//         todo!()
//     }
// }