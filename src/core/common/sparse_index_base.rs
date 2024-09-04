use half::f16;

// #[derive(Debug)]
// pub enum SparseIndexEnum {
//     SparseRam(SparseVectorIndex<InvertedIndexRam>),
//     SparseImmutableRam(SparseVectorIndex<InvertedIndexImmutableRam>),
//     SparseMmap(SparseVectorIndex<InvertedIndexMmap>),
//     SparseCompressedImmutableRamF32(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f32>>),
//     SparseCompressedImmutableRamF16(SparseVectorIndex<InvertedIndexCompressedImmutableRam<f16>>),
//     SparseCompressedImmutableRamU8(
//         SparseVectorIndex<InvertedIndexCompressedImmutableRam<QuantizedU8>>,
//     ),
//     SparseCompressedMmapF32(SparseVectorIndex<InvertedIndexCompressedMmap<f32>>),
//     SparseCompressedMmapF16(SparseVectorIndex<InvertedIndexCompressedMmap<f16>>),
//     SparseCompressedMmapU8(SparseVectorIndex<InvertedIndexCompressedMmap<QuantizedU8>>),
// }