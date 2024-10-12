#[derive(Debug)]
pub enum InvertedIndexEnum {
    // Not compressed
    InvertedIndexImmutableRam,
    InvertedIndexMmap,

    // Compressed
    // InvertedIndexCompressedImmutableRamF32,
    // InvertedIndexCompressedImmutableRamF16,
    // InvertedIndexCompressedImmutableRamU8,

    // InvertedIndexCompressedMmapF32,
    // InvertedIndexCompressedMmapF16,
    // InvertedIndexCompressedMmapU8,
}
