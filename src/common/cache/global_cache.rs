use std::sync::Arc;

use flurry::HashMap;
use half::f16;
use once_cell::sync::Lazy;

use crate::core::{InvertedIndexImmutableRam, InvertedIndexMmap, QuantizedU8};

use super::SparseIndexRamBuilderCache;

pub static RAM_BUILDER_CACHE: Lazy<SparseIndexRamBuilderCache> =
    Lazy::new(|| SparseIndexRamBuilderCache::new());

pub static INVERTED_INDEX_IMMUTABLE_RAM_CACHE: Lazy<
    HashMap<String, Arc<InvertedIndexImmutableRam>>,
> = Lazy::new(|| HashMap::new());

pub static INVERTED_INDEX_MMAP_CACHE: Lazy<HashMap<String, Arc<InvertedIndexMmap>>> =
    Lazy::new(|| HashMap::new());

// pub static INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F32_CACHE: Lazy<
//     HashMap<String, Arc<InvertedIndexCompressedImmutableRam<f32>>>,
// > = Lazy::new(|| HashMap::new());

// pub static INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F16_CACHE: Lazy<
//     HashMap<String, Arc<InvertedIndexCompressedImmutableRam<f16>>>,
// > = Lazy::new(|| HashMap::new());

// pub static INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_U8_CACHE: Lazy<
//     HashMap<String, Arc<InvertedIndexCompressedImmutableRam<QuantizedU8>>>,
// > = Lazy::new(|| HashMap::new());

// pub static INVERTED_INDEX_COMPRESSED_MMAP_F32_CACHE: Lazy<
//     HashMap<String, Arc<InvertedIndexCompressedMmap<f32>>>,
// > = Lazy::new(|| HashMap::new());

// pub static INVERTED_INDEX_COMPRESSED_MMAP_F16_CACHE: Lazy<
//     HashMap<String, Arc<InvertedIndexCompressedMmap<f16>>>,
// > = Lazy::new(|| HashMap::new());

// pub static INVERTED_INDEX_COMPRESSED_MMAP_U8_CACHE: Lazy<
//     HashMap<String, Arc<InvertedIndexCompressedMmap<QuantizedU8>>>,
// > = Lazy::new(|| HashMap::new());
