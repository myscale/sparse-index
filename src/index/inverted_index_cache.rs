use std::sync::{Arc, RwLock, RwLockWriteGuard};
use flurry::HashMap;
use half::f16;
use once_cell::sync::Lazy;
use crate::core::common::{QuantizedU8, StorageVersion};
use crate::core::inverted_index::{InvertedIndex, InvertedIndexBuilder, InvertedIndexCompressedImmutableRam, InvertedIndexCompressedMmap, InvertedIndexImmutableRam, InvertedIndexMmap, InvertedIndexRam};
use crate::core::posting_list::PostingListIter;
use crate::index::sparse_index_config::{SparseIndexConfig, SparseIndexType, VectorStorageDatatype};
// use crate::index::sparse_vector_index::SparseVectorIndex;
// trait ClonePostingListIter: Clone + PostingListIter {}
// impl<T: Clone + PostingListIter + 'static> ClonePostingListIter for T {}


// #[derive(Debug)]
// pub enum InvertedIndexEnum {
//     // InvertedIndexRam(InvertedIndexRam),
//
//     // Not compressed
//     InvertedIndexImmutableRam(InvertedIndexImmutableRam),
//
//     InvertedIndexMmap(InvertedIndexMmap),
//
//     // Compressed
//     InvertedIndexCompressedImmutableRamF32(InvertedIndexCompressedImmutableRam<f32>),
//     InvertedIndexCompressedImmutableRamF16(InvertedIndexCompressedImmutableRam<f16>),
//     InvertedIndexCompressedImmutableRamU8(InvertedIndexCompressedImmutableRam<QuantizedU8>),
//
//     InvertedIndexCompressedMmapF32(InvertedIndexCompressedMmap<f32>),
//     InvertedIndexCompressedMmapF16(InvertedIndexCompressedMmap<f16>),
//     InvertedIndexCompressedMmapU8(InvertedIndexCompressedMmap<QuantizedU8>),
// }

#[derive(Debug)]
pub enum InvertedIndexEnum {
    // Not compressed
    InvertedIndexImmutableRam,
    InvertedIndexMmap,

    // Compressed
    InvertedIndexCompressedImmutableRamF32,
    InvertedIndexCompressedImmutableRamF16,
    InvertedIndexCompressedImmutableRamU8,

    InvertedIndexCompressedMmapF32,
    InvertedIndexCompressedMmapF16,
    InvertedIndexCompressedMmapU8,
}

pub fn parse_index_type(config: SparseIndexConfig) -> Result<InvertedIndexEnum, String> {
    let (index_type, storage_type, compressed) = (config.index_type, config.datatype, config.compressed);
    match (index_type, storage_type, compressed) {
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32, false) => Ok(InvertedIndexEnum::InvertedIndexImmutableRam),
        (SparseIndexType::Mmap, VectorStorageDatatype::Float32, false) => Ok(InvertedIndexEnum::InvertedIndexMmap),

        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32, true) => Ok(InvertedIndexEnum::InvertedIndexCompressedImmutableRamF32),
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float16, true) => Ok(InvertedIndexEnum::InvertedIndexCompressedImmutableRamF16),
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::UInt8, true) => Ok(InvertedIndexEnum::InvertedIndexCompressedImmutableRamU8),

        (SparseIndexType::Mmap, VectorStorageDatatype::Float32, true) => Ok(InvertedIndexEnum::InvertedIndexCompressedMmapF32),
        (SparseIndexType::Mmap, VectorStorageDatatype::Float16, true) => Ok(InvertedIndexEnum::InvertedIndexCompressedMmapF16),
        (SparseIndexType::Mmap, VectorStorageDatatype::UInt8, true) => Ok(InvertedIndexEnum::InvertedIndexCompressedMmapU8),
        _ => Err("config not supported".to_string())
    }
}

pub static INVERTED_INDEX_IMMUTABLE_RAM_CACHE: Lazy<HashMap<String, Arc<InvertedIndexImmutableRam>>> = Lazy::new(||HashMap::new());
pub static INVERTED_INDEX_MMAP_CACHE: Lazy<HashMap<String, Arc<InvertedIndexMmap>>> = Lazy::new(||HashMap::new());

pub static INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F32_CACHE: Lazy<HashMap<String, Arc<InvertedIndexCompressedImmutableRam::<f32>>>> = Lazy::new(||HashMap::new());
pub static INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F16_CACHE: Lazy<HashMap<String, Arc<InvertedIndexCompressedImmutableRam::<f16>>>> = Lazy::new(||HashMap::new());
pub static INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_U8_CACHE: Lazy<HashMap<String, Arc<InvertedIndexCompressedImmutableRam::<QuantizedU8>>>> = Lazy::new(||HashMap::new());

pub static INVERTED_INDEX_COMPRESSED_MMAP_F32_CACHE: Lazy<HashMap<String, Arc<InvertedIndexCompressedMmap::<f32>>>> = Lazy::new(||HashMap::new());
pub static INVERTED_INDEX_COMPRESSED_MMAP_F16_CACHE: Lazy<HashMap<String, Arc<InvertedIndexCompressedMmap::<f16>>>> = Lazy::new(||HashMap::new());
pub static INVERTED_INDEX_COMPRESSED_MMAP_U8_CACHE: Lazy<HashMap<String, Arc<InvertedIndexCompressedMmap::<QuantizedU8>>>> = Lazy::new(||HashMap::new());


//
// pub struct InvertedIndexCache {
//     cache: HashMap<String, Arc<InvertedIndexImpl>>,
// }
//
// impl From<InvertedIndexRam> for InvertedIndexImpl {
//     fn from(value: InvertedIndexRam) -> Self {
//         InvertedIndexImpl::InvertedIndexRam(value)
//     }
// }
//
// // pub struct InvertedIndexCache {
// //     cache: HashMap<String, Arc<InvertedIndexImpl>>,
// // }
//
// impl InvertedIndexCache {
//     pub fn new() -> Self {
//         Self { cache: HashMap::new() }
//     }
//
//     pub fn insert(
//         &self,
//         index_path: &str,
//         inverted_index_impl: InvertedIndexImpl
//     ) -> Result<(), String>
//     {
//         let trimmed_path = index_path.trim_end_matches("/").to_string();
//         let pinned = self.cache.pin();
//         if pinned.contains_key(&trimmed_path) {
//             pinned.insert(trimmed_path.clone(), Arc::new(inverted_index_impl));
//             println!("[Warning] {} already exists, rewrite it.", trimmed_path);
//         } else {
//             pinned.insert(trimmed_path, Arc::new(inverted_index_impl));
//         }
//         Ok(())
//     }
//
//     pub fn get(
//         &self,
//         index_path: &str
//     ) -> Result<Arc<InvertedIndexImpl>, String>
//     {
//         let trimmed_path = index_path.trim_end_matches("/").to_string();
//         let pinned = self.cache.pin();
//         match pinned.get(&trimmed_path) {
//             None => {
//                 Err(format!("can't get, inverted index not exists, index_path is {}", trimmed_path))
//             }
//             Some(res) => {
//                 Ok(res.clone())
//             }
//         }
//     }
//
//     pub fn remove(&self, index_path: &str) -> Result<(), String>
//     {
//         let trimmed_path = index_path.trim_end_matches("/").to_string();
//         let pinned = self.cache.pin();
//         if pinned.contains_key(&trimmed_path) {
//             pinned.remove(&trimmed_path);
//         } else {
//             return Err(format!("can't remove, inverted index not exists, index_path is {}", trimmed_path));
//         }
//         Ok(())
//     }
// }