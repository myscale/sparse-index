use crate::core::InvertedIndexEnum;

use super::config::*;

pub fn parse_index_type(config: SparseIndexConfig) -> Result<InvertedIndexEnum, String> {
    let (index_type, storage_type, compressed) =
        (config.index_type, config.datatype, config.compressed);
    match (index_type, storage_type, compressed) {
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32, false) => {
            Ok(InvertedIndexEnum::InvertedIndexImmutableRam)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float32, false) => {
            Ok(InvertedIndexEnum::InvertedIndexMmap)
        }

        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32, true) => {
            Ok(InvertedIndexEnum::InvertedIndexCompressedImmutableRamF32)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float16, true) => {
            Ok(InvertedIndexEnum::InvertedIndexCompressedImmutableRamF16)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::UInt8, true) => {
            Ok(InvertedIndexEnum::InvertedIndexCompressedImmutableRamU8)
        }

        (SparseIndexType::Mmap, VectorStorageDatatype::Float32, true) => {
            Ok(InvertedIndexEnum::InvertedIndexCompressedMmapF32)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float16, true) => {
            Ok(InvertedIndexEnum::InvertedIndexCompressedMmapF16)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::UInt8, true) => {
            Ok(InvertedIndexEnum::InvertedIndexCompressedMmapU8)
        }
        _ => Err("config not supported".to_string()),
    }
}

