use std::{borrow::Cow, path::PathBuf};

use log::error;

use crate::core::{
    CompressedInvertedIndexMmap, IndexWeightType, InvertedIndexMmap, InvertedIndexMmapAccess,
    InvertedIndexMmapInit, InvertedIndexRam, StorageType,
};

pub enum GenericInvertedIndexRam {
    F32RamIndex(InvertedIndexRam<f32>),
    F16RamIndex(InvertedIndexRam<half::f16>),
    U8RamIndex(InvertedIndexRam<u8>),
}

impl GenericInvertedIndexRam {
    #[rustfmt::skip]
    pub fn save_to_mmap(
        self,
        storage_type: StorageType,
        weight_type: IndexWeightType,
        need_quantized: bool,
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Vec<PathBuf>> {
        match (storage_type, weight_type, need_quantized, self) {
            (StorageType::Mmap, IndexWeightType::Float32, true, GenericInvertedIndexRam::U8RamIndex(e)) => Ok(InvertedIndexMmap::<f32, u8>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::Mmap, IndexWeightType::Float32, false, GenericInvertedIndexRam::F32RamIndex(e)) => Ok(InvertedIndexMmap::<f32, f32>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::Mmap, IndexWeightType::Float16, true, GenericInvertedIndexRam::U8RamIndex(e)) => Ok(InvertedIndexMmap::<half::f16, u8>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::Mmap, IndexWeightType::Float16, false, GenericInvertedIndexRam::F16RamIndex(e)) => Ok(InvertedIndexMmap::<half::f16, half::f16>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::Mmap, IndexWeightType::UInt8, false, GenericInvertedIndexRam::U8RamIndex(e)) => Ok(InvertedIndexMmap::<u8, u8>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::CompressedMmap, IndexWeightType::Float32, true, GenericInvertedIndexRam::U8RamIndex(e)) => Ok(CompressedInvertedIndexMmap::<f32, u8>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::CompressedMmap, IndexWeightType::Float32, false, GenericInvertedIndexRam::F32RamIndex(e)) => Ok(CompressedInvertedIndexMmap::<f32, f32>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::CompressedMmap, IndexWeightType::Float16, true, GenericInvertedIndexRam::U8RamIndex(e)) => Ok(CompressedInvertedIndexMmap::<half::f16, u8>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::CompressedMmap, IndexWeightType::Float16, false, GenericInvertedIndexRam::F16RamIndex(e)) => Ok(CompressedInvertedIndexMmap::<half::f16, half::f16>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (StorageType::CompressedMmap, IndexWeightType::UInt8, false, GenericInvertedIndexRam::U8RamIndex(e)) => Ok(CompressedInvertedIndexMmap::<u8, u8>::from_ram_index(Cow::Owned(e), directory.to_path_buf(), segment_id)?.files(segment_id)),
            (_, _, _, _) => {
                let error_msg = format!("Invalid parameter when save from GenericInvertedIndexRam, storage_type:{:?}, weight_type:{:?}, need_quantized:{:?}", storage_type, weight_type, need_quantized);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
    }
}
