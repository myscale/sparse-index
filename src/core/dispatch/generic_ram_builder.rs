use std::path::PathBuf;

use log::error;

use crate::{
    core::{ElementType, InvertedIndexRamBuilder, InvertedIndexRamBuilderTrait, SparseVector, IndexWeightType, StorageType},
    RowId,
};

use super::GenericInvertedIndexRam;

pub enum GenericInvertedIndexRamBuilder {
    F32NoQuantized(InvertedIndexRamBuilder<f32, f32>),
    F32Quantized(InvertedIndexRamBuilder<f32, u8>),
    F16NoQuantized(InvertedIndexRamBuilder<half::f16, half::f16>),
    F16Quantized(InvertedIndexRamBuilder<half::f16, u8>),
    U8NoQuantized(InvertedIndexRamBuilder<u8, u8>),
}

impl GenericInvertedIndexRamBuilder {
    #[rustfmt::skip]
    pub fn new(weight_type: IndexWeightType, need_quantized: bool, element_type: ElementType) -> Self {
        match (weight_type, need_quantized) {
            (IndexWeightType::Float32, true) => Self::F32Quantized(InvertedIndexRamBuilder::<f32, u8>::new(ElementType::SIMPLE)),
            (IndexWeightType::Float32, false) => Self::F32NoQuantized(InvertedIndexRamBuilder::<f32, f32>::new(element_type)),
            (IndexWeightType::Float16, true) => Self::F16Quantized(InvertedIndexRamBuilder::<half::f16, u8>::new(ElementType::SIMPLE)),
            (IndexWeightType::Float16, false) => Self::F16NoQuantized(InvertedIndexRamBuilder::<half::f16, half::f16>::new(element_type)),
            (IndexWeightType::UInt8, false) => Self::U8NoQuantized(InvertedIndexRamBuilder::<u8, u8>::new(element_type)),
            (_, _) => {
                let error_msg = format!("Invalid parameter when create GenericInvertedIndexRamBuilder, weight_type:{:?}, need_quantized:{}", weight_type, need_quantized);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }

    }

    #[rustfmt::skip]
    pub fn add(&mut self, row_id: RowId, vector: SparseVector) -> bool {
        match self {
            GenericInvertedIndexRamBuilder::F32NoQuantized(e) => e.add(row_id, vector),
            GenericInvertedIndexRamBuilder::F32Quantized(e) => e.add(row_id, vector),
            GenericInvertedIndexRamBuilder::F16NoQuantized(e) => e.add(row_id, vector),
            GenericInvertedIndexRamBuilder::F16Quantized(e) => e.add(row_id, vector),
            GenericInvertedIndexRamBuilder::U8NoQuantized(e) => e.add(row_id, vector),
        }
    }


    #[rustfmt::skip]
    fn build_ram_index(self) -> GenericInvertedIndexRam {
        match self {
            GenericInvertedIndexRamBuilder::F32NoQuantized(e) => GenericInvertedIndexRam::F32RamIndex(e.build()),
            GenericInvertedIndexRamBuilder::F32Quantized(e) => GenericInvertedIndexRam::U8RamIndex(e.build()),
            GenericInvertedIndexRamBuilder::F16NoQuantized(e) => GenericInvertedIndexRam::F16RamIndex(e.build()),
            GenericInvertedIndexRamBuilder::F16Quantized(e) => GenericInvertedIndexRam::U8RamIndex(e.build()),
            GenericInvertedIndexRamBuilder::U8NoQuantized(e) => GenericInvertedIndexRam::U8RamIndex(e.build()),
        }
    }

    #[rustfmt::skip]
    pub fn memory_usage(&self) -> usize {
        match self {
            GenericInvertedIndexRamBuilder::F32NoQuantized(e) => e.memory_usage(),
            GenericInvertedIndexRamBuilder::F32Quantized(e) => e.memory_usage(),
            GenericInvertedIndexRamBuilder::F16NoQuantized(e) => e.memory_usage(),
            GenericInvertedIndexRamBuilder::F16Quantized(e) => e.memory_usage(),
            GenericInvertedIndexRamBuilder::U8NoQuantized(e) => e.memory_usage(),
        }
    }

    #[rustfmt::skip]
    pub fn build_and_flush(
        self,
        storage_type: StorageType,
        weight_type: IndexWeightType,
        need_quantized: bool,
        directory: &PathBuf,
        segment_id: Option<&str>
    ) -> crate::Result<Vec<PathBuf>> {
        match (storage_type, weight_type, need_quantized) {
            (StorageType::Mmap, IndexWeightType::Float32, true) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::Mmap, IndexWeightType::Float32, false) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::Mmap, IndexWeightType::Float16, true) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::Mmap, IndexWeightType::Float16, false) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::Mmap, IndexWeightType::UInt8, false) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::CompressedMmap, IndexWeightType::Float32, true) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::CompressedMmap, IndexWeightType::Float32, false) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::CompressedMmap, IndexWeightType::Float16, true) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::CompressedMmap, IndexWeightType::Float16, false) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::CompressedMmap, IndexWeightType::UInt8, true) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (StorageType::CompressedMmap, IndexWeightType::UInt8, false) => self.build_ram_index().save_to_mmap(storage_type, weight_type, need_quantized, directory, segment_id),
            (_, _, _) => {
                let error_msg = format!("Invalid parameter when flush index to disk. storage_type:{:?}, weight_type:{:?}, need_quantized:{}", storage_type, weight_type, need_quantized);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
    }
}
