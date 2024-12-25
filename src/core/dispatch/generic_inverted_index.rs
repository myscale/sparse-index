use enum_dispatch::enum_dispatch;
use log::{debug, error, info};
use std::path::PathBuf;

use crate::core::{IndexWeightType, InvertedIndexMetrics, StorageType};
use crate::index::IndexSettings;
use crate::{
    common::errors::SparseError,
    core::{
        CompressedInvertedIndexMmap, CompressedInvertedIndexMmapMerger, DimId, ElementRead, ElementType, InvertedIndexMmap, InvertedIndexMmapAccess, InvertedIndexMmapInit,
        InvertedIndexMmapMerger, PostingListIter, PostingListIterAccess, QuantizedWeight,
    },
    RowId,
};

use super::{generic_posting_iterator::PostingListIteratorWrapper, GenericPostingListIterator};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum InvertedIndexWrapperType {
    Simple,
    Compressed,
}

#[derive(Debug, Clone)]
#[enum_dispatch(InvertedIndexMmapAccess<OW, TW>)]
pub enum InvertedIndexWrapper<OW: QuantizedWeight, TW: QuantizedWeight> {
    SimpleInvertedIndex(InvertedIndexMmap<OW, TW>),
    CompressedInvertedIndex(CompressedInvertedIndexMmap<OW, TW>),
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexWrapper<OW, TW> {
    pub fn type_id(&self) -> InvertedIndexWrapperType {
        match self {
            InvertedIndexWrapper::SimpleInvertedIndex(_) => InvertedIndexWrapperType::Simple,
            InvertedIndexWrapper::CompressedInvertedIndex(_) => InvertedIndexWrapperType::Compressed,
        }
    }

    pub(super) fn support_pruning(&self) -> bool {
        match self {
            InvertedIndexWrapper::SimpleInvertedIndex(e) => match e.meta.inverted_index_meta.element_type {
                crate::core::ElementType::SIMPLE => false,
                crate::core::ElementType::EXTENDED => true,
            },
            InvertedIndexWrapper::CompressedInvertedIndex(_) => false,
        }
    }

    #[rustfmt::skip]
    pub(super) fn get_posting_opt(
        &self,
        dim_id: DimId,
        min_row_id: &mut RowId,
        max_row_id: &mut RowId,
    ) -> Option<GenericPostingListIterator<'_>>
    {
        match self {
            InvertedIndexWrapper::SimpleInvertedIndex(e) => {
                if let Some(mut value) = e.iter(&dim_id) {
                    if let (Some(first), Some(last_id)) = (&value.peek(), value.last_id()) {
                        *min_row_id = std::cmp::min(*min_row_id, first.row_id());
                        *max_row_id = std::cmp::max(*max_row_id, last_id);
                    }
                    let wrapper: PostingListIteratorWrapper<'_, OW, TW> = value.into();

                    Some(GenericPostingListIterator::from(wrapper))
                } else {
                    None
                }
            },
            InvertedIndexWrapper::CompressedInvertedIndex(e) => {
                if let Some(mut value) = e.iter(&dim_id) {
                    if let (Some(first), Some(last_id)) = (&value.peek(), value.last_id()) {
                        *min_row_id = std::cmp::min(*min_row_id, first.row_id());
                        *max_row_id = std::cmp::max(*max_row_id, last_id);
                    }
                    let wrapper: PostingListIteratorWrapper<'_, OW, TW> = value.into();

                    Some(GenericPostingListIterator::from(wrapper))
                } else {
                    None
                }
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GenericInvertedIndexType {
    F32NoQuantized,
    F32Quantized,
    F16NoQuantized,
    F16Quantized,
    U8NoQuantized,
}

#[derive(Debug, Clone)]
pub enum GenericInvertedIndex {
    F32NoQuantized(InvertedIndexWrapper<f32, f32>),
    F32Quantized(InvertedIndexWrapper<f32, u8>),
    F16NoQuantized(InvertedIndexWrapper<half::f16, half::f16>),
    F16Quantized(InvertedIndexWrapper<half::f16, u8>),
    U8NoQuantized(InvertedIndexWrapper<u8, u8>),
}

impl GenericInvertedIndex {
    #[rustfmt::skip]
    pub fn type_id(&self) -> (GenericInvertedIndexType, InvertedIndexWrapperType) {
        match self {
            GenericInvertedIndex::F32NoQuantized(e) => (GenericInvertedIndexType::F32NoQuantized, e.type_id()),
            GenericInvertedIndex::F32Quantized(e) => (GenericInvertedIndexType::F32Quantized, e.type_id()),
            GenericInvertedIndex::F16NoQuantized(e) => (GenericInvertedIndexType::F16NoQuantized, e.type_id()),
            GenericInvertedIndex::F16Quantized(e) => (GenericInvertedIndexType::F16Quantized, e.type_id()),
            GenericInvertedIndex::U8NoQuantized(e) => (GenericInvertedIndexType::U8NoQuantized, e.type_id()),
        }
    }

    #[rustfmt::skip]
    pub fn open_from(
        index_path: &PathBuf,
        segment_id: Option<&str>,
        index_settings: &IndexSettings,
    ) -> crate::Result<Self> {
        match (
            index_settings.inverted_index_config.storage_type,
            index_settings.inverted_index_config.weight_type,
            index_settings.inverted_index_config.quantized,
        ) {
            (StorageType::Mmap, IndexWeightType::Float32, true) => Ok(Self::F32Quantized(InvertedIndexMmap::<f32, u8>::open(index_path, segment_id)?.into())),
            (StorageType::Mmap, IndexWeightType::Float32, false) => Ok(Self::F32NoQuantized(InvertedIndexMmap::<f32, f32>::open(index_path, segment_id)?.into())),
            (StorageType::Mmap, IndexWeightType::Float16, true) => Ok(Self::F16Quantized(InvertedIndexMmap::<half::f16, u8>::open(index_path, segment_id)?.into())),
            (StorageType::Mmap, IndexWeightType::Float16, false) => Ok(Self::F16NoQuantized(InvertedIndexMmap::<half::f16, half::f16>::open(index_path, segment_id)?.into())),
            (StorageType::Mmap, IndexWeightType::UInt8, false) => Ok(Self::U8NoQuantized(InvertedIndexMmap::<u8, u8>::open(index_path, segment_id)?.into())),
            (StorageType::CompressedMmap, IndexWeightType::Float32, true) => Ok(Self::F32Quantized(CompressedInvertedIndexMmap::<f32, u8>::open(index_path, segment_id)?.into())),
            (StorageType::CompressedMmap, IndexWeightType::Float32, false) => Ok(Self::F32NoQuantized(CompressedInvertedIndexMmap::<f32, f32>::open(index_path, segment_id)?.into())),
            (StorageType::CompressedMmap, IndexWeightType::Float16, true) => Ok(Self::F16Quantized(CompressedInvertedIndexMmap::<half::f16, u8>::open(index_path, segment_id)?.into())),
            (StorageType::CompressedMmap, IndexWeightType::Float16, false) => Ok(Self::F16NoQuantized(CompressedInvertedIndexMmap::<half::f16, half::f16>::open(index_path, segment_id)?.into())),
            (StorageType::CompressedMmap, IndexWeightType::UInt8, false) => Ok(Self::U8NoQuantized(CompressedInvertedIndexMmap::<u8, u8>::open(index_path, segment_id)?.into())),
            _ => {
                let error_msg = format!(
                    "Not supported! storage_type:{:?}, weight_type:{:?}, quantized:{}",
                    index_settings.inverted_index_config.storage_type, index_settings.inverted_index_config.weight_type, index_settings.inverted_index_config.quantized
                );
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
    }

    #[rustfmt::skip]
    pub fn metrics(&self) -> InvertedIndexMetrics {
        match self {
            GenericInvertedIndex::F32NoQuantized(e) => e.metrics(),
            GenericInvertedIndex::F32Quantized(e) => e.metrics(),
            GenericInvertedIndex::F16NoQuantized(e) => e.metrics(),
            GenericInvertedIndex::F16Quantized(e) => e.metrics(),
            GenericInvertedIndex::U8NoQuantized(e) => e.metrics(),
        }
    }

    #[rustfmt::skip]
    pub fn get_posting_opt(
        &self,
        dim_id: DimId,
        min_row_id: &mut RowId,
        max_row_id: &mut RowId
    ) -> Option<GenericPostingListIterator<'_>> {
        match self {
            GenericInvertedIndex::F32NoQuantized(e) => e.get_posting_opt(dim_id, min_row_id, max_row_id),
            GenericInvertedIndex::F32Quantized(e) => e.get_posting_opt(dim_id, min_row_id, max_row_id),
            GenericInvertedIndex::F16NoQuantized(e) => e.get_posting_opt(dim_id, min_row_id, max_row_id),
            GenericInvertedIndex::F16Quantized(e) => e.get_posting_opt(dim_id, min_row_id, max_row_id),
            GenericInvertedIndex::U8NoQuantized(e) => e.get_posting_opt(dim_id, min_row_id, max_row_id),
        }
    }

    #[rustfmt::skip]
    pub fn support_pruning(&self) -> bool {
        match self {
            GenericInvertedIndex::F32NoQuantized(e) => e.support_pruning(),
            GenericInvertedIndex::F32Quantized(e) => e.support_pruning(),
            GenericInvertedIndex::F16NoQuantized(e) => e.support_pruning(),
            GenericInvertedIndex::F16Quantized(e) => e.support_pruning(),
            GenericInvertedIndex::U8NoQuantized(e) => e.support_pruning(),
        }
    }

    #[rustfmt::skip]
    pub fn merge(generic_inverted_indexes: Vec<&GenericInvertedIndex>, directory:PathBuf, segment_id: Option<&str>, element_type: Option<ElementType>) -> crate::Result<(usize, Vec<PathBuf>)> {
        // Boundary.
        if generic_inverted_indexes.len() <= 1 {
            return Err(SparseError::Error("Candidates size <= 1".to_string()));
        }
        let types: Vec<(GenericInvertedIndexType, InvertedIndexWrapperType)> = generic_inverted_indexes.iter().map(|e| e.type_id()).collect();
        if !types.iter().all(|&t|t==types[0]) {
            let error_msg = "Error happended when merging a group of GenericInvertedIndexes, they types should keep same.";
            error!("{}", error_msg);
            panic!("{}", error_msg);
        }

        // get first index type
        let type_id = generic_inverted_indexes.first().unwrap().type_id();
        info!(">>>>>>>>>>> type_id:{:?}", type_id);

        // collect and merge
        match type_id {
            (GenericInvertedIndexType::F32NoQuantized, InvertedIndexWrapperType::Simple) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<f32, f32>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F32NoQuantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::SimpleInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                debug!(">>>>>>>>>>> after inverted index mmaps converted, size:{:?}", inverted_index_mmaps.len());
                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::EXTENDED));
                debug!(">>>>>>>>>>> got inverted index mmap merger");
                let merged_index: InvertedIndexMmap<f32, f32> = merger.merge(&directory, segment_id)?;
                debug!(">>>>>>>>>>> executed merge");
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::F32Quantized, InvertedIndexWrapperType::Simple) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<f32, u8>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F32Quantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::SimpleInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();
                debug!(">>>>>>>>>>> after inverted index mmaps converted, size:{:?}", inverted_index_mmaps.len());
                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::SIMPLE));
                debug!(">>>>>>>>>>> got inverted index mmap merger");
                let merged_index: InvertedIndexMmap<f32, u8> = merger.merge(&directory, segment_id)?;
                debug!(">>>>>>>>>>> executed merge");
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::F16NoQuantized, InvertedIndexWrapperType::Simple) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<half::f16, half::f16>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F16NoQuantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::SimpleInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::EXTENDED));
                let merged_index: InvertedIndexMmap<half::f16, half::f16> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::F16Quantized, InvertedIndexWrapperType::Simple) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<half::f16, u8>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F16Quantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::SimpleInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::SIMPLE));
                let merged_index: InvertedIndexMmap<half::f16, u8> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::U8NoQuantized, InvertedIndexWrapperType::Simple) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<u8, u8>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::U8NoQuantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::SimpleInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::EXTENDED));
                let merged_index: InvertedIndexMmap<u8, u8> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::F32NoQuantized, InvertedIndexWrapperType::Compressed) => {
                let inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<f32, f32>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F32NoQuantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::CompressedInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = CompressedInvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::EXTENDED));
                let merged_index: CompressedInvertedIndexMmap<f32, f32> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::F32Quantized, InvertedIndexWrapperType::Compressed) => {
                let inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<f32, u8>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F32Quantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::CompressedInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = CompressedInvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::SIMPLE));
                let merged_index: CompressedInvertedIndexMmap<f32, u8> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::F16NoQuantized, InvertedIndexWrapperType::Compressed) => {
                let inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<half::f16, half::f16>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F16NoQuantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::CompressedInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = CompressedInvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::EXTENDED));
                let merged_index: CompressedInvertedIndexMmap<half::f16, half::f16> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::F16Quantized, InvertedIndexWrapperType::Compressed) => {
                let inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<half::f16, u8>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::F16Quantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::CompressedInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = CompressedInvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::SIMPLE));
                let merged_index: CompressedInvertedIndexMmap<half::f16, u8> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
            (GenericInvertedIndexType::U8NoQuantized, InvertedIndexWrapperType::Compressed) => {
                let inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<u8, u8>> = generic_inverted_indexes
                    .iter()
                    .map(|&index_mmap| match index_mmap {
                        GenericInvertedIndex::U8NoQuantized(wrapper) => match wrapper {
                            InvertedIndexWrapper::CompressedInvertedIndex(inverted_index_mmap) => inverted_index_mmap,
                            _ => panic!("Inconsistent index type, shouldn't happen."),
                        },
                        _ => panic!("Inconsistent index type, shouldn't happen."),
                    })
                    .collect();

                let merger = CompressedInvertedIndexMmapMerger::new(&inverted_index_mmaps, element_type.unwrap_or(ElementType::EXTENDED));
                let merged_index: CompressedInvertedIndexMmap<u8, u8> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            },
        }
    }
}
