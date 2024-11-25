use std::path::PathBuf;

use log::info;

use crate::{
    common::errors::SparseError,
    core::{
        CompressedInvertedIndexMmap, CompressedInvertedIndexMmapMerger,
        GenericInvertedIndexMmapType, InvertedIndexMmap, InvertedIndexMmapAccess,
        InvertedIndexMmapMerger,
    },
    index::{IndexSettings, Segment, SegmentReader},
};

pub struct IndexMerger {
    pub(crate) readers: Vec<SegmentReader>,
    pub(crate) index_settings: Option<IndexSettings>,
}

impl IndexMerger {
    pub fn open(segments: &[Segment]) -> crate::Result<IndexMerger> {
        if segments.len() == 0 {
            return Ok(Self {
                readers: vec![],
                index_settings: None,
            });
        }
        // WARN 编译器隐式转换, map 操作应该是返回 Vec<Result>，但是返回的是 Result<Vec>，这是编译器的隐式转换，如果所有的元素都是 Ok，就正常，若有一个是 Error，就会立刻返回 Error
        let segment_readers: Vec<SegmentReader> = segments
            .iter()
            .map(|seg| SegmentReader::open(seg))
            .collect::<crate::Result<Vec<SegmentReader>>>()?;

        // 获取所有 segment 对应的 index_settings
        let index_settings_vec: Vec<_> = segments
            .iter()
            .map(|seg| seg.index().index_settings())
            .collect();

        // 断言所有的 index_settings 彼此相等
        if !index_settings_vec.windows(2).all(|w| w[0] == w[1]) {
            return Err(crate::SparseError::Error(
                "index_settings should be same in IndexMerger".to_string(),
            ));
        }

        Ok(Self {
            readers: segment_readers,
            index_settings: Some(index_settings_vec[0].clone()),
        })
    }

    pub fn merge(
        &self,
        directory: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<(usize, Vec<PathBuf>)> {
        if self.readers.len() == 0 {
            return Ok((0, vec![]));
        }

        let inverted_index_mmaps: Vec<&GenericInvertedIndexMmapType> = self
            .readers
            .iter()
            .map(|segment_reader| segment_reader.get_inverted_index())
            .collect::<Vec<&GenericInvertedIndexMmapType>>();

        match inverted_index_mmaps.first().unwrap() {
            GenericInvertedIndexMmapType::InvertedIndexMmapF32NoQuantized(_) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<f32, f32>> = inverted_index_mmaps
                    .iter()
                    .map(|index_mmap| match index_mmap {
                        GenericInvertedIndexMmapType::InvertedIndexMmapF32NoQuantized(index) => {
                            index
                        }
                        _ => panic!("Inconsistent index types"),
                    })
                    .collect();
                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps);
                let merged_index: InvertedIndexMmap<f32, f32> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF32Quantized(_) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<f32, u8>> = inverted_index_mmaps
                    .iter()
                    .map(|index_mmap| match index_mmap {
                        GenericInvertedIndexMmapType::InvertedIndexMmapF32Quantized(index) => index,
                        _ => panic!("Inconsistent index types"),
                    })
                    .collect();
                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps);
                let merged_index: InvertedIndexMmap<f32, u8> = merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));

            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF16NoQuantized(_) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<half::f16, half::f16>> =
                    inverted_index_mmaps
                        .iter()
                        .map(|index_mmap| match index_mmap {
                            GenericInvertedIndexMmapType::InvertedIndexMmapF16NoQuantized(
                                index,
                            ) => index,
                            _ => panic!("Inconsistent index types"),
                        })
                        .collect();
                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps);
                let merged_index: InvertedIndexMmap<half::f16, half::f16> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF16Quantized(_) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<half::f16, u8>> =
                    inverted_index_mmaps
                        .iter()
                        .map(|index_mmap| match index_mmap {
                            GenericInvertedIndexMmapType::InvertedIndexMmapF16Quantized(index) => {
                                index
                            }
                            _ => panic!("Inconsistent index types"),
                        })
                        .collect();
                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps);
                let merged_index: InvertedIndexMmap<half::f16, u8> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapU8NoQuantized(_) => {
                let inverted_index_mmaps: Vec<&InvertedIndexMmap<u8, u8>> = inverted_index_mmaps
                    .iter()
                    .map(|index_mmap| match index_mmap {
                        GenericInvertedIndexMmapType::InvertedIndexMmapU8NoQuantized(index) => {
                            index
                        }
                        _ => panic!("Inconsistent index types"),
                    })
                    .collect();
                let merger = InvertedIndexMmapMerger::new(&inverted_index_mmaps);
                let merged_index: InvertedIndexMmap<u8, u8> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32NoQuantized(_) => {
                info!("comp-0");
                let compressed_inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<f32, f32>> = inverted_index_mmaps.iter().map(|index_mmap| {
                    match index_mmap {
                        GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32NoQuantized(index) => index,
                        _ => panic!("Inconsistent index types")
                    }
                }).collect();
                // info!("comp-1");
                let merger = CompressedInvertedIndexMmapMerger::new(&compressed_inverted_index_mmaps);
                info!("call compressed inverted index mmap -> merge");
                let merged_index: CompressedInvertedIndexMmap<f32, f32> = merger.merge(&directory, segment_id)?;
                // info!("comp-3");
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                // info!("comp-4");
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32Quantized(_) => {
                let compressed_inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<f32, u8>> = inverted_index_mmaps.iter().map(|index_mmap| {
                    match index_mmap {
                        GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32Quantized(index) => index,
                        _ => panic!("Inconsistent index types")
                    }
                }).collect();
                let merger =
                    CompressedInvertedIndexMmapMerger::new(&compressed_inverted_index_mmaps);
                let merged_index: CompressedInvertedIndexMmap<f32, u8> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16NoQuantized(_) => {
                let compressed_inverted_index_mmaps: Vec<
                    &CompressedInvertedIndexMmap<half::f16, half::f16>,
                > = inverted_index_mmaps
                    .iter()
                    .map(|index_mmap| match index_mmap {
                        GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16NoQuantized(
                            index,
                        ) => index,
                        _ => panic!("Inconsistent index types"),
                    })
                    .collect();
                let merger =
                    CompressedInvertedIndexMmapMerger::new(&compressed_inverted_index_mmaps);
                let merged_index: CompressedInvertedIndexMmap<half::f16, half::f16> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16Quantized(_) => {
                let compressed_inverted_index_mmaps: Vec<
                    &CompressedInvertedIndexMmap<half::f16, u8>,
                > = inverted_index_mmaps
                    .iter()
                    .map(|index_mmap| match index_mmap {
                        GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16Quantized(
                            index,
                        ) => index,
                        _ => panic!("Inconsistent index types"),
                    })
                    .collect();
                let merger =
                    CompressedInvertedIndexMmapMerger::new(&compressed_inverted_index_mmaps);
                let merged_index: CompressedInvertedIndexMmap<half::f16, u8> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapU8NoQuantized(_) => {
                let compressed_inverted_index_mmaps: Vec<&CompressedInvertedIndexMmap<u8, u8>> = inverted_index_mmaps.iter().map(|index_mmap| {
                    match index_mmap {
                        GenericInvertedIndexMmapType::CompressedInvertedIndexMmapU8NoQuantized(index) => index,
                        _ => panic!("Inconsistent index types")
                    }
                }).collect();
                let merger =
                    CompressedInvertedIndexMmapMerger::new(&compressed_inverted_index_mmaps);
                let merged_index: CompressedInvertedIndexMmap<u8, u8> =
                    merger.merge(&directory, segment_id)?;
                let vector_count = merged_index.meta.inverted_index_meta.vector_count;
                let related_files = merged_index.files(segment_id);
                return Ok((vector_count, related_files));
            }
        }
    }
}
