use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use cxx::CxxString;
use half::f16;
use crate::api::cpp::utils::ApiUtils;
use crate::common::constants::{CXX_STRING_CONVERTER, RAM_BUILDER_CACHE};
use crate::common::{ElementOffsetType, TopK};
use crate::core::common::QuantizedU8;
use crate::core::inverted_index::{InvertedIndex, InvertedIndexCompressedImmutableRam, InvertedIndexCompressedMmap, InvertedIndexImmutableRam, InvertedIndexMmap};
use crate::core::scores::ScoresMemoryPool;
use crate::core::SearchContext;
use crate::ffi::*;
use crate::index::{parse_index_type, InvertedIndexEnum, INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F16_CACHE, INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F32_CACHE, INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_U8_CACHE, INVERTED_INDEX_COMPRESSED_MMAP_F16_CACHE, INVERTED_INDEX_COMPRESSED_MMAP_F32_CACHE, INVERTED_INDEX_COMPRESSED_MMAP_U8_CACHE, INVERTED_INDEX_IMMUTABLE_RAM_CACHE, INVERTED_INDEX_MMAP_CACHE};
use crate::index::sparse_index_config::SparseIndexConfig;

/// load 索引对象到全局 cache
pub fn ffi_load_index(
    index_path: &CxxString,
) -> FFIBoolResult
{
    let func_name = "ffi_load_index";
    // TODO 将 Ram 中的内存转换为 mmap 存储
    // ApiUtils::handle_error("ffi_sparse_commit_index", "Error creating index", "".to_string());
    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(func_name, "Error parse index_path", e.to_string())
        }
    };

    let config = SparseIndexConfig::load(Path::new(&index_path)).expect("can't load");
    let index_type = parse_index_type(config).expect("can't parse");

    // TODO 封装一个自定义的 Flurry Hash Map 简化逻辑
    if let Err(e) = RAM_BUILDER_CACHE.consume(&index_path, |builder| {
        match index_type {
            InvertedIndexEnum::InvertedIndexImmutableRam => {
                let index = InvertedIndexImmutableRam::open(Path::new(&index_path)).expect("can't open immutable ram file");
                let pinned = INVERTED_INDEX_IMMUTABLE_RAM_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
            }
            InvertedIndexEnum::InvertedIndexMmap => {
                let index = InvertedIndexMmap::open(Path::new(&index_path)).expect("can't open mmap index");
                let pinned = INVERTED_INDEX_MMAP_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamF32 => {
                let index = InvertedIndexCompressedImmutableRam::<f32>::open(Path::new(&index_path)).expect("can't open compressed immutable ram f32 index");
                let pinned = INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F32_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));

            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamF16 => {
                let index = InvertedIndexCompressedImmutableRam::<f16>::open(Path::new(&index_path)).expect("can't open compressed immutable ram f16 index");
                let pinned = INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_F16_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamU8 => {
                let index = InvertedIndexCompressedImmutableRam::<QuantizedU8>::open(Path::new(&index_path)).expect("can't open compressed immutable ram u8 index");
                let pinned = INVERTED_INDEX_COMPRESSED_IMMUTABLE_RAM_U8_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapF32 => {
                let index = InvertedIndexCompressedMmap::<f32>::open(Path::new(&index_path)).expect("can't open compressed mmap f32 index");
                let pinned = INVERTED_INDEX_COMPRESSED_MMAP_F32_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapF16 => {
                let index = InvertedIndexCompressedMmap::<f16>::open(Path::new(&index_path)).expect("can't open compressed mmap f16 index");
                let pinned = INVERTED_INDEX_COMPRESSED_MMAP_F16_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapU8 => {
                let index = InvertedIndexCompressedMmap::<QuantizedU8>::open(Path::new(&index_path)).expect("can't open compressed mmap u8 index");
                let pinned = INVERTED_INDEX_COMPRESSED_MMAP_U8_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
            }
        }
    }) {
        FFIBoolResult {
            result: false,
            error: FFIError {
                is_error: true,
                message: e,
            },
        }
    } else {
        FFIBoolResult {
            result: true,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        }
    }
}

pub fn ffi_sparse_search(
    index_path: &CxxString,
    sparse_vector: &Vec<TupleElement>,
    filter: &Vec<u8>,
    top_k: u32,
) -> FFIScoreResult {
    let func_name = "ffi_load_index";
    // TODO 将 Ram 中的内存转换为 mmap 存储
    // ApiUtils::handle_error("ffi_sparse_commit_index", "Error creating index", "".to_string());
    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(func_name, "Error parse index_path", e.to_string())
        }
    };

    let config = SparseIndexConfig::load(Path::new(&index_path)).expect("can't load");
    let index_type = parse_index_type(config).expect("can't parse");

    // TODO 封装一个自定义的 Flurry Hash Map 简化逻辑
    let search_result  =
        match index_type {
            InvertedIndexEnum::InvertedIndexImmutableRam => {
                let index = InvertedIndexImmutableRam::open(Path::new(&index_path)).expect("can't open immutable ram file");
                let pinned = INVERTED_INDEX_IMMUTABLE_RAM_CACHE.pin();
                pinned.insert(index_path.clone(), Arc::new(index));
                let res = pinned.get(&index_path).unwrap();

                // TODO 简化逻辑
                let is_stopped = AtomicBool::default();
                let scores_memory_pool = ScoresMemoryPool::new();

                let memory_handle = scores_memory_pool.get();
                let mut search_context = SearchContext::new(
                    sparse_vector.clone().try_into().unwrap(),
                    top_k as usize,
                    res.as_ref(),
                    memory_handle,
                    &is_stopped,
                );
                let f = |id: ElementOffsetType| true;
                Ok(search_context.search(&f))
            }
            InvertedIndexEnum::InvertedIndexMmap => {
                Err("NotSupported".to_string())
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamF32 => {
                Err("NotSupported".to_string())
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamF16 => {
                Err("NotSupported".to_string())
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamU8 => {
                Err("NotSupported".to_string())
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapF32 => {
                Err("NotSupported".to_string())
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapF16 => {
                Err("NotSupported".to_string())
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapU8 => {
                Err("NotSupported".to_string())
            }
        };

    if search_result.is_ok() {
        FFIScoreResult {
            result: search_result.unwrap(),
            error: FFIError { is_error: false, message: "".to_string() },
        }
    } else {
        FFIScoreResult {
            result: vec![],
            error: FFIError { is_error: true, message: "Not supported".to_string() },
        }
    }
}