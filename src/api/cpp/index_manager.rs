use crate::api::cpp::utils::ApiUtils;
use crate::common::constants::CXX_STRING_CONVERTER;
use crate::common::RAM_BUILDER_CACHE;
use crate::core::{
    InvertedIndex, InvertedIndexBuilder, InvertedIndexCompressedImmutableRam, InvertedIndexCompressedMmap, InvertedIndexEnum, InvertedIndexImmutableRam, InvertedIndexMmap, QuantizedU8
};
use crate::ffi::*;
use crate::sparse_index::{parse_index_type, SparseIndexConfig};
use cxx::{let_cxx_string, CxxString};
use half::f16;
use std::borrow::Cow;
use std::path::Path;

pub fn ffi_create_index(index_path: &CxxString) -> FFIBoolResult {
    let_cxx_string!(parameter = "{}");
    ffi_create_index_with_parameter(index_path, &parameter)
}

pub fn ffi_create_index_with_parameter(
    index_path: &CxxString,
    index_json_parameter: &CxxString,
) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_sparse_create_index_with_parameter";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    let index_json_parameter: String = match CXX_STRING_CONVERTER.convert(index_json_parameter) {
        Ok(json) => json,
        Err(e) => {
            return ApiUtils::handle_error(
                FUNC_NAME,
                "Can't convert 'index_json_parameter'",
                e.to_string(),
            );
        }
    };
    // 存储索引配置
    let config: SparseIndexConfig = match serde_json::from_str(&index_json_parameter) {
        Ok(conf) => conf,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't serde frome 'index_json_parameter'", e.to_string());
        }
    };
    match config.save(Path::new(&index_path)) {
        Ok(_) => (),
        Err(e) => return ApiUtils::handle_error(FUNC_NAME, "Can't save json parameter to disk", e.to_string()),
    };

    // TODO insert 到 cache 里的时候没有用到 mut，但是使用的时候可以重新声明为 mut?
    let builder = InvertedIndexBuilder::new();
    match RAM_BUILDER_CACHE.push(&index_path, builder) {
        Ok(res) => FFIBoolResult {
            result: true,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => FFIBoolResult {
            result: false,
            error: FFIError {
                is_error: true,
                message: e,
            },
        },
    }
}

/// 向 ram 状态的索引中插入数据
pub fn ffi_insert_sparse_vector(
    index_path: &CxxString,
    row_id: u32,
    sparse_vector: &Vec<TupleElement>,
) -> FFIBoolResult {
    let func_name = "ffi_insert_sparse_vector";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(func_name, "Error parse index_path", e.to_string())
        }
    };

    if let Err(e) = RAM_BUILDER_CACHE.update(&index_path, |builder| {
        builder.add(row_id, sparse_vector.clone().try_into().unwrap());
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

/// 将索引存储到本地
pub fn ffi_commit_index(index_path: &CxxString) -> FFIBoolResult {
    let func_name = "ffi_commit_index";
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

    if let Err(e) = RAM_BUILDER_CACHE.consume(&index_path, |builder| {
        match index_type {
            InvertedIndexEnum::InvertedIndexImmutableRam => {
                let index = InvertedIndexImmutableRam::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("");
                let _ = index
                    .save(Path::new(&index_path.clone()))
                    .expect("can't save immutable ram index file.");
            }
            InvertedIndexEnum::InvertedIndexMmap => {
                // will auto save index file.
                let _ = InvertedIndexMmap::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("can't create mmap index");
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamF32 => {
                let index = InvertedIndexCompressedImmutableRam::<f32>::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("can't create compressed immutable ram index f32");
                let _ = index
                    .save(Path::new(&index_path.clone()))
                    .expect("can't save compressed immutable ram file 32.");
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamF16 => {
                let index = InvertedIndexCompressedImmutableRam::<f16>::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("can't create compressed immutable ram index f16");
                let _ = index
                    .save(Path::new(&index_path.clone()))
                    .expect("can't save compressed immutable ram file 16.");
            }
            InvertedIndexEnum::InvertedIndexCompressedImmutableRamU8 => {
                let index = InvertedIndexCompressedImmutableRam::<QuantizedU8>::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("can't create compressed immutable ram index u8");
                let _ = index
                    .save(Path::new(&index_path.clone()))
                    .expect("can't save compressed immutable ram file u8.");
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapF32 => {
                let index = InvertedIndexCompressedMmap::<f32>::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("can't create compressed mmap index f32");
                let _ = index
                    .save(Path::new(&index_path.clone()))
                    .expect("can't save compressed mmap file 32.");
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapF16 => {
                let index = InvertedIndexCompressedMmap::<f16>::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("can't create compressed mmap index f16");
                let _ = index
                    .save(Path::new(&index_path.clone()))
                    .expect("can't save compressed mmap file 16.");
            }
            InvertedIndexEnum::InvertedIndexCompressedMmapU8 => {
                let index = InvertedIndexCompressedMmap::<QuantizedU8>::from_ram_index(
                    Cow::Owned(builder.build()),
                    index_path.clone(),
                )
                .expect("can't create compressed mmap index u8");
                let _ = index
                    .save(Path::new(&index_path.clone()))
                    .expect("can't save compressed mmap file u8.");
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
