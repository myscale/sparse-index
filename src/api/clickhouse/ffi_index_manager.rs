use crate::api::clickhouse::cache::{
    IndexReaderBridge, IndexWriterBridge, FFI_INDEX_SEARCHER_CACHE, FFI_INDEX_WRITER_CACHE,
};
use crate::api::clickhouse::converter::CXX_STRING_CONVERTER;
use crate::api::clickhouse::utils::{ApiUtils, IndexManager};
use crate::core::SparseRowContent;
use crate::index::{Index, IndexSettings};
use crate::{ffi::*, RowId};
use cxx::{let_cxx_string, CxxString};
use std::path::Path;
use std::sync::Arc;

pub fn ffi_create_index(index_path: &CxxString) -> FFIBoolResult {
    let_cxx_string!(parameter = "{}");
    ffi_create_index_with_parameter(index_path, &parameter)
}

pub fn ffi_create_index_with_parameter(
    index_path: &CxxString,
    index_json_parameter: &CxxString,
) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_create_index_with_parameter";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string());
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

    if let Err(error) = IndexManager::prepare_directory(&index_path) {
        return ApiUtils::handle_error(FUNC_NAME, "failed to prepare directory", error.to_string());
    }

    // TODO 放到 Sparse Index 内部完成
    if let Err(error) = IndexManager::persist_index_params(&index_path, &index_json_parameter) {
        return ApiUtils::handle_error(
            FUNC_NAME,
            "failed to persist index json params",
            error.to_string(),
        );
    }

    // TODO 将 json_parameter 里面的参数传递给 IndexSettings
    let index = match Index::create_in_dir(Path::new(&index_path), IndexSettings::default()) {
        Ok(res) => res,
        Err(error) => {
            return ApiUtils::handle_error(
                FUNC_NAME,
                "failed create index in directory",
                error.to_string(),
            );
        }
    };

    let bridge = match IndexManager::create_writer(&index, &index_path) {
        Ok(res) => res,
        Err(error) => {
            return ApiUtils::handle_error(
                FUNC_NAME,
                "failed create index writer bridge",
                error.to_string(),
            );
        }
    };

    if let Err(error) =
        FFI_INDEX_WRITER_CACHE.set_index_writer_bridge(index_path.to_string(), Arc::new(bridge))
    {
        return ApiUtils::handle_error(FUNC_NAME, "ffailed set index writer bridge", error);
    }

    FFIBoolResult {
        result: true,
        error: FFIError {
            is_error: false,
            message: "".to_string(),
        },
    }
}

/// 向 ram 状态的索引中插入数据
pub fn ffi_insert_sparse_vector(
    index_path: &CxxString,
    row_id: RowId,
    sparse_vector: &Vec<TupleElement>,
) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_insert_sparse_vector";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string())
        }
    };

    let bridge = match IndexManager::get_index_writer_bridge(&index_path) {
        Ok(res) => res,
        Err(error) => {
            return ApiUtils::handle_error(
                FUNC_NAME,
                "failed get index writer bridge",
                error.to_string(),
            );
        }
    };

    let res = bridge.add_row(SparseRowContent {
        row_id,
        sparse_vector: sparse_vector.clone().try_into().unwrap(),
    });

    if res.is_err() {
        return ApiUtils::handle_error(
            FUNC_NAME,
            "failed add sparse row content to index",
            res.err().unwrap(),
        );
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
    static FUNC_NAME: &str = "ffi_commit_index";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string())
        }
    };

    let bridge = match IndexManager::get_index_writer_bridge(&index_path) {
        Ok(res) => res,
        Err(error) => {
            return ApiUtils::handle_error(
                FUNC_NAME,
                "failed get index writer bridge",
                error.to_string(),
            );
        }
    };

    if let Err(error) = bridge.commit() {
        return ApiUtils::handle_error(FUNC_NAME, "failed commit index", error);
    }
    // Reload, not need handle error.
    let _ = IndexManager::reload_index_reader(&index_path);

    FFIBoolResult {
        result: true,
        error: FFIError {
            is_error: false,
            message: String::new(),
        },
    }
}

pub fn ffi_free_index_writer(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_free_index_writer";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Can't convert 'index_path'", e.to_string());
        }
    };

    match IndexManager::free_index_writer(&index_path) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Error freeing index writer", e.to_string());
        }
    }
}
