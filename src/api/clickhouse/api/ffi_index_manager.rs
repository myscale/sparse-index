use crate::api::clickhouse::cache::{
    IndexReaderBridge, IndexWriterBridge, FFI_INDEX_SEARCHER_CACHE, FFI_INDEX_WRITER_CACHE,
};
use crate::api::clickhouse::converter::CXX_STRING_CONVERTER;
use crate::api::clickhouse::utils::{ApiUtils, IndexManager};
use crate::api::clickhouse::{
    ffi_commit_index_impl, ffi_create_index_with_parameter_impl, ffi_free_index_writer_impl,
    ffi_insert_sparse_vector_impl,
};
use crate::core::{SparseRowContent, SparseVector};
use crate::index::{Index, IndexSettings};
use crate::{ffi::*, RowId};
use cxx::{let_cxx_string, CxxString};
use rand::seq::index;
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

    match ffi_create_index_with_parameter_impl(&index_path, &index_json_parameter) {
        Ok(result) => {
            FFIBoolResult { result, error: FFIError { is_error: false, message: String::new() } }
        }
        Err(e) => ApiUtils::handle_error(
            FUNC_NAME,
            "failed to create index with parameter",
            e.to_string(),
        ),
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
    let sparse_vector: SparseVector = sparse_vector.clone().try_into().unwrap();

    match ffi_insert_sparse_vector_impl(&index_path, row_id, &sparse_vector) {
        Ok(result) => {
            FFIBoolResult { result, error: FFIError { is_error: false, message: String::new() } }
        }
        Err(e) => ApiUtils::handle_error(
            FUNC_NAME,
            "failed add sparse row content to index",
            e.to_string(),
        ),
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

    match ffi_commit_index_impl(&index_path) {
        Ok(result) => {
            FFIBoolResult { result, error: FFIError { is_error: false, message: String::new() } }
        }
        Err(e) => ApiUtils::handle_error(FUNC_NAME, "failed commit index", e.to_string()),
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

    match ffi_free_index_writer_impl(&index_path) {
        Ok(result) => {
            FFIBoolResult { result, error: FFIError { is_error: false, message: String::new() } }
        }
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "Error freeing index writer", e.to_string());
        }
    }
}
