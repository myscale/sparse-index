use std::sync::Arc;

use crate::api::clickhouse::cache::IndexReaderBridge;
use crate::{
    api::clickhouse::{
        cache::FFI_INDEX_SEARCHER_CACHE,
        converter::CXX_STRING_CONVERTER,
        utils::{ApiUtils, IndexManager},
    },
    ffi::{FFIBoolResult, FFIError, FFIScoreResult, TupleElement},
};
use cxx::CxxString;

pub fn ffi_load_index_reader(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_load_index_reader";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string());
        }
    };

    match IndexManager::load_index_reader_bridge(&index_path) {
        Ok(result) => FFIBoolResult {
            result,
            error: FFIError {
                is_error: false,
                message: String::new(),
            },
        },
        Err(e) => {
            ApiUtils::handle_error(FUNC_NAME, "failed load index reader bridge", e.to_string())
        }
    }
}

pub fn ffi_free_index_reader(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_free_index_reader";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string());
        }
    };

    if let Err(error) = IndexManager::free_index_reader(&index_path) {
        return ApiUtils::handle_error(FUNC_NAME, "failed free index reader", error.to_string());
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
    static FUNC_NAME: &str = "ffi_sparse_search";
    // ApiUtils::handle_error("ffi_sparse_commit_index", "Error creating index", "".to_string());
    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string())
        }
    };

    let reader_bridge: Arc<IndexReaderBridge> =
        match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
            Ok(res) => res,
            Err(error) => {
                return ApiUtils::handle_error(FUNC_NAME, "failed get index reader bridge", error);
            }
        };

    let searcher = reader_bridge.reader.searcher();
    let res = match searcher.search(sparse_vector.clone().try_into().unwrap(), top_k) {
        Ok(res) => res,
        Err(error) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed execute search", error.to_string());
        }
    };

    FFIScoreResult {
        result: res,
        error: FFIError {
            is_error: false,
            message: "".to_string(),
        },
    }
}
