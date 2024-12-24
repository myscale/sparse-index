use crate::api::cxx_ffi::converter::cxx_vector_converter;
use crate::api::cxx_ffi::{
    ffi_free_index_reader_impl, ffi_load_index_reader_impl, ffi_sparse_search_impl,
};
use crate::core::{SparseBitmap, SparseVector};
use crate::{
    api::cxx_ffi::{converter::CXX_STRING_CONVERTER, utils::ApiUtils},
    ffi::{FFIBoolResult, FFIError, FFIScoreResult, TupleElement},
};
use cxx::{CxxString, CxxVector};

pub fn ffi_load_index_reader(index_path: &CxxString) -> FFIBoolResult {
    static FUNC_NAME: &str = "ffi_load_index_reader";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string());
        }
    };

    match ffi_load_index_reader_impl(&index_path) {
        Ok(result) => {
            FFIBoolResult { result, error: FFIError { is_error: false, message: String::new() } }
        }
        Err(e) => ApiUtils::handle_error(FUNC_NAME, "failed load index reader", e.to_string()),
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

    if let Err(error) = ffi_free_index_reader_impl(&index_path) {
        return ApiUtils::handle_error(FUNC_NAME, "failed free index reader", error.to_string());
    } else {
        FFIBoolResult { result: true, error: FFIError { is_error: false, message: String::new() } }
    }
}

pub fn ffi_sparse_search(
    index_path: &CxxString,
    sparse_vector: &Vec<TupleElement>,
    filter: &CxxVector<u8>,
    enable_filter: bool,
    top_k: u32,
) -> FFIScoreResult {
    static FUNC_NAME: &str = "ffi_sparse_search";

    let index_path: String = match CXX_STRING_CONVERTER.convert(index_path) {
        Ok(path) => path,
        Err(e) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed convert 'index_path'", e.to_string())
        }
    };

    // convert `filter` u8_bitmap`
    let u8_alive_bitmap: Vec<u8> = match cxx_vector_converter::<u8>().convert(filter) {
        Ok(bitmap) => bitmap,
        Err(e) => {
            return ApiUtils::handle_error(
                FUNC_NAME,
                "Can't convert 'u8_alive_bitmap'",
                e.to_string(),
            );
        }
    };

    let sparse_bitmap = match enable_filter {
        true => Some(SparseBitmap::from(u8_alive_bitmap)),
        false => None,
    };

    // convert `sparse_vector`
    let sparse_vector: SparseVector = sparse_vector.clone().try_into().unwrap();

    let scores = match ffi_sparse_search_impl(&index_path, &sparse_vector, &sparse_bitmap, top_k) {
        Ok(res) => res,
        Err(error) => {
            return ApiUtils::handle_error(FUNC_NAME, "failed execute search", error.to_string());
        }
    };

    FFIScoreResult { result: scores, error: FFIError { is_error: false, message: "".to_string() } }
}
