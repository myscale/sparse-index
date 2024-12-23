use std::{path::Path, sync::Arc};

use crate::{
    api::cxx_ffi::{cache::FFI_INDEX_WRITER_CACHE, utils::IndexManager},
    core::{SparseRowContent, SparseVector},
    index::{Index, IndexSettings},
    RowId,
};

/// impl for `ffi_create_index_with_parameter`
pub fn ffi_create_index_with_parameter_impl(
    index_path: &str,
    index_json_parameter: &str,
) -> crate::Result<bool> {
    let _ = IndexManager::prepare_directory(&index_path)?;

    // Parse json_parameter into `IndexSettings` and check it's valid.
    let index_settings: IndexSettings = serde_json::from_str(&index_json_parameter)?;
    let _ = index_settings.inverted_index_config.is_valid()?;
    
    // Create Index.
    let index = Index::create_in_dir(Path::new(index_path), index_settings)?;

    let bridge = IndexManager::create_writer(&index, &index_path)?;

    let _ =
        FFI_INDEX_WRITER_CACHE.set_index_writer_bridge(index_path.to_string(), Arc::new(bridge))?;

    Ok(true)
}

/// impl for `ffi_insert_sparse_vector`
pub fn ffi_insert_sparse_vector_impl(
    index_path: &str,
    row_id: RowId,
    sparse_vector: &SparseVector,
) -> crate::Result<bool> {
    let bridge = IndexManager::get_index_writer_bridge(&index_path)?;

    let _ = bridge.add_row(SparseRowContent { row_id, sparse_vector: sparse_vector.clone() })?;

    Ok(true)
}

/// impl for `ffi_commit_index`
pub fn ffi_commit_index_impl(index_path: &str) -> crate::Result<bool> {
    let bridge = IndexManager::get_index_writer_bridge(&index_path)?;
    let _ = bridge.commit()?;
    // Reload, not need handle error.
    let _ = IndexManager::reload_index_reader(&index_path);
    Ok(true)
}

/// impl for `ffi_free_index_writer`
pub fn ffi_free_index_writer_impl(index_path: &str) -> crate::Result<bool> {
    let res = IndexManager::free_index_writer(&index_path)?;
    Ok(res)
}
