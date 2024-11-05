use std::sync::Arc;

use crate::{
    api::clickhouse::{
        cache::{IndexReaderBridge, FFI_INDEX_SEARCHER_CACHE},
        utils::IndexManager,
    },
    core::SparseVector,
    ffi::ScoredPointOffset,
    reader::searcher::Searcher,
};

/// impl for `ffi_load_index_reader`
pub fn ffi_load_index_reader_impl(index_path: &str) -> crate::Result<bool> {
    IndexManager::load_index_reader_bridge(index_path)
}

/// impl for `ffi_free_index_reader`
pub fn ffi_free_index_reader_impl(index_path: &str) -> crate::Result<()> {
    IndexManager::free_index_reader(index_path)
}

/// impl for `ffi_sparse_search`
pub fn ffi_sparse_search_impl(
    index_path: &str,
    sparse_vector: &SparseVector,
    filter: &Vec<u8>,
    top_k: u32,
) -> crate::Result<Vec<ScoredPointOffset>> {
    let reader_bridge: Arc<IndexReaderBridge> =
        FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string())?;
    let searcher: Searcher = reader_bridge.reader.searcher();

    let res: Vec<ScoredPointOffset> =
        searcher.search(sparse_vector.clone().try_into().unwrap(), top_k)?;
    Ok(res)
}
