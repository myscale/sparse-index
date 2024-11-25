use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::api::clickhouse::cache::{
    IndexReaderBridge, IndexWriterBridge, FFI_INDEX_SEARCHER_CACHE, FFI_INDEX_WRITER_CACHE,
};
use crate::common::errors::SparseError;
use crate::error_ck;
use crate::index::Index;
use crate::indexer::LogMergePolicy;
use crate::reader::{IndexReader, ReloadPolicy};
use crate::sparse_index::SparseIndexConfig;

const MEMORY_64MB: usize = 1024 * 1024 * 64;
const BUILD_THREADS: usize = 4;

pub struct IndexManager;

impl IndexManager {
    pub(crate) fn free_index_writer(index_path: &str) -> crate::Result<bool> {
        // Get index writer bridge from CACHE
        let bridge = IndexManager::get_index_writer_bridge(index_path);
        if bridge.is_err() {
            return Ok(false);
        }
        let bridge = bridge.unwrap();

        // Waiting all merging threads finished.
        bridge.wait_merging_threads().map_err(|e| {
            let error_info = format!("Can't wait merging threads, exception: {}", e);
            error_ck!("{}", error_info);
            SparseError::Error(error_info)
        })?;

        // Remove index writer from CACHE
        FFI_INDEX_WRITER_CACHE.remove_index_writer_bridge(index_path.to_string())?;

        Ok(true)
    }

    pub(crate) fn free_index_reader(index_path: &str) -> crate::Result<()> {
        FFI_INDEX_SEARCHER_CACHE.remove_index_reader_bridge(index_path.to_string())
    }

    /// drop `reader` and `writer` in current directory.
    pub(crate) fn prepare_directory(index_path: &str) -> crate::Result<()> {
        let _ = Self::free_index_reader(index_path);
        Self::free_index_writer(index_path)?;
        Ok(())
    }

    pub(crate) fn persist_index_params(
        index_path: &str,
        index_json_parameter: &str,
    ) -> crate::Result<()> {
        let config: SparseIndexConfig = serde_json::from_str(&index_json_parameter)?;
        Ok(config.save(Path::new(&index_path))?)
    }

    pub(crate) fn create_writer(
        index: &Index,
        index_path: &str,
    ) -> crate::Result<IndexWriterBridge> {
        let writer = index.writer_with_num_threads(BUILD_THREADS, MEMORY_64MB).map_err(|e| {
            let error_info = format!("Failed to create sparse index writer: {}", e);
            error_ck!("{}", error_info);
            SparseError::Error(error_info)
        })?;

        let mut merge_policy = LogMergePolicy::default();
        // merge_policy.set_min_num_segments(5);
        writer.set_merge_policy(Box::new(merge_policy));

        Ok(IndexWriterBridge {
            path: index_path.trim_end_matches('/').to_string(),
            writer: Mutex::new(Some(writer)),
        })
    }

    pub(crate) fn get_index_writer_bridge(
        index_path: &str,
    ) -> crate::Result<Arc<IndexWriterBridge>> {
        Ok(FFI_INDEX_WRITER_CACHE.get_index_writer_bridge(index_path.to_string())?)
    }

    pub(crate) fn reload_index_reader(index_path: &str) -> crate::Result<bool> {
        let reload_status =
            match FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string()) {
                Ok(current_index_reader) => match current_index_reader.reload() {
                    Ok(_) => true,
                    Err(e) => {
                        return Err(SparseError::Error(e));
                    }
                },
                Err(_) => true,
            };
        return Ok(reload_status);
    }

    pub fn load_index_reader_bridge(index_path: &str) -> crate::Result<bool> {
        // Boundary.
        let index_files_directory = Path::new(index_path);
        if !index_files_directory.exists() || !index_files_directory.is_dir() {
            let error_info: String = format!("index_path not exists: {:?}", index_path);
            return Err(SparseError::Error(error_info));
        }
        let index_path = index_path.trim_end_matches('/');

        // Free old reader bridge.
        let bridge = FFI_INDEX_SEARCHER_CACHE.get_index_reader_bridge(index_path.to_string());
        if bridge.is_ok() {
            let _ = Self::free_index_reader(index_path);
        }

        // Load sparse index with given directory.
        let mut index = Index::open_in_dir(index_files_directory)?;

        // set shared thread pool for index reader
        match FFI_INDEX_SEARCHER_CACHE.get_shared_multi_thread_executor(num_cpus::get()) {
            Ok(shared_thread_pool) => {
                index.set_shared_multithread_executor(shared_thread_pool)?;
            }
            Err(_) => {
                index.set_default_multithread_executor()?;
            }
        }

        // Create a reader for the index with an appropriate reload policy.
        // OnCommit: reload when commiting; Manual: developer need call IndexReader::reload() to reload.
        let reader: IndexReader =
            index.reader_builder().reload_policy(ReloadPolicy::OnCommitWithDelay).try_into()?;

        // Save IndexReaderBridge to cache.
        let index_reader_bridge =
            IndexReaderBridge { reader, path: index_path.trim_end_matches('/').to_string() };

        FFI_INDEX_SEARCHER_CACHE
            .set_index_reader_bridge(index_path.to_string(), Arc::new(index_reader_bridge))?;

        Ok(true)
    }
}
