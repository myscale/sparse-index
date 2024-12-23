use crate::{
    common::{errors::SparseError, executor::Executor},
    debug_ck, info_ck,
    reader::IndexReader,
    warn_ck,
};
use flurry::HashMap;
use once_cell::sync::OnceCell;
use std::sync::Arc;

pub struct IndexReaderBridge {
    pub path: String,
    pub reader: IndexReader,
}

impl Drop for IndexReaderBridge {
    fn drop(&mut self) {
        info_ck!("IndexReaderBridge has been dropped. index_path:[{}]", self.path);
    }
}

impl IndexReaderBridge {
    #[allow(dead_code)]
    pub fn reader_address(&self) -> usize {
        &self.reader as *const IndexReader as usize
    }
    pub fn reload(&self) -> Result<(), String> {
        self.reader.reload().map_err(|e| e.to_string())
    }
}

pub struct IndexReaderBridgeCache {
    cache: HashMap<String, Arc<IndexReaderBridge>>,
    shared_thread_pool: OnceCell<Arc<Executor>>,
}

impl IndexReaderBridgeCache {
    pub fn new() -> Self {
        Self { cache: HashMap::new(), shared_thread_pool: OnceCell::new() }
    }

    pub fn set_index_reader_bridge(
        &self,
        key: String,
        value: Arc<IndexReaderBridge>,
    ) -> Result<(), String> {
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        let pinned = self.cache.pin();
        if pinned.contains_key(&trimmed_key) {
            pinned.insert(trimmed_key.clone(), value.clone());
            let message = format!(
                "IndexReaderBridge already exists with given key: [{}], it has been overwritten.",
                trimmed_key
            );
            warn_ck!("{}", message)
        } else {
            pinned.insert(trimmed_key, value.clone());
        }
        Ok(())
    }

    pub fn get_index_reader_bridge(&self, key: String) -> Result<Arc<IndexReaderBridge>, String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        match pinned.get(&trimmed_key) {
            Some(result) => Ok(result.clone()),
            None => {
                Err(format!("IndexReaderBridge doesn't exist with given key: [{}]", trimmed_key))
            }
        }
    }

    pub fn remove_index_reader_bridge(&self, key: String) -> crate::Result<()> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.remove(&trimmed_key);
        } else {
            let message: String = format!(
                "IndexReaderBridge doesn't exist, can't remove it with given key [{}]",
                trimmed_key
            );
            debug_ck!("{}", message);
            SparseError::Error(message);
        }
        Ok(())
    }

    // shared thread pool for index searcher.
    pub fn get_shared_multi_thread_executor(
        &self,
        num_threads: usize,
    ) -> Result<Arc<Executor>, String> {
        if num_threads <= 0 {
            return Err("threads number minimum is 1".to_string());
        }
        let res: Result<&Arc<Executor>, String> = self.shared_thread_pool.get_or_try_init(|| {
            Executor::multi_thread(num_threads, "sparse-search-")
                .map(Arc::new)
                .map_err(|e| e.to_string())
        });

        res.map(|executor| executor.clone())
    }
}
