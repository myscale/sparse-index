use std::sync::{Arc, Mutex};

use flurry::HashMap;
use log::{debug, info, warn};

use crate::{core::SparseRowContent, index::Index, indexer::IndexWriter, Opstamp};


pub struct IndexWriterBridge {
    pub path: String,
    pub index: Index,
    pub writer: Mutex<Option<IndexWriter>>,
}

impl IndexWriterBridge {
    pub fn commit(&self) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.commit().map_err(|e| e.to_string())
                } else {
                    Err("IndexWriterBridge is not available for commit".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    pub fn add_row(&self, row: SparseRowContent) -> Result<Opstamp, String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.as_mut() {
                    writer.add_document(row).map_err(|e| e.to_string())
                } else {
                    Err("IndexWriterBridge is not available for add_document".to_string())
                }
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }

    pub fn wait_merging_threads(&self) -> Result<(), String> {
        match self.writer.lock() {
            Ok(mut writer) => {
                if let Some(writer) = writer.take() {
                    let _ = writer.wait_merging_threads();
                };
                Ok(())
            }
            Err(e) => Err(format!("Lock error: {}", e)),
        }
    }
}

impl Drop for IndexWriterBridge {
    fn drop(&mut self) {
        info!("IndexW has been dropped. index_path:[{}]", self.path);
    }
}





pub struct IndexWriterBridgeCache {
    cache: HashMap<String, Arc<IndexWriterBridge>>,
}

impl IndexWriterBridgeCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn get_index_writer_bridge(&self, key: String) -> Result<Arc<IndexWriterBridge>, String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        match pinned.get(&trimmed_key) {
            Some(result) => Ok(result.clone()),
            None => Err(format!(
                "Index Writer doesn't exist with given key: [{}]",
                trimmed_key
            )),
        }
    }

    pub fn set_index_writer_bridge(
        &self,
        key: String,
        value: Arc<IndexWriterBridge>,
    ) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.insert(trimmed_key.clone(), value.clone());
            warn!(
                "{}",
                format!(
                    "Index writer already exists with given key: [{}], it has been overwritten.",
                    trimmed_key
                )
            )
        } else {
            pinned.insert(trimmed_key, value.clone());
        }
        Ok(())
    }
    pub fn remove_index_writer_bridge(&self, key: String) -> Result<(), String> {
        let pinned = self.cache.pin();
        let trimmed_key: String = key.trim_end_matches('/').to_string();
        if pinned.contains_key(&trimmed_key) {
            pinned.remove(&trimmed_key);
        } else {
            let message = format!(
                "IndexWriterBridge doesn't exist, can't remove it with given key: [{}]",
                trimmed_key
            );
            debug!("{}", message)
        }
        Ok(())
    }
}
