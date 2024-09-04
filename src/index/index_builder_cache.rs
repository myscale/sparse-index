use std::sync::{Arc, RwLock, RwLockWriteGuard};
use flurry::HashMap;
use crate::core::common::StorageVersion;
use crate::core::inverted_index::{InvertedIndex, InvertedIndexBuilder};
use crate::core::posting_list::PostingListIter;

pub struct SparseIndexRamBuilderCache {
    cache: HashMap<String, Arc<RwLock<InvertedIndexBuilder>>>,
}

impl SparseIndexRamBuilderCache {
    pub fn new() -> Self {
        Self { cache: HashMap::new() }
    }

    pub fn push(&self, index_path: &str, ram_builder: InvertedIndexBuilder) -> Result<(), String>
    {
        let trimmed_path = index_path.trim_end_matches("/").to_string();
        let pinned = self.cache.pin();
        if pinned.contains_key(&trimmed_path) {
            pinned.insert(trimmed_path.clone(), Arc::new(RwLock::new(ram_builder)));
            println!("[Warning] {} already exists, rewrite it.", trimmed_path);
        } else {
            pinned.insert(trimmed_path, Arc::new(RwLock::new(ram_builder)));
        }
        // let mut index_map: HashMap<String, Box<dyn InvertedIndex<...>>> = HashMap::new();

        Ok(())
    }

    pub fn update(&self, index_path: &str, update_func: impl FnOnce(&mut InvertedIndexBuilder)) -> Result<(), String>
    {
        let trimmed_path = index_path.trim_end_matches("/").to_string();
        let pinned = self.cache.pin();
        match pinned.get(&trimmed_path) {
            None => {
                Err(format!("can't get, ram builder not exists, index_path is {}", trimmed_path))
            }
            Some(res) => {
                // TODO 将 PoisonError 传递回去 (每次转换为 String 太不优雅了)
                let mut builder = res.write().map_err(|e| e.to_string())?;
                update_func(&mut builder);
                Ok(())
            }
        }
    }

    pub fn consume<T>(&self, index_path: &str, update_func: impl FnOnce(InvertedIndexBuilder)-> T) -> Result<T, String>
    {
        let trimmed_path = index_path.trim_end_matches("/").to_string();
        let pinned = self.cache.pin();
        match pinned.remove(&trimmed_path) {
            None => {
                Err(format!("Can't consume, ram builder not exists, index_path is {}", trimmed_path))
            }
            Some(res) => { // res 是 &Arc<RwLock<InvertedIndexBuilder>> 类型
                let mut builder: RwLockWriteGuard<InvertedIndexBuilder> = res.write().map_err(|e| e.to_string())?;
                // TODO: std::mem::replace 返回了原始的 builder，并使用新值替换掉旧的
                let origin_builder = std::mem::replace(&mut *builder, InvertedIndexBuilder::new());
                Ok(update_func(origin_builder))
            }
        }
    }


    pub fn remove(&self, index_path: &str) -> Result<(), String>
    {
        let trimmed_path = index_path.trim_end_matches("/").to_string();
        let pinned = self.cache.pin();
        if pinned.contains_key(&trimmed_path) {
            pinned.remove(&trimmed_path);
        } else {
            return Err(format!("can't remove, ram builder not exists, index_path is {}", trimmed_path));
        }
        Ok(())
    }
}