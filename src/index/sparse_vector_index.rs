// use std::borrow::Cow;
// use std::fs::{create_dir_all, remove_dir_all};
// use std::path::{Path, PathBuf};
// use std::sync::Arc;
// use std::sync::atomic::AtomicBool;
// use crate::core::inverted_index::{InvertedIndex, InvertedIndexBuilder};
// use crate::index::sparse_index_config::SparseIndexConfig;
// use atomic_refcell::AtomicRefCell;
// use crate::common::constants::{INVERTED_INDEX_CACHE, RAM_BUILDER_CACHE};
// use crate::core::common::types::ElementOffsetType;
// use crate::core::scores::ScoresMemoryPool;
// use crate::core::SearchContext;
// use crate::core::sparse_vector::{RemappedSparseVector, SparseVector};
// use crate::ffi::ScoredPointOffset;
//
// #[derive(Debug)]
// pub struct SparseVectorIndex<TInvertedIndex: InvertedIndex> {
//     config: SparseIndexConfig,
//     // vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
//     // payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
//     path: PathBuf,
//     inverted_index: TInvertedIndex,
//     // searches_telemetry: SparseSearchesTelemetry,
//     scores_memory_pool: ScoresMemoryPool,
// }
//
// // TODO impl 使用泛型的语法
// impl<TInvertedIndex: InvertedIndex> SparseVectorIndex<TInvertedIndex> {
//     pub fn config(&self) -> SparseIndexConfig { self.config }
//
//     pub fn inverted_index(&self) -> &TInvertedIndex { &self.inverted_index }
//
//     /// 创建 inverted index
//     /// 即初始化一个 ram builder 并放入缓存
//     fn create_inverted_index(
//         index_path: &str,
//     ) -> Result<(), String>
//     {
//         create_dir_all(index_path).map_err(|e|e.to_string())?;
//         let builder = InvertedIndexBuilder::new();
//         let _ = RAM_BUILDER_CACHE.push(index_path, builder)?;
//
//         Ok(())
//     }
//
//     /// 索引稀疏向量
//     /// 往缓存中的 ram_builder 添加稀疏向量
//     fn insert_sparse_vector(
//         index_path: &str,
//         row_id: u32,
//         sparse_vector: &RemappedSparseVector,
//     ) -> Result<(), String>
//     {
//         RAM_BUILDER_CACHE.update(&index_path, |builder| {
//             builder.add(row_id, sparse_vector.clone());
//         })
//     }
//
//     // TODO: Handel 异常处理
//     /// 将构建好的 inverted index 放入缓存
//     fn commit_inverted_index(
//         index_path: &Path,
//     ) -> Result<(), String>
//     {
//         let res = match RAM_BUILDER_CACHE.consume(index_path.to_str().unwrap(), |builder| {
//             let index =  TInvertedIndex::from_ram_index(Cow::Owned(builder.build()), index_path).expect("");
//             return index;
//         }) {
//             Ok(index) => {Ok(index)},
//             Err(e) => {Err(e)}
//         }?;
//         let _ = INVERTED_INDEX_CACHE.insert(index_path.to_str().unwrap(), InvertedIndexImpl::from(res))?;
//         Ok(())
//
//     }
//
//     /// 确保缓存中存在 inverted index
//     /// 可以视为一种保活策略
//     fn load_inverted_index(
//         index_path: &Path
//     ) -> Result<(), String>
//     {
//         let res = INVERTED_INDEX_CACHE.get(index_path.to_str().unwrap());
//         if res.is_ok() {
//             Ok(())
//         } else {
//             let inverted_index = TInvertedIndex::open(index_path).map_err(|e|e.to_string())?;
//             let s = InvertedIndexImpl::from(inverted_index);
//             INVERTED_INDEX_CACHE.insert(index_path.to_str().unwrap(), s)
//         }
//     }
//
//     pub fn search_plain(
//         &self,
//         sparse_vector: RemappedSparseVector,
//         filter: Option<&Vec<u8>>,
//         top: usize,
//     ) -> Result<Vec<ScoredPointOffset>, String> {
//         // TODO: filter 后续的使用
//         let memory_handle = self.scores_memory_pool.get();
//         let mut search_context = SearchContext::new(
//             sparse_vector,
//             top,
//             &self.inverted_index,
//             memory_handle,
//             &AtomicBool::default(),
//         );
//         // TODO 修改 filter 逻辑
//         let alive_ids:[ElementOffsetType] = (0u32..9999u32).collect();
//         Ok(search_context.plain_search(&alive_ids))
//     }
//
//     pub fn search(
//         &self,
//         vector: &SparseVector,
//         filter: Option<&Vec<u8>>,
//         top: usize
//     ) {
//
//     }
//
//     fn search_sparse(
//         &self,
//         sparse_vector: RemappedSparseVector,
//         filter: Option<&Vec<u8>>,
//         top: usize,
//     ) -> Vec<ScoredPointOffset> {
//         let is_stopped = AtomicBool::default();
//         let memory_handle = self.scores_memory_pool.get();
//         let mut search_context = SearchContext::new(
//             sparse_vector,
//             top,
//             &self.inverted_index,
//             memory_handle,
//             &is_stopped,
//         );
//         // TODO 修改 filter 逻辑
//         let f = |id: ElementOffsetType| true;
//         match filter {
//             Some(filter) => {
//                 search_context.search(&f)
//             }
//             None => search_context.search(&f),
//         }
//     }
//
// }