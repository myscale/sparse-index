use crate::common::errors::{DataCorruption, SparseError};
use crate::common::executor::Executor;
use crate::directory::error::OpenReadError;
use crate::directory::managed_directory::ManagedDirectory;
use crate::directory::mmap_directory::MmapDirectory;
use crate::directory::INDEX_WRITER_LOCK;
use crate::indexer::index_writer::{MAX_NUM_THREAD, MEMORY_BUDGET_NUM_BYTES_MIN};
use log::{error, info};
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::indexer::IndexWriter;
use crate::reader::{IndexReader, IndexReaderBuilder};
use crate::sparse_index::SparseIndexConfig;
use crate::{directory::Directory, META_FILEPATH};
use crate::{RowId, INDEX_CONFIG_FILEPATH};

use super::index_meta::{IndexMeta, SegmentMetaInventory};
use super::{IndexBuilder, IndexSettings, Segment, SegmentId, SegmentMeta};

/// 根据 directory 读取当前索引目录下的 meta.json 文件 </br>
/// 将未被追踪的 `UntrackedIndexMeta` 转换为 `IndexMeta` (tracked by inventory) </br>
/// 返回 `IndexMeta` 对象
fn load_metas(
    directory: &dyn Directory,
    inventory: &SegmentMetaInventory,
) -> crate::Result<IndexMeta> {
    let meta_data = directory.atomic_read(&META_FILEPATH)?;
    let meta_string = String::from_utf8(meta_data).map_err(|_utf8_err| {
        error!("Meta data is not valid utf8.");
        DataCorruption::new(
            META_FILEPATH.to_path_buf(),
            "Meta file does not contain valid utf8 file.".to_string(),
        )
    })?;
    IndexMeta::deserialize(&meta_string, inventory)
        .map_err(|e| {
            DataCorruption::new(
                META_FILEPATH.to_path_buf(),
                format!("Meta file cannot be deserialized. {e:?}. Content: {meta_string:?}"),
            )
        })
        .map_err(From::from)
}

/// Sparse Index 入口位置
#[derive(Clone)]
pub struct Index {
    /// directory 负责进行文件 IO
    pub(super) directory: ManagedDirectory,
    /// SparseIndex 的配置文件
    pub(super) index_settings: IndexSettings,
    /// 用于搜索的线程池
    pub(super) executor: Arc<Executor>,
    /// 追踪 SegmentMeta 的仓库
    pub(super) inventory: SegmentMetaInventory,
}

/// For `Search Executor`
/// User can set the thread pool to be used for searching
impl Index {
    /// 获取 search executor
    pub fn search_executor(&self) -> &Executor {
        self.executor.as_ref()
    }

    /// 使用多线程搜索
    pub fn set_multithread_executor(&mut self, num_threads: usize) -> crate::Result<()> {
        self.executor = Arc::new(Executor::multi_thread(num_threads, "sparse-search-")?);
        Ok(())
    }

    /// 使用共享线程池
    pub fn set_shared_multithread_executor(
        &mut self,
        shared_thread_pool: Arc<Executor>,
    ) -> crate::Result<()> {
        self.executor = shared_thread_pool.clone();
        Ok(())
    }

    /// 默认 1 个 Index 对应 cpu logic number 数量的线程池
    pub fn set_default_multithread_executor(&mut self) -> crate::Result<()> {
        let default_num_threads = num_cpus::get();
        self.set_multithread_executor(default_num_threads)
    }
}

/// create, write.
impl Index {
    /// 创建 IndexBuilder
    // TODO 删除 builder() 函数
    pub fn builder() -> IndexBuilder {
        IndexBuilder::new()
    }

    /// 提供 自定义的 Directory 和 settings
    pub fn create<T: Into<Box<dyn Directory>>>(
        dir: T,
        settings: IndexSettings,
    ) -> crate::Result<Index> {
        let dir: Box<dyn Directory> = dir.into();
        let builder: IndexBuilder = IndexBuilder::new();
        builder.with_settings(settings).create(dir)
    }

    /// 在给定路径下以 mmap 模式创建索引
    pub fn create_in_dir<P: AsRef<Path>>(
        directory_path: P,
        settings: IndexSettings,
    ) -> crate::Result<Index> {
        IndexBuilder::new()
            .with_settings(settings)
            .create_in_dir(directory_path)
    }

    /// 创建一个新的 segment_meta（仅限高级用户）。
    ///
    /// 只要 `SegmentMeta` 存在，与 `SegmentMeta` 关联的文件就保证不会被垃圾回收，
    /// 无论该段是否被记录为索引的一部分。
    pub fn new_segment_meta(&self, segment_id: SegmentId, rows_count: RowId) -> SegmentMeta {
        self.inventory
            .new_segment_meta(self.directory().get_path(), segment_id, rows_count)
    }

    /// 打开一个新的索引写入器。尝试获取一个锁文件。
    ///
    /// 锁文件应该在结束时删除，但可能由于程序崩溃或其他错误，导致一个过期的锁文件留在索引目录中。如果确定系统上没有其他 `IndexWriter` 正在访问索引目录，手动删除锁文件是安全的。
    ///
    /// - `num_threads` 定义了同时工作的索引工作线程数。
    /// - `overall_memory_budget_in_bytes` 设置为所有索引线程分配的内存量。
    /// 每个线程将分配到 `overall_memory_budget_in_bytes / num_threads` 的内存预算。
    ///
    /// # 错误
    /// 如果锁文件已经存在，返回 `Error::DirectoryLockBusy` 或 `Error::IoError`。
    /// 如果每个线程的内存空间太小或太大，返回 `SparseError::InvalidArgument`。
    pub fn writer_with_num_threads(
        &self,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> crate::Result<IndexWriter> {
        let directory_lock =
            self.directory
                .acquire_lock(&INDEX_WRITER_LOCK)
                .map_err(|err| {
                    SparseError::LockFailure(
                    err,
                    Some(
                        "Failed to acquire index lock. If you are using a regular directory, this \
                         means there is already an `IndexWriter` working on this `Directory`, in \
                         this process or in a different process.".to_string(),
                    ),
                )
                })?;
        let memory_arena_in_bytes_per_thread = overall_memory_budget_in_bytes / num_threads;

        IndexWriter::new(
            self,
            num_threads,
            memory_arena_in_bytes_per_thread,
            directory_lock,
        )
    }

    /// 内存预算 15MB, 仅用来测试
    #[cfg(test)]
    pub fn writer_for_tests(&self) -> crate::Result<IndexWriter> {
        self.writer_with_num_threads(1, MEMORY_BUDGET_NUM_BYTES_MIN)
    }

    /// 创建一个多线程写入器。
    ///
    /// Sparse 会自动定义要使用的线程数，但不会超过 8 个线程。
    /// `overall_memory_arena_in_bytes` 是将在一定数量的线程之间分配的总目标内存使用量。
    ///
    /// # 错误
    /// 如果锁文件已经存在，返回 `Error::FileAlreadyExists`。
    /// 如果每个线程的内存空间太小或太大，返回 `TantivyError::InvalidArgument`。
    pub fn writer(&self, memory_budget_in_bytes: usize) -> crate::Result<IndexWriter> {
        let mut num_threads = std::cmp::min(num_cpus::get(), MAX_NUM_THREAD);
        let memory_budget_num_bytes_per_thread = memory_budget_in_bytes / num_threads;
        if memory_budget_num_bytes_per_thread < MEMORY_BUDGET_NUM_BYTES_MIN {
            num_threads = (memory_budget_in_bytes / MEMORY_BUDGET_NUM_BYTES_MIN).max(1);
        }

        info!("create index writer, num threads is {:?}", num_threads);
        self.writer_with_num_threads(num_threads, memory_budget_in_bytes)
    }

    /// Creates a new segment.
    pub fn new_segment(&self) -> Segment {
        let segment_meta = self.inventory.new_segment_meta(
            self.directory().get_path(),
            SegmentId::generate_random(),
            0,
        );
        self.segment(segment_meta)
    }
}

/// metrics
impl Index {
    pub fn index_settings(&self) -> IndexSettings {
        return self.index_settings.clone();
    }

    #[doc(hidden)]
    pub fn segment(&self, segment_meta: SegmentMeta) -> Segment {
        Segment::for_index(self.clone(), segment_meta)
    }

    /// 返回可以被搜索的 Segments
    pub fn searchable_segments(&self) -> crate::Result<Vec<Segment>> {
        Ok(self
            .searchable_segment_metas()?
            .into_iter()
            .map(|segment_meta| self.segment(segment_meta))
            .collect())
    }

    /// 通过读取 `meta.json` 文件获取当前 Index 的 `SegmentMeta` 列表(从最后一次 commit 开始)
    pub fn searchable_segment_metas(&self) -> crate::Result<Vec<SegmentMeta>> {
        Ok(self.load_metas()?.segments)
    }

    /// 返回可以被搜索的 segment ids
    pub fn searchable_segment_ids(&self) -> crate::Result<Vec<SegmentId>> {
        Ok(self
            .searchable_segment_metas()?
            .iter()
            .map(SegmentMeta::id)
            .collect())
    }

    /// 列出所有的 segment metas，这些 segment meta 可能是正在 build 或者是正在 merge 中
    pub(crate) fn list_all_segment_metas(&self) -> Vec<SegmentMeta> {
        self.inventory.all()
    }

    /// 返回当前 Index 使用的 directory
    pub fn directory(&self) -> &ManagedDirectory {
        &self.directory
    }

    /// 返回当前 Index 使用的 mutable 类型的 directory
    pub fn directory_mut(&mut self) -> &mut ManagedDirectory {
        &mut self.directory
    }

    /// 判断 meta.json 文件是否存在
    pub fn exists(dir: &dyn Directory) -> Result<bool, OpenReadError> {
        dir.exists(&META_FILEPATH)
    }

    /// Returns the set of corrupted files
    pub fn validate_checksum(&self) -> crate::Result<HashSet<PathBuf>> {
        // 列出管理目录中所有的文件
        let managed_files = self.directory.list_managed_files();

        // 获取所有可搜索的 segment 文件，并将其收集到一个 HashSet 中
        let active_segments_files: HashSet<PathBuf> = self
            .searchable_segment_metas()?
            .iter()
            .flat_map(|segment_meta| segment_meta.list_files())
            .collect();

        // 找到同时存在于管理文件和 segments 中的文件
        let active_existing_files: HashSet<&PathBuf> =
            active_segments_files.intersection(&managed_files).collect();

        let mut damaged_files = HashSet::new();
        for path in active_existing_files {
            if !self.directory.validate_checksum(path)? {
                damaged_files.insert((*path).clone());
            }
        }
        Ok(damaged_files)
    }
}

/// open, read, load index.
impl Index {
    /// Open the index using the provided directory
    pub fn open<T: Into<Box<dyn Directory>>>(directory: T) -> crate::Result<Index> {
        let directory: Box<dyn Directory> = directory.into();
        let directory: ManagedDirectory = ManagedDirectory::wrap(directory)?;

        // 加载 IndexMeta, 初始化 IndexMeta 的仓库 inventory
        let inventory: SegmentMetaInventory = SegmentMetaInventory::default();
        let _metas: IndexMeta = load_metas(&directory, &inventory)?;

        // 加载 sparse index 配置文件
        let _data: Vec<u8> = directory.atomic_read(&INDEX_CONFIG_FILEPATH)?;
        let _index_config_str: Cow<'_, str> = String::from_utf8_lossy(&_data);
        let index_config: SparseIndexConfig = serde_json::from_str(&_index_config_str)?;
        let index_settings = IndexSettings {
            config: index_config,
        };

        Ok(Index {
            directory,
            index_settings,
            executor: Arc::new(Executor::single_thread()),
            inventory,
        })
    }

    /// 加载 Index Reader
    pub fn reader(&self) -> crate::Result<IndexReader> {
        self.reader_builder().try_into()
    }
    /// Reads the index meta file from the directory.
    pub fn load_metas(&self) -> crate::Result<IndexMeta> {
        load_metas(self.directory(), &self.inventory)
    }

    /// Create a [`IndexReader`] for the given index.
    ///
    /// Most project should create at most one reader for a given index.
    /// This method is typically called only once per `Index` instance.
    pub fn reader_builder(&self) -> IndexReaderBuilder {
        IndexReaderBuilder::new(self.clone())
    }

    /// 使用 mmap 方式打开 index
    // TODO #[cfg(feature = "mmap")]
    pub fn open_in_dir<P: AsRef<Path>>(directory_path: P) -> crate::Result<Index> {
        let mmap_directory = MmapDirectory::open(directory_path)?;
        Index::open(mmap_directory)
    }
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index({:?})", self.directory)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, time::Instant};

    use log::info;
    use tempfile::TempDir;

    use crate::{
        core::{SparseRowContent, SparseVector},
        index::{IndexBuilder, IndexSettings},
        indexer::{index_writer, LogMergePolicy, MergePolicy, NoMergePolicy},
    };

    use super::Index;

    use rand::Rng;

    fn generate_random_vectors(
        len: usize,
        dim_range: u32,
        value_range: f32,
    ) -> (Vec<u32>, Vec<f32>) {
        let mut rng = rand::thread_rng();

        let random_dims: Vec<u32> = (0..len).map(|_| rng.gen_range(0..dim_range)).collect();
        let random_values: Vec<f32> = (0..len).map(|_| rng.gen_range(0.0..value_range)).collect();

        (random_dims, random_values)
    }

    fn mock_row_content(base: u32, rows: u32) -> impl Iterator<Item = SparseRowContent> {
        (base * rows..base * rows + rows).map(|i| {
            // max_dim 1024 维
            let indices = (0..384).map(|j| (i + j) % 2048).collect();
            let values = (0..384).map(|j| 0.1 + ((i + j) / 16) as f32).collect();

            SparseRowContent {
                row_id: i,
                sparse_vector: SparseVector { indices, values },
            }
        })
    }

    fn get_logger() -> env_logger::Builder {
        // 创建一个新的日志构建器
        let mut builder = env_logger::Builder::from_default_env();

        // 设置日志级别为 Debug
        builder.filter(None, log::LevelFilter::Info);

        return builder;
    }

    #[test]
    pub fn test_create_index() {
        get_logger().init();
        // let dir = TempDir::new().expect("error create temp dir");
        let dir2 = Path::new("/home/mochix/test/sparse_index_files/temp");
        let index = Index::create_in_dir(dir2, IndexSettings::default())
            .expect("error create index in dir");
        let mut index_writer = index
            .writer(1024 * 1024 * 128)
            .expect("error create index writer");

        let mut log_merge_policy = LogMergePolicy::default();
        // log_merge_policy.set_max_docs_before_merge(5);
        index_writer.set_merge_policy(Box::new(log_merge_policy));
        // index_writer.set_merge_policy(Box::new(NoMergePolicy::default()));

        let time_begin = Instant::now();
        for base in 0..1 {
            for row in mock_row_content(base, 100000) {
                let res = index_writer.add_document(row);
            }
            let commit_res = index_writer.commit();
            info!(
                "[BASE-{}] commit res opstamp is: {:?}",
                base,
                commit_res.unwrap()
            );
        }

        let res = index_writer.wait_merging_threads();
        let time_end = Instant::now();
        info!(
            "release merging threads is {}, duration is {}s",
            res.is_ok(),
            time_end.duration_since(time_begin).as_secs()
        );

        let searcher = index.reader().expect("error index reader").searcher();
        for row in mock_row_content(5, 100) {
            let res = searcher.search(row.sparse_vector, 4).expect("error search");
            info!("RES: {:?}", res);
        }
    }
}
