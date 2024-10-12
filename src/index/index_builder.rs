use super::IndexSettings;
use super::{
    index_meta::{IndexMeta, SegmentMetaInventory},
    Index,
};
use crate::common::errors::SparseError;
use crate::common::executor::Executor;
use crate::directory::managed_directory::ManagedDirectory;
use crate::directory::mmap_directory::MmapDirectory;
use crate::directory::ram_directory::RamDirectory;
use crate::directory::Directory;
use crate::indexer::segment_updater::save_metas;
use crate::indexer::single_segment_index_writer::SingleSegmentIndexWriter;
use crate::sparse_index::SparseIndexType;
use std::path::Path;
use std::sync::Arc;

pub struct IndexBuilder {
    index_settings: IndexSettings,
}
impl Default for IndexBuilder {
    fn default() -> Self {
        IndexBuilder::new()
    }
}
impl IndexBuilder {
    /// Creates a new `IndexBuilder` with default index config.
    pub fn new() -> Self {
        Self {
            index_settings: IndexSettings::default(),
        }
    }

    pub fn settings(mut self, settings: IndexSettings) -> Self {
        self.index_settings = settings;
        self
    }

    /// 使用内存模式创建索引
    pub fn create_in_ram(self) -> Result<Index, SparseError> {
        // 需要 index 类型为 ram
        assert_eq!(
            self.index_settings.config.index_type,
            SparseIndexType::MutableRam
        );

        let ram_directory = RamDirectory::create();
        self.create(ram_directory)
    }

    /// 使用 mmap 模式在路径下创建索引，如果存在之前的索引则报错
    // TODO #[cfg(feature = "mmap")]
    pub fn create_in_dir<P: AsRef<Path>>(self, directory_path: P) -> crate::Result<Index> {
        // 需要 index 类型是 mmap
        assert_eq!(self.index_settings.config.index_type, SparseIndexType::Mmap);

        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::open(directory_path)?);
        if Index::exists(&*mmap_directory)? {
            return Err(SparseError::IndexAlreadyExists);
        }
        self.create(mmap_directory)
    }

    /// 该函数未经过测试 </br>
    /// 它期望给一个空的目录，并且不会创建任何的垃圾回收操作。
    pub fn single_segment_index_writer(
        self,
        dir: impl Into<Box<dyn Directory>>,
        mem_budget: usize,
    ) -> crate::Result<SingleSegmentIndexWriter> {
        // 默认用 mmap
        assert_eq!(self.index_settings.config.index_type, SparseIndexType::Mmap);

        let index = self.create(dir)?;
        let index_simple_writer = SingleSegmentIndexWriter::new(index, mem_budget)?;
        Ok(index_simple_writer)
    }

    /// 在 Index 对象销毁之后，这个 temp 路径也会被删除，函数用来测试 MmapDirectory
    // TODO #[cfg(feature = "mmap")]
    pub fn create_from_tempdir(self) -> crate::Result<Index> {
        // 默认使用 mmap
        assert_eq!(self.index_settings.config.index_type, SparseIndexType::Mmap);
        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::create_from_tempdir()?);
        self.create(mmap_directory)
    }

    /// 在给定的 [`Directory`] 实现下创建索引
    pub(super) fn create<T: Into<Box<dyn Directory>>>(self, dir: T) -> crate::Result<Index> {
        let directory: Box<dyn Directory> = dir.into();
        let managed_directory: ManagedDirectory = ManagedDirectory::wrap(directory)?;

        save_metas(&IndexMeta::default(), &managed_directory)?;
        managed_directory.sync_directory()?;

        // let mut metas = IndexMeta::default();

        Ok(Index {
            directory: managed_directory,
            index_settings: self.index_settings,
            executor: Arc::new(Executor::single_thread()),
            inventory: SegmentMetaInventory::default(),
        })
    }
}
