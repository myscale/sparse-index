use super::IndexSettings;
use super::{
    index_meta::{IndexMeta, SegmentMetaInventory},
    Index,
};
use crate::common::errors::SparseError;
use crate::common::executor::Executor;
use crate::directory::managed_directory::ManagedDirectory;
use crate::directory::mmap_directory::MmapDirectory;
use crate::directory::Directory;
use crate::indexer::segment_updater::save_metas;
// use crate::indexer::single_segment_index_writer::SingleSegmentIndexWriter;
use crate::core::StorageType;
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
        Self { index_settings: IndexSettings::default() }
    }

    pub fn with_settings(mut self, settings: IndexSettings) -> Self {
        self.index_settings = settings;
        self
    }

    /// not fully tested.
    pub fn create_in_ram(self) -> Result<Index, SparseError> {
        return Err(SparseError::Error("Currently ram mode is not supported!".to_string()));
        // debug_assert_eq!(self.index_settings.inverted_index_config.storage_type, StorageType::Ram);

        // let ram_directory = RamDirectory::create();
        // self.create(ram_directory)
    }

    /// Create mmap index in given directory.
    pub fn create_in_dir<P: AsRef<Path>>(self, directory_path: P) -> crate::Result<Index> {
        debug_assert_ne!(self.index_settings.inverted_index_config.storage_type, StorageType::Ram);

        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::open(directory_path)?);
        if Index::exists(&*mmap_directory)? {
            return Err(SparseError::IndexAlreadyExists);
        }
        self.create(mmap_directory)
    }

    // This function has not been tested.
    // pub fn single_segment_index_writer(
    //     self,
    //     dir: impl Into<Box<dyn Directory>>,
    //     mem_budget: usize,
    // ) -> crate::Result<SingleSegmentIndexWriter> {
    //     debug_assert_eq!(
    //         self.index_settings.config.storage_type, StorageType::Mmap
    //     );
    //     let index = self.create(dir)?;
    //     let index_simple_writer = SingleSegmentIndexWriter::new(index, mem_budget)?;
    //     Ok(index_simple_writer)
    // }

    /// When [`Index`] is destroyed, the `tempdir` will be removed.
    pub fn create_from_tempdir(self) -> crate::Result<Index> {
        debug_assert_eq!(self.index_settings.inverted_index_config.storage_type, StorageType::Mmap);
        let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::create_from_tempdir()?);
        self.create(mmap_directory)
    }

    /// Create index with given [`Directory`]
    pub(super) fn create<T: Into<Box<dyn Directory>>>(self, dir: T) -> crate::Result<Index> {
        let directory: Box<dyn Directory> = dir.into();
        let managed_directory: ManagedDirectory = ManagedDirectory::wrap(directory)?;
        // persist index settings.
        let _ = self.index_settings.save(&managed_directory.get_path().unwrap())?;
        // save index metas.
        save_metas(&IndexMeta::default(), &managed_directory)?;
        managed_directory.sync_directory()?;

        Ok(Index { directory: managed_directory, index_settings: self.index_settings, executor: Arc::new(Executor::single_thread()), inventory: SegmentMetaInventory::default() })
    }
}
