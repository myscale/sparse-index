use std::io::BufWriter;
use std::path::PathBuf;

pub mod directory;
pub mod directory_lock;
pub mod error;
pub mod file_watcher;
pub mod footer;
pub mod managed_directory;
pub mod mmap_directory;
pub mod ram_directory;
pub mod watch_event_router;

pub use common::file_slice::{FileHandle, FileSlice};
pub use common::{AntiCallToken, OwnedBytes, TerminatingWrite};

pub use self::directory::{Directory, DirectoryClone, DirectoryLock};
pub use self::directory_lock::{Lock, INDEX_WRITER_LOCK, META_LOCK};
pub use self::watch_event_router::{WatchCallback, WatchCallbackList, WatchHandle};

/// Write object for Directory.
///
/// `WritePtr` are required to implement both Write
/// and Seek.
pub type WritePtr = BufWriter<Box<dyn TerminatingWrite>>;

/// Outcome of the Garbage collection
pub struct GarbageCollectionResult {
    /// List of files that were deleted in this cycle
    pub deleted_files: Vec<PathBuf>,
    /// List of files that were schedule to be deleted in this cycle,
    /// but deletion did not work. This typically happens on windows,
    /// as deleting a memory mapped file is forbidden.
    ///
    /// If a searcher is still held, a file cannot be deleted.
    /// This is not considered a bug, the file will simply be deleted
    /// in the next GC.
    pub failed_to_delete_files: Vec<PathBuf>,
}
