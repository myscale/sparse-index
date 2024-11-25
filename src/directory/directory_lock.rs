use std::path::PathBuf;

use once_cell::sync::Lazy;

/// A directory lock.
///
/// A lock is associated with a specific path.
///
/// The lock will be passed to [`Directory::acquire_lock`](crate::Directory::acquire_lock).
///
/// Tantivy itself uses only two locks but client application
/// can use the directory facility to define their own locks.
/// - [`INDEX_WRITER_LOCK`]
/// - [`META_LOCK`]
///
/// Check out these locks documentation for more information.
#[derive(Debug)]
pub struct Lock {
    /// The lock needs to be associated with its own file `path`.
    /// Depending on the platform, the lock might rely on the creation
    /// and deletion of this filepath.
    pub filepath: PathBuf,
    /// `is_blocking` describes whether acquiring the lock is meant
    /// to be a blocking operation or a non-blocking.
    ///
    /// Acquiring a blocking lock blocks until the lock is
    /// available.
    ///
    /// Acquiring a non-blocking lock returns rapidly, either successfully
    /// or with an error signifying that someone is already holding
    /// the lock.
    pub is_blocking: bool,
}

/// Only one process should be able to write tantivy's index at a time.
/// This lock file, when present, is in charge of preventing other processes to open an
/// `IndexWriter`.
///
/// If the process is killed and this file remains, it is safe to remove it manually.
///
/// Failing to acquire this lock usually means a misuse of tantivy's API,
/// (creating more than one instance of the `IndexWriter`), are a spurious
/// lock file remaining after a crash. In the latter case, removing the file after
/// checking no process running tantivy is running is safe.
pub static INDEX_WRITER_LOCK: Lazy<Lock> =
    Lazy::new(|| Lock { filepath: PathBuf::from(".sparse-index-writer.lock"), is_blocking: false });

/// The meta lock file is used to protect the segment files being opened by
/// `IndexReader::reload()` from being garbage collected.
/// It allows another process to safely access our index while it is being written.
/// Ideally, we would prefer `RWLock` semantics here, but achieving this on Windows
/// is challenging.
///
/// Opening segment readers is a very fast process.
///
/// In SparseIndex, when `IndexReader` reloads using `reload()`, it needs to open segment files.
/// During this process, garbage collection (GC) may be running and deleting
/// some unused segment files.
/// This could lead to `IndexReader` attempting to open segment files that are being deleted,
/// resulting in unpredictable issues.
///
/// To avoid this problem, a META_LOCK is introduced. When `IndexReader` reloads,
/// it will attempt to acquire the META_LOCK.
/// Similarly, the META_LOCK must also be acquired when GC begins executing.
pub static META_LOCK: Lazy<Lock> =
    Lazy::new(|| Lock { filepath: PathBuf::from(".sparse-index-meta.lock"), is_blocking: true });
