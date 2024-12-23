use std::{fmt, io};
use std::{path::PathBuf, str::Utf8Error, sync::Arc};
use thiserror::Error;

use crate::core::{InvertedIndexError, PostingListError};
use crate::{
    core::FileOperationError,
    directory::error::{
        Incompatibility, LockError, OpenDirectoryError, OpenReadError, OpenWriteError,
    },
};

#[derive(Debug, Error)]
pub enum SparseIndexLibError {
    #[error(transparent)]
    FileOperationError(#[from] FileOperationError),

    #[error(transparent)]
    CxxConvertError(#[from] CxxConvertError),
}

#[derive(Debug, Clone, Error)]
#[allow(dead_code)]
pub enum CxxConvertError {
    #[error("Failed to convert cxx vector variable. '{0}'")]
    CxxVectorConvertError(String),
    #[error("Failed to convert cxx element variable. '{0}'")]
    CxxElementConvertError(String),
    #[error("Failed to convert CxxString to Rust String: {0}")]
    Utf8Error(#[from] Utf8Error),
}

/// Represents a `DataCorruption` error.
///
/// When facing data corruption, tantivy actually panics or returns this error.
#[derive(Clone)]
pub struct DataCorruption {
    filepath: Option<PathBuf>,
    comment: String,
}

impl DataCorruption {
    /// Creates a `DataCorruption` Error.
    pub fn new(filepath: PathBuf, comment: String) -> DataCorruption {
        DataCorruption { filepath: Some(filepath), comment }
    }

    /// Creates a `DataCorruption` Error, when the filepath is irrelevant.
    pub fn comment_only<TStr: ToString>(comment: TStr) -> DataCorruption {
        DataCorruption { filepath: None, comment: comment.to_string() }
    }
}

impl fmt::Debug for DataCorruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Data corruption")?;
        if let Some(ref filepath) = &self.filepath {
            write!(f, " (in file `{filepath:?}`)")?;
        }
        write!(f, ": {}.", self.comment)?;
        Ok(())
    }
}

// /// Type of index incompatibility between the library and the index found on disk
// /// Used to catch and provide a hint to solve this incompatibility issue
// #[derive(Clone)]
// pub enum Incompatibility {
//     /// This library cannot decompress the index found on disk
//     CompressionMismatch {
//         /// Compression algorithm used by the current version of tantivy
//         library_compression_format: String,
//         /// Compression algorithm that was used to serialise the index
//         index_compression_format: String,
//     },
//     /// The index format found on disk isn't supported by this version of the library
//     IndexMismatch {
//         /// Version used by the library
//         library_version: Version,
//         /// Version the index was built with
//         index_version: Version,
//     },
// }

// impl fmt::Debug for Incompatibility {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
//         match self {
//             Incompatibility::CompressionMismatch {
//                 library_compression_format,
//                 index_compression_format,
//             } => {
//                 let err = format!(
//                     "Library was compiled with {library_compression_format:?} compression, index \
//                      was compressed with {index_compression_format:?}"
//                 );
//                 let advice = format!(
//                     "Change the feature flag to {index_compression_format:?} and rebuild the \
//                      library"
//                 );
//                 write!(f, "{err}. {advice}")?;
//             }
//             Incompatibility::IndexMismatch {
//                 library_version,
//                 index_version,
//             } => {
//                 let err = format!(
//                     "Library version: {}, index version: {}",
//                     library_version.index_format_version, index_version.index_format_version
//                 );
//                 // TODO make a more useful error message
//                 // include the version range that supports this index_format_version
//                 let advice = format!(
//                     "Change tantivy to a version compatible with index format {} (e.g. {}.{}.x) \
//                      and rebuild your project.",
//                     index_version.index_format_version, index_version.major, index_version.minor
//                 );
//                 write!(f, "{err}. {advice}")?;
//             }
//         }

//         Ok(())
//     }
// }

/// Error while trying to acquire a directory [lock](crate::directory::Lock).
///
/// This is returned from [`Directory::acquire_lock`](crate::Directory::acquire_lock).
// #[derive(Debug, Clone, Error)]
// pub enum LockError {
//     /// Failed to acquired a lock as it is already held by another
//     /// client.
//     /// - In the context of a blocking lock, this means the lock was not released within some
//     ///   `timeout` period.
//     /// - In the context of a non-blocking lock, this means the lock was busy at the moment of the
//     ///   call.
//     #[error("Could not acquire lock as it is already held, possibly by a different process.")]
//     LockBusy,
//     /// Trying to acquire a lock failed with an `IoError`
//     #[error("Failed to acquire the lock due to an io:Error.")]
//     IoError(Arc<io::Error>),
// }

// impl LockError {
//     /// Wraps an io error.
//     pub fn wrap_io_error(io_error: io::Error) -> Self {
//         Self::IoError(Arc::new(io_error))
//     }
// }
// use futures::Error as FuturesIoError;

/// The library's error enum
#[derive(Debug, Error)]
pub enum SparseError {
    /// IO Error.
    #[error("An IO error occurred: '{0}'")]
    IoError(Arc<io::Error>),
    // #[error("A futures-io error occurred: '{0}'")]
    // FuturesIoError(#[from] FuturesIoError),
    /// Failed to open the directory.
    #[error("Failed to open the directory: '{0:?}'")]
    OpenDirectoryError(#[from] OpenDirectoryError),
    /// Data corruption.
    #[error("Data corrupted: '{0:?}'")]
    DataCorruption(DataCorruption),
    /// Index already exists in this directory.
    #[error("Index already exists")]
    IndexAlreadyExists,
    /// Index incompatible with current version of Tantivy.
    #[error("{0:?}")]
    IncompatibleIndex(Incompatibility),
    /// Failed to acquire file lock.
    #[error("Failed to acquire Lockfile: {0:?}. {1:?}")]
    LockFailure(LockError, Option<String>),
    /// Invalid argument was passed by the user.
    #[error("An invalid argument was passed: '{0}'")]
    InvalidArgument(String),
    /// An internal error occurred. This is are internal states that should not be reached.
    /// e.g. a datastructure is incorrectly inititalized.
    #[error("Internal error: '{0}'")]
    InternalError(String),
    /// An Error occurred in one of the threads.
    #[error("An error occurred in a thread: '{0}'")]
    ErrorInThread(String),
    /// System error. (e.g.: We failed spawning a new thread).
    #[error("System error.'{0}'")]
    SystemError(String),
    /// Failed to open a file for read.
    #[error("Failed to open file for read: '{0:?}'")]
    OpenReadError(#[from] OpenReadError),
    /// Failed to open a file for write.
    #[error("Failed to open file for write: '{0:?}'")]
    OpenWriteError(#[from] OpenWriteError),
    /// A thread holding the locked panicked and poisoned the lock.
    #[error("A thread holding the locked panicked and poisoned the lock")]
    Poisoned,

    #[error("'{0:?}'")]
    FileOperationError(#[from] FileOperationError),
    #[error("'{0}'")]
    Error(String),

    #[error("'{0:?}'")]
    PostingListError(#[from] PostingListError),

    #[error("'{0:?}'")]
    InvertedIndexError(#[from] InvertedIndexError),
}

impl From<io::Error> for SparseError {
    fn from(io_err: io::Error) -> SparseError {
        SparseError::IoError(Arc::new(io_err))
    }
}
impl From<rayon::ThreadPoolBuildError> for SparseError {
    fn from(error: rayon::ThreadPoolBuildError) -> SparseError {
        SparseError::SystemError(error.to_string())
    }
}

impl From<DataCorruption> for SparseError {
    fn from(data_corruption: DataCorruption) -> SparseError {
        SparseError::DataCorruption(data_corruption)
    }
}
impl From<LockError> for SparseError {
    fn from(lock_error: LockError) -> SparseError {
        SparseError::LockFailure(lock_error, None)
    }
}

impl From<serde_json::Error> for SparseError {
    fn from(serde_error: serde_json::Error) -> SparseError {
        SparseError::SystemError(serde_error.to_string())
    }
}

impl From<String> for SparseError {
    fn from(value: String) -> Self {
        SparseError::Error(value)
    }
}
