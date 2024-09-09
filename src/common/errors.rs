use std::str::Utf8Error;
use thiserror::Error;

use crate::core::FileOperationError;



pub const PROCESS_CANCELLED_BY_SERVICE_MESSAGE: &str = "process cancelled by service";

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
