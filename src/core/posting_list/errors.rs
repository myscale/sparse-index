use thiserror::Error;

use crate::directory::error;

#[derive(Debug, Error)]
pub enum PostingListError {
    #[error("A merge error happened: '{0}'")]
    MergeError(String),

    #[error("Invalid parameter: '{0}'")]
    InvalidParameter(String),

    #[error("Duplicated row_id: '{0}'")]
    DuplicatedRowId(String),

    #[error("Convert error: '{0}'")]
    TypeConvertError(String),

    #[error("UncompressError: '{0}")]
    UncompressError(String),

    #[error("LogicError: '{0}")]
    LogicError(String),
}
