use thiserror::Error;

use crate::core::PostingListError;

#[derive(Debug, Error)]
pub enum InvertedIndexError {
    #[error("Invalid InvertedIndexConfig: '{0}'")]
    InvalidIndexConfig(String),

    #[error("Can't add sparse_vector to index: '{0}'")]
    AddError(String),

    #[error("Invalid parameter: '{0}'")]
    InvalidParameter(String),
}

impl From<PostingListError> for InvertedIndexError {
    fn from(error: PostingListError) -> Self {
        InvertedIndexError::AddError(error.to_string())
    }
}
