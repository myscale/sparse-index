use thiserror::Error;

#[derive(Debug, Error)]
pub enum InvertedIndexError {
    #[error("Invalid InvertedIndexConfig: '{0}'")]
    InvalidIndexConfig(String),
}
