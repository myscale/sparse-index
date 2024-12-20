use thiserror::Error;

#[derive(Debug, Error)]
pub enum PostingListError {
    #[error("A merge error happened: '{0}'")]
    MergeError(String),
}
