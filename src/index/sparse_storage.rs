use crate::common::file_operations::FileOperationError;
use crate::core::common::types::ElementOffsetType;
use crate::core::sparse_vector::SparseVector;

pub trait SparseVectorStorage {
    fn get_sparse(&self, key: ElementOffsetType) -> Result<SparseVector, FileOperationError>;
}


