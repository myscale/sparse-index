use crate::core::{SparseRowContent, SparseVector};
use crate::Opstamp;

/// Timestamped Delete operation.
pub struct DeleteOperation {
    pub opstamp: Opstamp,
    // pub target: Box<dyn Weight>,
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation {
    pub opstamp: Opstamp,
    pub row_content: SparseRowContent,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation {
    /// 新增数据操作，新增 1 个 SparseVector
    Add(SparseRowContent),
    // 后续换成 row_id，表示删除掉一行
    // Delete(u64),
}
