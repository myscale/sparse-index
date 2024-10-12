use crate::{Opstamp, RowId};

// Doc to opstamp 用于识别应该删除哪些文档。
//
// 由于删除操作的查询匹配集不是在接收到删除操作时立即计算的，
// 我们需要找到一种方法来评估每个文档是在删除操作之前还是之后添加的。
// 这种先后关系通过比较文档的 docstamp 来实现。
//
// Doc to opstamp 映射精确地存储一个数组，
// 该数组以文档 ID 为索引，存储文档的 opstamp。
//
// 这种映射不一定是递增的，因为我们可能会根据快速字段对文档进行排序。
#[derive(Clone)]
pub enum DocToOpstampMapping<'a> {
    WithMap(&'a [Opstamp]),
    None,
}

impl<'a> DocToOpstampMapping<'a> {
    /// 评估一个文档是否应被视为已删除，前提是它包含在操作戳 `delete_opstamp` 删除的删除项。
    ///
    /// 如果 `DocToOpstamp` 映射为空或 `doc_opstamp` 早于删除操作戳，此函数返回 true。
    pub fn is_deleted(&self, inner_row_id: RowId, delete_opstamp: Opstamp) -> bool {
        match self {
            Self::WithMap(sv_opstamps) => {
                let sv_opstamp = sv_opstamps[inner_row_id as usize];
                sv_opstamp < delete_opstamp
            }
            Self::None => true,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::DocToOpstampMapping;

    #[test]
    fn test_doc_to_opstamp_mapping_none() {
        let doc_to_opstamp_mapping = DocToOpstampMapping::None;
        assert!(doc_to_opstamp_mapping.is_deleted(1u32, 0u64));
        assert!(doc_to_opstamp_mapping.is_deleted(1u32, 2u64));
    }

    #[test]
    fn test_doc_to_opstamp_mapping_with_map() {
        let doc_to_opstamp_mapping = DocToOpstampMapping::WithMap(&[5u64, 1u64, 0u64, 4u64, 3u64]);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(0u32, 2u64), false);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(1u32, 2u64), true);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(2u32, 2u64), true);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(3u32, 2u64), false);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(4u32, 2u64), false);
    }
}
