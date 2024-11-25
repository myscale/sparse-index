use crate::core::DimId;
use crate::RowId;
use std::cmp::{max, min};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct InvertedIndexMetrics {
    pub min_row_id: RowId,
    pub max_row_id: RowId,
    pub min_dim_id: DimId,
    pub max_dim_id: DimId,

    pub vector_count: usize,
}

impl Default for InvertedIndexMetrics {
    fn default() -> Self {
        Self {
            min_row_id: RowId::MAX,
            max_row_id: RowId::MIN,
            min_dim_id: 0,
            max_dim_id: DimId::MIN,
            vector_count: 0,
        }
    }
}

impl InvertedIndexMetrics {
    pub fn compare_and_update_row_id(&mut self, other: RowId) {
        self.min_row_id = min(self.min_row_id, other);
        self.max_row_id = max(self.max_row_id, other);
    }

    pub fn compare_and_update_dim_id(&mut self, other: DimId) {
        self.min_dim_id = min(self.min_dim_id, other);
        self.max_dim_id = max(self.max_dim_id, other);
    }

    pub fn increase_vector_count(&mut self) {
        self.vector_count += 1;
    }
}

pub trait InvertedIndexRamAccess: Sized + Debug {
    fn metrics(&self) -> InvertedIndexMetrics;
    // fn posting_len(&self, dim_id: &DimId) -> Option<usize>;

    // TODO 这里返回值使用到的 PostingList 后面可以换成一个 Trait, 否则的话 Compressed 还得重新写
    // fn posting_with_param(&self, dim_id: &DimId) -> Option<(&PostingList<OW>, Option<QuantizedParam>)>;
    fn size(&self) -> usize;
    fn empty(&self) -> bool {
        self.size() == 0
    }

    // type Iter<'a>: PostingListIteratorTrait<W> + Clone
    // where
    //     Self: 'a;

    // 直接遍历原始数据
    // fn iter(&self, dim_id: &DimId) -> Option<Self::Iter<'_>>;

    // Get number of posting lists

    // Check if the core is empty

    // Get number of posting lists for dimension id

    // TODO 后续使用 1个 统一的 meta 处理
    // fn vector_count(&self) -> usize;

    // fn min_dim_id(&self) -> DimId;

    // fn max_dim_id(&self) -> DimId;

    // fn min_row_id(&self) -> RowId;

    // fn max_row_id(&self) -> RowId;

    // fn remove(&mut self, row_id: RowId);

    // fn insert(&mut self, row_id: RowId, sparse_vector: SparseVector);

    // fn update(&mut self, row_id: RowId, new_vector: SparseVector, old_vector: SparseVector);
}
