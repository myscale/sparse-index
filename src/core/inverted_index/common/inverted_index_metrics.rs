use std::cmp::{max, min};

use crate::{core::DimId, RowId};

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