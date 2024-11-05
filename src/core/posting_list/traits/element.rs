use crate::{
    core::common::types::{DimWeight, ElementOffsetType},
    RowId,
};

pub const DEFAULT_MAX_NEXT_WEIGHT: DimWeight = DimWeight::NEG_INFINITY;

#[derive(Debug, Clone, PartialEq)]
pub struct PostingElement {
    /// Row ID
    pub row_id: ElementOffsetType,
    /// Weight of the record in the dimension
    pub weight: DimWeight,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct PostingElementEx {
    /// Row ID
    pub row_id: RowId,
    /// Weight of the record in the dimension
    pub weight: DimWeight,
    /// Max weight of the next elements in the posting list.
    pub max_next_weight: DimWeight,
}

impl From<PostingElementEx> for PostingElement {
    fn from(value: PostingElementEx) -> Self {
        Self {
            row_id: value.row_id,
            weight: value.weight,
        }
    }
}

impl PostingElementEx {
    pub fn new(row_id: ElementOffsetType, weight: DimWeight) -> Self {
        Self {
            row_id,
            weight,
            max_next_weight: DEFAULT_MAX_NEXT_WEIGHT,
        }
    }
}
