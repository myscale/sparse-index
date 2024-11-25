use crate::{
    core::{common::types::DimWeight, QuantizedWeight},
    RowId,
};

#[derive(Debug, Clone, PartialEq)]
pub struct PostingListElement<W: QuantizedWeight> {
    /// Row ID
    pub row_id: RowId,
    /// Weight of the record in the dimension
    pub weight: W,
}

pub const DEFAULT_MAX_NEXT_WEIGHT: DimWeight = DimWeight::NEG_INFINITY;
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct PostingElementEx<W: QuantizedWeight> {
    /// Row ID
    pub row_id: RowId,

    /// Weight of the record in the dimension
    pub weight: W,

    /// The `weight` of the posting list where the element
    /// with the maximum weight among all remaining elements.
    pub max_next_weight: W,
}

impl<W: QuantizedWeight> Default for PostingElementEx<W> {
    fn default() -> Self {
        Self {
            row_id: 0,
            weight: W::MINIMUM(),
            max_next_weight: W::MINIMUM(),
        }
    }
}

impl<W: QuantizedWeight> From<PostingElementEx<W>> for PostingListElement<W> {
    fn from(value: PostingElementEx<W>) -> Self {
        Self {
            row_id: value.row_id,
            weight: W::from_f32(value.weight.to_f32()),
        }
    }
}

impl<W: QuantizedWeight> PostingElementEx<W> {
    pub fn new(row_id: RowId, weight: DimWeight) -> Self {
        Self {
            row_id,
            weight: W::from_f32(weight),
            max_next_weight: W::from_f32(DEFAULT_MAX_NEXT_WEIGHT),
        }
    }
}
