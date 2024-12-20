use crate::core::{dispatch::GenericPostingListIterator, DimId, DimWeight};

pub struct SearchPostingIterator<'a> {
    pub generic_posting: GenericPostingListIterator<'a>,
    pub dim_id: DimId,
    pub dim_weight: DimWeight,
}
