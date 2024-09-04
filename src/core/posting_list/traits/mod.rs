mod element;
mod iterator;

pub use element::{
    GenericPostingElement, PostingElement, PostingElementEx, DEFAULT_MAX_NEXT_WEIGHT,
};
pub use iterator::PostingListIter;
