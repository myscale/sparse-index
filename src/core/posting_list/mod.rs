mod compressed;
mod simple;
mod traits;

pub use simple::{PostingList, PostingListBuilder, PostingListIterator};
pub use traits::*;

pub use compressed::{
    CompressedPostingBuilder, CompressedPostingChunk, CompressedPostingList,
    CompressedPostingListIterator, CompressedPostingListStdIterator,
    CompressedPostingListStoreSize, CompressedPostingListView,
};
