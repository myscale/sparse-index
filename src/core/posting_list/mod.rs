mod compressed;
mod simple;
mod traits;

pub use simple::{PostingList, PostingListBuilder, PostingListIterator};
pub use traits::*;

pub use compressed::{
    CompressedPostingChunk,
    CompressedPostingList,
    CompressedPostingBuilder,
    CompressedPostingListIterator,
    CompressedPostingListStdIterator,
    CompressedPostingListStoreSize,
    CompressedPostingListView
};