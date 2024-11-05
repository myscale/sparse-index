// mod compressed;
mod posting_list_merge;
mod simple;
mod traits;

pub use posting_list_merge::PostingListMerge;
pub use simple::{PostingList, PostingListBuilder, PostingListIterator};
pub use traits::*;
// pub use compressed::{
//     CompressedPostingBuilder, CompressedPostingChunk, CompressedPostingList,
//     CompressedPostingListIterator, CompressedPostingListStdIterator,
//     CompressedPostingListStoreSize, CompressedPostingListView,
// };
