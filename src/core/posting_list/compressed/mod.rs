mod comp_posting_chunk;
mod comp_posting_list;
mod comp_posting_list_builder;
mod comp_posting_list_iterator;
mod comp_posting_list_store_size;
mod comp_posting_list_view;

type BitPackerImpl = bitpacking::BitPacker4x;

pub use comp_posting_chunk::CompressedPostingChunk;
pub use comp_posting_list::CompressedPostingList;
pub use comp_posting_list_builder::CompressedPostingBuilder;
pub use comp_posting_list_iterator::{
    CompressedPostingListIterator, CompressedPostingListStdIterator,
};
pub use comp_posting_list_store_size::CompressedPostingListStoreSize;
pub use comp_posting_list_view::CompressedPostingListView;

/// Find the amount of elements in the sorted array that are less or equal to `val`. In other words,
/// the first core `i` such that `data[i] > val`, or `data.len()` if all elements are less or equal
/// to `val`.
fn count_le_sorted<T: Copy + Eq + Ord>(val: T, data: &[T]) -> usize {
    if data.last().map_or(true, |&x| x < val) {
        // Happy case
        return data.len();
    }

    data.binary_search(&val).map_or_else(|x| x, |x| x + 1)
}
