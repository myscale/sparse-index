// mod compressed;
mod compress;
mod encoder;
mod simple;
mod traits;

pub use compress::*;
pub use encoder::{BlockDecoder, BlockEncoder, COMPRESSION_BLOCK_SIZE};
pub use simple::{PostingList, PostingListBuilder, PostingListIterator, PostingListMerger};
pub use traits::*;
