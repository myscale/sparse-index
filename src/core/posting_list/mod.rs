// mod compressed;
mod compress;
mod encoder;
mod simple;
// mod traits;
mod element;
mod errors;
pub use compress::*;
pub use encoder::{BlockDecoder, BlockEncoder, COMPRESSION_BLOCK_SIZE};
pub use simple::{PostingList, PostingListBuilder, PostingListIterator, PostingListMerger};
// pub use traits::*;
pub use element::*;

use crate::RowId;

use super::QuantizedWeight;


/// OW: We should restore weight type to `origin type`.
/// TW: Weight type actually stored in disk.
pub trait PostingListIter<OW: QuantizedWeight, TW: QuantizedWeight> {
    fn peek(&mut self) -> Option<GenericElement<OW>>;

    fn last_id(&self) -> Option<RowId>;

    fn skip_to(&mut self, row_id: RowId) -> Option<GenericElement<OW>>;

    // TODO: skip_to_end 使用 length 还是 length-1？
    fn skip_to_end(&mut self);

    fn remains(&self) -> usize;

    fn cursor(&self) -> usize;

    /// Iter till specific row_id.
    /// TODO: If need contains this row_id.
    fn for_each_till_row_id(&mut self, row_id: RowId, f: impl FnMut(&GenericElement<OW>));
}
