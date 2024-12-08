// mod compressed;
mod compress;
mod encoder;
mod simple;
// mod traits;
mod element;

pub use compress::*;
pub use encoder::{BlockDecoder, BlockEncoder, COMPRESSION_BLOCK_SIZE};
pub use simple::{PostingList, PostingListBuilder, PostingListIterator, PostingListMerger};
// pub use traits::*;
pub use element::*;

use crate::RowId;

use super::QuantizedWeight;



/// TW: Weight type actually stored in disk.
/// OW: We should restore weight type to `origin type`.
pub trait PostingListIter<TW: QuantizedWeight, OW: QuantizedWeight> {
    fn peek(&mut self) -> Option<GenericElement<OW>>;

    fn last_id(&self) -> Option<RowId>;

    fn skip_to(&mut self, row_id: RowId) -> Option<GenericElement<OW>>;

    fn skip_to_end(&mut self);

    fn remains(&self) -> usize;

    fn cursor(&self) -> usize;

    /// Iter till specific row_id.
    /// TODO: If need contains this row_id.
    fn for_each_till_row_id(&mut self, row_id: RowId, f: impl FnMut(&GenericElement<OW>));
}
