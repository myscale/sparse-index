use crate::core::posting_list::traits::PostingElementEx;
use crate::core::QuantizedWeight;
use crate::RowId;

pub trait PostingListIteratorTrait<OW: QuantizedWeight, TW: QuantizedWeight> {
    fn peek(&mut self) -> Option<PostingElementEx<TW>>;

    fn last_id(&self) -> Option<RowId>;

    fn skip_to(&mut self, row_id: RowId) -> Option<PostingElementEx<TW>>;

    fn skip_to_end(&mut self);

    fn remains(&self) -> usize;

    fn cursor(&self) -> usize;

    /// Iter till specific row_id.
    /// TODO: If need contains this row_id.
    fn for_each_till_row_id(&mut self, row_id: RowId, f: impl FnMut(&PostingElementEx<TW>));
}
