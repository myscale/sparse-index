use crate::core::posting_list::traits::PostingElementEx;
use crate::core::QuantizedWeight;
use crate::RowId;

pub trait PostingListIteratorTrait<OW: QuantizedWeight, TW: QuantizedWeight> {
    /// 返回当前 Iterator 顶部的 Element
    fn peek(&mut self) -> Option<PostingElementEx<TW>>;

    /// 返回 PostingList 末尾的 row_id
    fn last_id(&self) -> Option<RowId>;

    /// 将 Iterator 的 cursor 移动到目标 row_id </br>
    /// 如果找不到该 row_id 就移动到附近
    fn skip_to(&mut self, row_id: RowId) -> Option<PostingElementEx<TW>>;

    /// 移动到 PostingList 末尾
    fn skip_to_end(&mut self);

    /// Iterator 遍历完 PostingList 还需要多少个元素
    fn remains(&self) -> usize;

    /// 返回 Iterator 当前的 cursor 位置
    fn cursor(&self) -> usize;

    /// Iter till specific row_id.
    /// TODO: 是否需要包含这个 row_id
    fn for_each_till_row_id(&mut self, row_id: RowId, f: impl FnMut(&PostingElementEx<TW>));
}
