use crate::core::common::types::{DimWeight, ElementOffsetType};
use crate::core::posting_list::traits::{PostingElement, PostingElementEx};

pub trait PostingListIter {
    fn peek(&mut self) -> Option<PostingElementEx>;

    fn last_id(&self) -> Option<ElementOffsetType>;

    fn skip_to(&mut self, row_id: ElementOffsetType) -> Option<PostingElementEx>;

    fn skip_to_end(&mut self);

    fn len_to_end(&self) -> usize;

    fn current_index(&self) -> usize;

    /// Iterate over the posting list until `id` is reached (inclusive).
    /// 遍历倒排列表，直到达到指定的 ID(包括该 ID)，并对每个元素应用 f 函数
    /// Sized 表示在编译时变量的长度可以确定，?Sized 表示编译时长度不固定的类型(切片)
    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: ElementOffsetType,
        ctx: &mut Ctx,
        f: impl FnMut(&mut Ctx, ElementOffsetType, DimWeight),
    );

    /// Whether the max_next_weight is reliable.
    /// 返回 max_next_weight 是否可靠
    fn reliable_max_next_weight() -> bool;

    // 将倒排列表迭代器转换为普通的迭代器
    fn into_std_iter(self) -> impl Iterator<Item = PostingElement>;
}
