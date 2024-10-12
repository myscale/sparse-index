use crate::core::common::types::{DimWeight, ElementOffsetType};
use crate::core::posting_list::traits::{PostingElement, PostingElementEx};
use crate::RowId;

pub trait PostingListIter {
    /// 返回当前 Iterator 顶部的 Element
    fn peek(&mut self) -> Option<PostingElementEx>;

    /// 返回 PostingList 末尾的 row_id
    fn last_id(&self) -> Option<RowId>;

    /// 将 Iterator 的 cursor 移动到目标 row_id </br>
    /// 如果找不到该 row_id 就移动到附近
    fn skip_to(&mut self, row_id: RowId) -> Option<PostingElementEx>;

    /// 移动到 PostingList 末尾
    fn skip_to_end(&mut self);

    /// Iterator 遍历完 PostingList 还需要多少个元素
    fn len_to_end(&self) -> usize;

    /// 返回 Iterator 当前的 cursor 位置
    fn current_index(&self) -> usize;

    /// 遍历倒排列表，直到达到指定的 ID(包括该 ID)，并对每个元素应用 f 函数
    /// Sized 表示在编译时变量的长度可以确定，?Sized 表示编译时长度不固定的类型(切片)
    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: RowId,
        ctx: &mut Ctx,
        f: impl FnMut(&mut Ctx, ElementOffsetType, DimWeight),
    );

    fn for_each_till_row_id(
        &mut self,
        row_id: RowId,
        f: impl FnMut(&PostingElementEx),
    );

    /// 返回 max_next_weight 是否可靠 </br>
    /// 据此用来判断是否需要重新写入一遍 max_next_weight
    fn reliable_max_next_weight() -> bool;

    // 将倒排列表迭代器转换为普通的迭代器
    fn into_std_iter(self) -> impl Iterator<Item = PostingElement>;
}
