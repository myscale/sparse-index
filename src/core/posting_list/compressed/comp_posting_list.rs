use crate::core::common::types::{DimWeight, ElementOffsetType, Weight};
use crate::core::posting_list::compressed::comp_posting_list_builder::CompressedPostingBuilder;
use crate::core::posting_list::compressed::comp_posting_list_iterator::CompressedPostingListIterator;
use crate::core::posting_list::compressed::comp_posting_list_view::CompressedPostingListView;
use crate::core::posting_list::compressed::CompressedPostingChunk;
use crate::core::posting_list::GenericPostingElement;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingList<W: Weight> {
    /// Compressed ids data. Chunks refer to subslies of this data.
    /// 压缩之后的 id 数据，以字节形式存储
    pub(super) id_data: Vec<u8>,

    /// Fixed-size chunks.
    /// 固定大小的一组块，每个块包含一组压缩后的 ID 和对应的权重
    pub(super) chunks: Vec<CompressedPostingChunk<W>>,

    /// Remainder elements that do not fit into chunks.
    /// 剩余的无法填充到完整的块中的元素
    pub(super) remainders: Vec<GenericPostingElement<W>>,

    /// Id of the last element in the list. Used to avoid unpacking the last chunk.
    // 列表中最后一个元素的 ID, 用来避免解压最后一个块
    pub(super) last_id: Option<ElementOffsetType>,

    /// Quantization parameters.
    /// 量化参数
    pub(super) quantization_params: W::QuantizationParams,
}


impl<W: Weight> CompressedPostingList<W> {
    pub fn view(&self) -> CompressedPostingListView<W> {
        CompressedPostingListView {
            id_data: &self.id_data,
            chunks: &self.chunks,
            remainders: &self.remainders,
            last_id: self.last_id,
            multiplier: self.quantization_params,
        }
    }

    pub fn iter(&self) -> CompressedPostingListIterator<W> {
        self.view().iter()
    }

    #[cfg(test)]
    pub fn from(records: Vec<(ElementOffsetType, DimWeight)>) -> CompressedPostingList<W> {
        let mut posting_list = CompressedPostingBuilder::new();
        for (id, weight) in records {
            posting_list.add(id, weight);
        }
        posting_list.build()
    }
}