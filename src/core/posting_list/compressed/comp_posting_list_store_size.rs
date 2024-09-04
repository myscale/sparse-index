use crate::core::common::types::Weight;
use crate::core::posting_list::compressed::CompressedPostingChunk;
use crate::core::posting_list::GenericPostingElement;
use std::mem::size_of;

pub struct CompressedPostingListStoreSize {
    pub total: usize,
    pub id_data_bytes: usize,
    pub chunks_count: usize,
}

impl CompressedPostingListStoreSize {
    pub(super) fn new<W: Weight>(
        id_data_bytes: usize,
        chunks_count: usize,
        remainders_count: usize,
    ) -> Self {
        CompressedPostingListStoreSize {
            total: id_data_bytes
                + chunks_count * size_of::<CompressedPostingChunk<W>>()
                + remainders_count * size_of::<GenericPostingElement<W>>(),
            id_data_bytes,
            chunks_count,
        }
    }
}
