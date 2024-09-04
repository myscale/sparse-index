use crate::core::common::types::{ElementOffsetType, Weight};
use crate::core::posting_list::compressed::comp_posting_list::CompressedPostingList;
use crate::core::posting_list::compressed::comp_posting_list_store_size::CompressedPostingListStoreSize;
use crate::core::posting_list::compressed::{BitPackerImpl, CompressedPostingChunk};
use crate::core::posting_list::{CompressedPostingListIterator, GenericPostingElement};
use bitpacking::BitPacker;

/// A non-owning view of [`GenericCompressedPostingList`].
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CompressedPostingListView<'a, W: Weight> {
    pub(super) id_data: &'a [u8],
    pub(super) chunks: &'a [CompressedPostingChunk<W>],
    pub(super) remainders: &'a [GenericPostingElement<W>],
    pub(super) last_id: Option<ElementOffsetType>,
    pub(super) multiplier: W::QuantizationParams,
}

impl<'a, W: Weight> CompressedPostingListView<'a, W> {
    pub fn new(
        id_data: &'a [u8],
        chunks: &'a [CompressedPostingChunk<W>],
        remainders: &'a [GenericPostingElement<W>],
        last_id: Option<ElementOffsetType>,
        multiplier: W::QuantizationParams,
    ) -> Self {
        Self {
            id_data,
            chunks,
            remainders,
            last_id,
            multiplier,
        }
    }

    pub fn parts(
        &self,
    ) -> (
        &'a [u8],
        &'a [CompressedPostingChunk<W>],
        &'a [GenericPostingElement<W>],
    ) {
        (self.id_data, self.chunks, self.remainders)
    }

    pub fn last_id(&self) -> Option<ElementOffsetType> {
        self.last_id
    }

    pub fn multiplier(&self) -> W::QuantizationParams {
        self.multiplier
    }

    pub fn store_size(&self) -> CompressedPostingListStoreSize {
        CompressedPostingListStoreSize::new::<W>(
            self.id_data.len(),
            self.chunks.len(),
            self.remainders.len(),
        )
    }

    // TODO: 这些。内部元素（id_data, chunks）在 to_vec 之后所有权怎么变化？当前 self 自己的所有权如何变化？
    pub fn to_owned(&self) -> CompressedPostingList<W> {
        CompressedPostingList {
            id_data: self.id_data.to_vec(),
            chunks: self.chunks.to_vec(),
            remainders: self.remainders.to_vec(),
            last_id: self.last_id,
            quantization_params: self.multiplier,
        }
    }

    pub fn len(&self) -> usize {
        self.chunks.len() * BitPackerImpl::BLOCK_LEN + self.remainders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.remainders.is_empty()
    }

    /// 解压缩了 chunk 位置对应的 id_data: Vec<u8> -> Vec<u32>
    pub fn decompress_chunk(
        &self,
        chunk_index: usize,
        decompressed_chunk: &mut [ElementOffsetType; BitPackerImpl::BLOCK_LEN],
    ) {
        let chunk = &self.chunks[chunk_index];
        let chunk_size = Self::get_chunk_size(self.chunks, self.id_data, chunk_index);
        let chunk_bits = chunk_size * u8::BITS as usize / BitPackerImpl::BLOCK_LEN;

        BitPackerImpl::new().decompress_strictly_sorted(
            chunk.initial.checked_sub(1),
            &self.id_data[chunk.offset as usize..chunk.offset as usize + chunk_size],
            decompressed_chunk,
            chunk_bits as u8
        );
    }

    pub fn get_chunk_size(
        chunks: &[CompressedPostingChunk<W>],
        data: &[u8],
        chunk_index: usize,
    ) -> usize {
        if chunk_index + 1 < chunks.len() {
            chunks[chunk_index+1].offset as usize - chunks[chunk_index].offset as usize
        } else {
            data.len() - chunks[chunk_index].offset as usize
        }
    }

    pub fn iter(&self) -> CompressedPostingListIterator<'a, W> {
        CompressedPostingListIterator::new(self)
    }
}
