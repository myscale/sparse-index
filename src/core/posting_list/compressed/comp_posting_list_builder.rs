use bitpacking::BitPacker;
use itertools::Itertools;
use crate::core::common::types::{DimWeight, ElementOffsetType, Weight};
use crate::core::posting_list::compressed::{BitPackerImpl, CompressedPostingChunk};
use crate::core::posting_list::compressed::comp_posting_list::CompressedPostingList;
use crate::core::posting_list::{GenericPostingElement, PostingElement};
use crate::core::posting_list::compressed::comp_posting_list_view::CompressedPostingListView;

pub struct CompressedPostingBuilder {
    elements: Vec<PostingElement>,
}

impl CompressedPostingBuilder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        CompressedPostingBuilder {
            elements: Vec::new(),
        }
    }

    /// Add a new record to the posting list.
    pub fn add(&mut self, row_id: ElementOffsetType, weight: DimWeight) {
        self.elements.push(PostingElement { row_id, weight });
    }

    pub fn build<W: Weight>(mut self) -> CompressedPostingList<W> {
        // 对所有的 element 根据 row_id 排序
        self.elements.sort_unstable_by_key(|e| e.row_id);

        let quantization_params =
            W::quantization_params_for(self.elements.iter().map(|e| e.weight));

        // 检查是否存在重复的 row_id，如果存在则触发 Panic
        #[cfg(debug_assertions)]
        if let Some(e) = self.elements.iter().duplicates_by(|e| e.row_id).next() {
            panic!("Duplicate id {} in posting list", e.row_id);
        }

        // this_chunk 用于存储当前正在处理的分块数据
        let mut this_chunk = Vec::with_capacity(BitPackerImpl::BLOCK_LEN);
        // 用于压缩与解压缩数据的 BitPackerImpl 实例对象
        let bitpacker = BitPackerImpl::new();
        // 存储压缩后的分块信息
        let mut chunks = Vec::with_capacity(self.elements.len() / BitPackerImpl::BLOCK_LEN);
        // 压缩后的数据大小
        let mut data_size = 0;
        // 存储不满一个分块的剩余元素
        let mut remainders = Vec::with_capacity(self.elements.len() % BitPackerImpl::BLOCK_LEN);

        // 填充 chunks 和 remainders
        for chunk in self.elements.chunks(BitPackerImpl::BLOCK_LEN) {
            if chunk.len() == BitPackerImpl::BLOCK_LEN {
                // 清除 this_chunk 中旧数据, 将当前遍历到的分块 row_id 数据存储至 this_chunk
                this_chunk.clear();
                this_chunk.extend(chunk.iter().map(|e| e.row_id));

                // 记录起始 row_id
                let initial = this_chunk[0];
                // chunk_bits 表示在该 `this_chunk` 中，至少用 x 位 bit，就能够表示每一个元素(element offset u32)
                let chunk_bits =
                    bitpacker.num_bits_strictly_sorted(initial.checked_sub(1), &this_chunk);
                // 一个 compressed chunk 所占据的字节数量 B
                let chunk_size = BitPackerImpl::compressed_block_size(chunk_bits);
                chunks.push(CompressedPostingChunk {
                    initial,
                    offset: data_size as u32,
                    weights: chunk
                        .iter()
                        .map(|e| Weight::from_f32(quantization_params, e.weight))
                        .collect::<Vec<_>>()
                        .try_into()
                        .expect("Invalid chunk size"),
                });
                data_size += chunk_size;
            } else {
                for e in chunk {
                    remainders.push(GenericPostingElement {
                        row_id: e.row_id,
                        weight: Weight::from_f32(quantization_params, e.weight),
                    });
                }
            }
        }

        let mut id_data = vec![0u8; data_size];
        for (chunk_index, chunk_data) in self
            .elements
            // chunks_exact 会将 self.elements 分割成固定大小为 BitPackerImpl::BLOCK_LEN 的块
            .chunks_exact(BitPackerImpl::BLOCK_LEN)
            .enumerate()
        {
            this_chunk.clear();
            this_chunk.extend(chunk_data.iter().map(|e| e.row_id));

            let chunk = &chunks[chunk_index];
            // 获得当前 chunk 压缩之后的大小（以字节为单位）
            let chunk_size =
                CompressedPostingListView::get_chunk_size(&chunks, &id_data, chunk_index);
            // chunk 内部每个元素（u32）至少使用的 bits 位数
            // 下面这个计算流程实际上相当于 BitPackerImpl::compressed_block_size 的逆运算
            let chunk_bits = chunk_size * u8::BITS as usize / BitPackerImpl::BLOCK_LEN;
            // 对当前块进行压缩
            bitpacker.compress_strictly_sorted(
                // chunk 起始 id
                chunk.initial.checked_sub(1),
                // 当前未压缩的 chunk
                &this_chunk,
                // id_data 用于存储当前块压缩后数据的切片, 长度为 chunk_size
                &mut id_data[chunk.offset as usize..chunk.offset as usize + chunk_size],
                // 当前块中每个元素所需 bits 位数
                chunk_bits as u8,
            );
        }

        CompressedPostingList {
            id_data,
            chunks,
            remainders,
            last_id: self.elements.last().map(|e| e.row_id),
            quantization_params,
        }
    }
}
