use log::{debug, info};

use crate::{
    core::{
        BlockDecoder, PostingElementEx, PostingListIteratorTrait, QuantizedParam, QuantizedWeight,
        WeightType, COMPRESSION_BLOCK_SIZE,
    },
    RowId,
};
use std::marker::PhantomData;

use super::CompressedPostingListView;

// OW 表示 CompressedPostingList 里面真正存储的内容，有可能是量化后存储的 U8 类型
// TW 表示在迭代器遍历的时候, 返回的 element 需要进行类型还原
#[derive(Debug, Clone)]
pub struct CompressedPostingListIterator<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    // posting: &'a CompressedPostingList<OW>,
    posting: CompressedPostingListView<'a, OW>,
    is_uncompressed: bool,
    row_ids_uncompressed_in_block: Vec<RowId>,
    cursor: usize,
    decoder: BlockDecoder,
    quantized_param: Option<QuantizedParam>,

    _tw: PhantomData<TW>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> CompressedPostingListIterator<'a, OW, TW> {
    pub fn new(
        posting: &CompressedPostingListView<'a, OW>,
        quantized_param: Option<QuantizedParam>,
    ) -> Self {
        let use_quantized = OW::weight_type() != TW::weight_type() && OW::weight_type() == WeightType::WeightU8;
        if use_quantized && quantized_param.is_none() {
            debug!("Not expect!");
            panic!("Not expect!")
        }

        Self {
            posting: posting.clone(),
            is_uncompressed: false,
            row_ids_uncompressed_in_block: vec![],
            cursor: 0,
            decoder: BlockDecoder::default(),
            quantized_param,
            _tw: PhantomData,
        }
    }
    // 将 OW (内部存储) 转换为 TW (原始类型)
    fn convert_type(&self, raw_element: &PostingElementEx<OW>) -> PostingElementEx<TW> {
        if self.quantized_param.is_none() {
            assert_eq!(OW::weight_type(), TW::weight_type());

            let weight_convert = TW::from_f32(OW::to_f32(raw_element.weight));
            let max_next_weight_convert = TW::from_f32(OW::to_f32(raw_element.max_next_weight));
            let converted_element: PostingElementEx<TW> = PostingElementEx {
                row_id: raw_element.row_id,
                weight: weight_convert,
                max_next_weight: max_next_weight_convert,
            };

            return converted_element;
        } else {
            assert_eq!(OW::weight_type(), WeightType::WeightU8);
            let param: QuantizedParam = self.quantized_param.unwrap();
            let converted: PostingElementEx<TW> = PostingElementEx::<TW> {
                row_id: raw_element.row_id,
                weight: TW::unquantize_with_param(OW::to_u8(raw_element.weight), param),
                max_next_weight: TW::unquantize_with_param(
                    OW::to_u8(raw_element.max_next_weight),
                    param,
                ),
            };
            return converted;
        }
    }
    // TODO 返回值应该是 next 的 element，而不是当前 cursor 所指向的 element；需要验证一下 Simple Posting
    pub fn next(&mut self) -> Option<PostingElementEx<TW>> {
        // 边界判断
        if self.cursor >= self.posting.row_ids_count as usize {
            return None;
        }
        // 如果游标达到了新的 block, 那么标记新的 block 未被解压缩
        if self.cursor % COMPRESSION_BLOCK_SIZE == 0 {
            self.is_uncompressed = false;
        }
        let element_opt: Option<PostingElementEx<TW>> = self.peek();
        // 步进游标
        self.cursor += 1;
        element_opt
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIteratorTrait<OW, TW>
    for CompressedPostingListIterator<'a, OW, TW>
{
    fn peek(&mut self) -> Option<PostingElementEx<TW>> {
        // Boundary
        if self.cursor >= self.posting.row_ids_count as usize {
            return None;
        }

        let block_idx = self.cursor / COMPRESSION_BLOCK_SIZE;
        let block = &self.posting.blocks[block_idx];

        if !self.is_uncompressed {
            // 执行解压缩的逻辑, 在 posting uncompress_block 内部执行动态 block 解压缩
            self.posting.uncompress_block(
                block_idx,
                &mut self.decoder,
                &mut self.row_ids_uncompressed_in_block,
            );
            self.is_uncompressed = true;
        }

        let relative_row_id = self.cursor % COMPRESSION_BLOCK_SIZE;

        let element_ow = PostingElementEx {
            row_id: self.row_ids_uncompressed_in_block[relative_row_id],
            weight: block.weights[relative_row_id],
            max_next_weight: block.max_next_weights[relative_row_id],
        };
        let posting_element: PostingElementEx<TW> = self.convert_type(&element_ow);
        Some(posting_element)
    }

    fn last_id(&self) -> Option<RowId> {
        self.posting.max_row_id
    }

    fn skip_to(&mut self, row_id: RowId) -> Option<PostingElementEx<TW>> {
        while let Some(element) = self.peek() {
            match element.row_id.cmp(&row_id) {
                std::cmp::Ordering::Less => {
                    self.next();
                }
                std::cmp::Ordering::Equal => return Some(element),
                std::cmp::Ordering::Greater => return None,
            }
        }
        None
    }

    fn skip_to_end(&mut self) {
        // 跳跃 cursor 的过程中, 经过了至少 1 个 Block, 则需要将当前 Block 置为未解压状态
        if (self.posting.row_ids_count - self.cursor as u32) / COMPRESSION_BLOCK_SIZE as u32 >= 1 {
            self.is_uncompressed = false;
        }
        self.cursor = (self.posting.row_ids_count - 1) as usize;
    }

    fn remains(&self) -> usize {
        self.posting.row_ids_count as usize - self.cursor
    }

    fn cursor(&self) -> usize {
        self.cursor
    }

    fn for_each_till_row_id(&mut self, row_id: RowId, mut f: impl FnMut(&PostingElementEx<TW>)) {
        let mut element_opt = self.peek();
        while let Some(element) = element_opt {
            // 判断 element 的 row_id 是否达到边界
            if element.row_id > row_id {
                break;
            }
            f(&element);
            // 步进 iterator 游标
            element_opt = self.next();
        }
    }
}
