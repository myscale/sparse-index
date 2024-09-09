use crate::core::common::types::{DimWeight, ElementOffsetType, Weight};
use crate::core::posting_list::compressed::comp_posting_list_view::CompressedPostingListView;
use crate::core::posting_list::compressed::{count_le_sorted, BitPackerImpl};
use crate::core::posting_list::{PostingElement, PostingElementEx, PostingListIter};
use bitpacking::BitPacker;
use serde_json::de::Read;
use std::cmp::Ordering;

#[derive(Clone)]
pub struct CompressedPostingListIterator<'a, W: Weight> {
    list: CompressedPostingListView<'a, W>,

    /// If true, then `decompressed_chunk` contains the unpacked chunk for the current position.
    unpacked: bool,

    decompressed_chunk: [ElementOffsetType; BitPackerImpl::BLOCK_LEN],

    pos: usize,
}

impl<'a, W: Weight> CompressedPostingListIterator<'a, W> {
    #[inline]
    pub fn new(list: &CompressedPostingListView<'a, W>) -> Self {
        Self {
            list: list.clone(),
            unpacked: false,
            decompressed_chunk: [0; BitPackerImpl::BLOCK_LEN],
            pos: 0,
        }
    }

    #[inline]
    fn next(&mut self) -> Option<PostingElement> {
        // 获取当前位置的 PostingElementEx
        let result = self.peek()?;

        // 判断当前的 position 是否处于压缩分块的范围内
        // 当前位置所在的分块索引 self.pos / BitPackerImpl::BLOCK_LEN
        if self.pos / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            self.pos += 1;
            if self.pos % BitPackerImpl::BLOCK_LEN == 0 {
                // 达到分块边界，后续位置的分块均未压缩
                self.unpacked = false;
            }
        } else {
            self.pos += 1;
        }

        Some(result.into())
    }
}

impl<'a, W: Weight> PostingListIter for CompressedPostingListIterator<'a, W> {
    #[inline]
    fn peek(&mut self) -> Option<PostingElementEx> {
        let pos = self.pos;
        if pos / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            if !self.unpacked {
                self.list
                    .decompress_chunk(pos / BitPackerImpl::BLOCK_LEN, &mut self.decompressed_chunk);
                self.unpacked = true;
            }

            let chunk = &self.list.chunks[pos / BitPackerImpl::BLOCK_LEN];
            return Some(PostingElementEx {
                row_id: self.decompressed_chunk[pos % BitPackerImpl::BLOCK_LEN],
                weight: chunk.weights[pos % BitPackerImpl::BLOCK_LEN].to_f32(self.list.multiplier),
                max_next_weight: Default::default(),
            });
        }

        self.list
            .remainders
            .get(pos - self.list.chunks.len() * BitPackerImpl::BLOCK_LEN)
            .map(|e| PostingElementEx {
                row_id: e.row_id,
                weight: e.weight.to_f32(self.list.multiplier),
                max_next_weight: Default::default(),
            })
    }

    #[inline]
    fn last_id(&self) -> Option<ElementOffsetType> {
        self.list.last_id
    }

    #[inline]
    fn skip_to(&mut self, row_id: ElementOffsetType) -> Option<PostingElementEx> {
        // TODO: optimize
        while let Some(e) = self.peek() {
            match e.row_id.cmp(&row_id) {
                Ordering::Equal => return Some(e),
                Ordering::Greater => return None,
                Ordering::Less => {
                    self.next();
                }
            }
        }
        None
    }

    #[inline]
    fn skip_to_end(&mut self) {
        self.pos = self.list.chunks.len() * BitPackerImpl::BLOCK_LEN + self.list.remainders.len();
    }

    #[inline]
    fn len_to_end(&self) -> usize {
        self.list.len() - self.pos
    }

    #[inline]
    fn current_index(&self) -> usize {
        self.pos
    }

    #[inline]
    fn for_each_till_id<Ctx: ?Sized>(
        &mut self,
        id: ElementOffsetType,
        ctx: &mut Ctx,
        mut f: impl FnMut(&mut Ctx, ElementOffsetType, DimWeight),
    ) {
        let mut pos = self.pos;

        // Iterate over compressed chunks
        let mut weights_buf = [0.0; BitPackerImpl::BLOCK_LEN];

        let mut need_unpack = !self.unpacked;
        while pos / BitPackerImpl::BLOCK_LEN < self.list.chunks.len() {
            if need_unpack {
                self.list
                    .decompress_chunk(pos / BitPackerImpl::BLOCK_LEN, &mut self.decompressed_chunk);
            }
            need_unpack = true;

            let chunk = &self.list.chunks[pos / BitPackerImpl::BLOCK_LEN];

            let start = pos % BitPackerImpl::BLOCK_LEN;
            let count = count_le_sorted(id, &self.decompressed_chunk[start..]);
            let weights = W::into_f32_slice(
                self.list.multiplier,
                &chunk.weights[start..start + count],
                &mut weights_buf[..count],
            );

            for (idx, weight) in
                std::iter::zip(&self.decompressed_chunk[start..start + count], weights)
            {
                f(ctx, *idx, *weight);
            }
            pos += count;
            if start + count != BitPackerImpl::BLOCK_LEN {
                self.unpacked = true;
                self.pos = pos;
                return;
            }
        }

        // Iterate over remainders
        for e in &self.list.remainders[pos - self.list.chunks.len() * BitPackerImpl::BLOCK_LEN..] {
            if e.row_id > id {
                self.pos = pos;
                return;
            }
            f(ctx, e.row_id, e.weight.to_f32(self.list.multiplier));
            pos += 1;
        }
        self.pos = pos;
    }

    fn reliable_max_next_weight() -> bool {
        false
    }

    fn into_std_iter(self) -> impl Iterator<Item = PostingElement> {
        CompressedPostingListStdIterator(self)
    }
}

#[derive(Clone)]
pub struct CompressedPostingListStdIterator<'a, W: Weight>(CompressedPostingListIterator<'a, W>);

impl<W: Weight> Iterator for CompressedPostingListStdIterator<'_, W> {
    type Item = PostingElement;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
