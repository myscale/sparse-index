// rustfmt::skip
mod common;
pub mod index_searcer;
mod inverted_index;
mod posting_list;
mod scores;
mod sparse_vector;
pub use common::*;
pub use inverted_index::*;
pub use posting_list::*;
pub use scores::*;
pub use sparse_vector::*;

use index_searcer::IndexedPostingListIterator;
use std::{borrow::Cow, marker::PhantomData, path::PathBuf};

use crate::{
    index::IndexSettings,
    sparse_index::{IndexWeightType, StorageType},
    RowId,
};

#[derive(Debug, Clone)]
pub enum GenericInvertedIndexMmapType {
    InvertedIndexMmapF32NoQuantized(InvertedIndexMmap<f32, f32>),
    InvertedIndexMmapF32Quantized(InvertedIndexMmap<f32, u8>),
    InvertedIndexMmapF16NoQuantized(InvertedIndexMmap<half::f16, half::f16>),
    InvertedIndexMmapF16Quantized(InvertedIndexMmap<half::f16, u8>),
    InvertedIndexMmapU8NoQuantized(InvertedIndexMmap<u8, u8>),
    CompressedInvertedIndexMmapF32NoQuantized(CompressedInvertedIndexMmap<f32, f32>),
    CompressedInvertedIndexMmapF32Quantized(CompressedInvertedIndexMmap<f32, u8>),
    CompressedInvertedIndexMmapF16NoQuantized(CompressedInvertedIndexMmap<half::f16, half::f16>),
    CompressedInvertedIndexMmapF16Quantized(CompressedInvertedIndexMmap<half::f16, u8>),
    CompressedInvertedIndexMmapU8NoQuantized(CompressedInvertedIndexMmap<u8, u8>),
}

pub enum GenericPostingsIterator<'a> {
    F32NoQuantized(IndexedPostingListIterator<f32, f32, PostingListIterator<'a, f32, f32>>),
    F32Quantized(IndexedPostingListIterator<f32, u8, PostingListIterator<'a, u8, f32>>),
    F16NoQuantized(
        IndexedPostingListIterator<
            half::f16,
            half::f16,
            PostingListIterator<'a, half::f16, half::f16>,
        >,
    ),
    F16Quantized(IndexedPostingListIterator<half::f16, u8, PostingListIterator<'a, u8, half::f16>>),
    U8NoQuantized(IndexedPostingListIterator<u8, u8, PostingListIterator<'a, u8, u8>>),
    CompressedF32NoQuantized(
        IndexedPostingListIterator<f32, f32, CompressedPostingListIterator<'a, f32, f32>>,
    ),
    CompressedF32Quantized(
        IndexedPostingListIterator<f32, u8, CompressedPostingListIterator<'a, u8, f32>>,
    ),
    CompressedF16NoQuantized(
        IndexedPostingListIterator<
            half::f16,
            half::f16,
            CompressedPostingListIterator<'a, half::f16, half::f16>,
        >,
    ),
    CompressedF16Quantized(
        IndexedPostingListIterator<half::f16, u8, CompressedPostingListIterator<'a, u8, half::f16>>,
    ),
    CompressedU8NoQuantized(
        IndexedPostingListIterator<u8, u8, CompressedPostingListIterator<'a, u8, u8>>,
    ),
}

pub enum GenericInvertedIndexRamBuilder {
    F32NoQuantized(InvertedIndexRamBuilder<f32, f32>),
    F32Quantized(InvertedIndexRamBuilder<f32, u8>),
    F16NoQuantized(InvertedIndexRamBuilder<half::f16, half::f16>),
    F16Quantized(InvertedIndexRamBuilder<half::f16, u8>),
    U8NoQuantized(InvertedIndexRamBuilder<u8, u8>),
}

pub enum GenericInvertedIndexRam {
    F32InvertedIndexRam(InvertedIndexRam<f32>),
    F16InvertedIndexRam(InvertedIndexRam<half::f16>),
    U8InvertedIndexRam(InvertedIndexRam<u8>),
}

impl GenericInvertedIndexRam {
    pub fn save_to_mmap(
        self,
        index_settings: &IndexSettings,
        directory: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Vec<PathBuf>> {
        match (
            index_settings.config.storage_type,
            index_settings.config.weight_type,
            index_settings.config.quantized,
            self,
        ) {
            (
                StorageType::Mmap,
                IndexWeightType::Float32,
                true,
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: InvertedIndexMmap<f32, u8> = InvertedIndexMmap::<f32, u8>::from_ram_index(
                    Cow::Owned(inverted_index_ram),
                    directory,
                    segment_id,
                )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::Mmap,
                IndexWeightType::Float32,
                false,
                GenericInvertedIndexRam::F32InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: InvertedIndexMmap<f32, f32> =
                    InvertedIndexMmap::<f32, f32>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::Mmap,
                IndexWeightType::Float16,
                true,
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: InvertedIndexMmap<half::f16, u8> =
                    InvertedIndexMmap::<half::f16, u8>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::Mmap,
                IndexWeightType::Float16,
                false,
                GenericInvertedIndexRam::F16InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: InvertedIndexMmap<half::f16, half::f16> =
                    InvertedIndexMmap::<half::f16, half::f16>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::Mmap,
                IndexWeightType::UInt8,
                false,
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: InvertedIndexMmap<u8, u8> = InvertedIndexMmap::<u8, u8>::from_ram_index(
                    Cow::Owned(inverted_index_ram),
                    directory,
                    segment_id,
                )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::CompressedMmap,
                IndexWeightType::Float32,
                true,
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: CompressedInvertedIndexMmap<f32, u8> =
                    CompressedInvertedIndexMmap::<f32, u8>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::CompressedMmap,
                IndexWeightType::Float32,
                false,
                GenericInvertedIndexRam::F32InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: CompressedInvertedIndexMmap<f32, f32> =
                    CompressedInvertedIndexMmap::<f32, f32>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::CompressedMmap,
                IndexWeightType::Float16,
                true,
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: CompressedInvertedIndexMmap<half::f16, u8> =
                    CompressedInvertedIndexMmap::<half::f16, u8>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::CompressedMmap,
                IndexWeightType::Float16,
                false,
                GenericInvertedIndexRam::F16InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: CompressedInvertedIndexMmap<half::f16, half::f16> =
                    CompressedInvertedIndexMmap::<half::f16, half::f16>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            (
                StorageType::CompressedMmap,
                IndexWeightType::UInt8,
                false,
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram),
            ) => {
                let res: CompressedInvertedIndexMmap<u8, u8> =
                    CompressedInvertedIndexMmap::<u8, u8>::from_ram_index(
                        Cow::Owned(inverted_index_ram),
                        directory,
                        segment_id,
                    )?;
                Ok(res.files(segment_id))
            }
            _ => panic!("Not supported!"),
        }
    }
}

impl GenericInvertedIndexRamBuilder {
    pub fn new(index_settings: &IndexSettings) -> Self {
        match (index_settings.config.weight_type, index_settings.config.quantized) {
            (IndexWeightType::Float32, true) => {
                Self::F32Quantized(InvertedIndexRamBuilder::<f32, u8>::new())
            }
            (IndexWeightType::Float32, false) => {
                Self::F32NoQuantized(InvertedIndexRamBuilder::<f32, f32>::new())
            }
            (IndexWeightType::Float16, true) => {
                Self::F16Quantized(InvertedIndexRamBuilder::<half::f16, u8>::new())
            }
            (IndexWeightType::Float16, false) => {
                Self::F16NoQuantized(InvertedIndexRamBuilder::<half::f16, half::f16>::new())
            }
            (IndexWeightType::UInt8, true) => panic!("U8 Can't be quantized!"),
            (IndexWeightType::UInt8, false) => {
                Self::U8NoQuantized(InvertedIndexRamBuilder::<u8, u8>::new())
            }
        }
    }

    pub fn build(self) -> GenericInvertedIndexRam {
        match self {
            GenericInvertedIndexRamBuilder::F32NoQuantized(inverted_index_ram_builder) => {
                GenericInvertedIndexRam::F32InvertedIndexRam(inverted_index_ram_builder.build())
            }
            GenericInvertedIndexRamBuilder::F32Quantized(inverted_index_ram_builder) => {
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram_builder.build())
            }
            GenericInvertedIndexRamBuilder::F16NoQuantized(inverted_index_ram_builder) => {
                GenericInvertedIndexRam::F16InvertedIndexRam(inverted_index_ram_builder.build())
            }
            GenericInvertedIndexRamBuilder::F16Quantized(inverted_index_ram_builder) => {
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram_builder.build())
            }
            GenericInvertedIndexRamBuilder::U8NoQuantized(inverted_index_ram_builder) => {
                GenericInvertedIndexRam::U8InvertedIndexRam(inverted_index_ram_builder.build())
            }
        }
    }

    pub fn finalize(
        self,
        index_settings: &IndexSettings,
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> Vec<PathBuf> {
        match (
            index_settings.config.storage_type,
            index_settings.config.weight_type,
            index_settings.config.quantized,
        ) {
            (StorageType::Mmap, IndexWeightType::Float32, true) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::Mmap, IndexWeightType::Float32, false) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::Mmap, IndexWeightType::Float16, true) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::Mmap, IndexWeightType::Float16, false) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::Mmap, IndexWeightType::UInt8, false) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::CompressedMmap, IndexWeightType::Float32, true) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::CompressedMmap, IndexWeightType::Float32, false) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::CompressedMmap, IndexWeightType::Float16, true) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::CompressedMmap, IndexWeightType::Float16, false) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            (StorageType::CompressedMmap, IndexWeightType::UInt8, false) => {
                let inverted_index_ram = self.build();
                inverted_index_ram
                    .save_to_mmap(index_settings, directory.to_path_buf(), segment_id)
                    .unwrap_or(vec![])
            }
            _ => panic!("Ram Not Supported!"),
        }
    }

    pub fn memory_usage(&self) -> usize {
        match self {
            GenericInvertedIndexRamBuilder::F32NoQuantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.memory_usage()
            }
            GenericInvertedIndexRamBuilder::F32Quantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.memory_usage()
            }
            GenericInvertedIndexRamBuilder::F16NoQuantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.memory_usage()
            }
            GenericInvertedIndexRamBuilder::F16Quantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.memory_usage()
            }
            GenericInvertedIndexRamBuilder::U8NoQuantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.memory_usage()
            }
        }
    }

    pub fn add_row(&mut self, row_id: RowId, sparse_vector: SparseVector) -> bool {
        match self {
            GenericInvertedIndexRamBuilder::F32NoQuantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.add(row_id, sparse_vector)
            }
            GenericInvertedIndexRamBuilder::F32Quantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.add(row_id, sparse_vector)
            }
            GenericInvertedIndexRamBuilder::F16NoQuantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.add(row_id, sparse_vector)
            }
            GenericInvertedIndexRamBuilder::F16Quantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.add(row_id, sparse_vector)
            }
            GenericInvertedIndexRamBuilder::U8NoQuantized(inverted_index_ram_builder) => {
                inverted_index_ram_builder.add(row_id, sparse_vector)
            }
        }
    }
}

impl<'a> GenericPostingsIterator<'a> {
    fn remains(&self) -> usize {
        match self {
            GenericPostingsIterator::F32NoQuantized(iter) => iter.posting_list_iterator.remains(),
            GenericPostingsIterator::F32Quantized(iter) => iter.posting_list_iterator.remains(),
            GenericPostingsIterator::F16NoQuantized(iter) => iter.posting_list_iterator.remains(),
            GenericPostingsIterator::F16Quantized(iter) => iter.posting_list_iterator.remains(),
            GenericPostingsIterator::U8NoQuantized(iter) => iter.posting_list_iterator.remains(),
            GenericPostingsIterator::CompressedF32NoQuantized(iter) => {
                iter.posting_list_iterator.remains()
            }
            GenericPostingsIterator::CompressedF32Quantized(iter) => {
                iter.posting_list_iterator.remains()
            }
            GenericPostingsIterator::CompressedF16NoQuantized(iter) => {
                iter.posting_list_iterator.remains()
            }
            GenericPostingsIterator::CompressedF16Quantized(iter) => {
                iter.posting_list_iterator.remains()
            }
            GenericPostingsIterator::CompressedU8NoQuantized(iter) => {
                iter.posting_list_iterator.remains()
            }
        }
    }

    fn prune_longest_posting_list(
        &mut self,
        min_score: f32,
        right_iters: &mut [GenericPostingsIterator<'a>],
    ) -> bool {
        match self {
            GenericPostingsIterator::F32NoQuantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::F32Quantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::F16NoQuantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::F16Quantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::U8NoQuantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::CompressedF32NoQuantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::CompressedF32Quantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::CompressedF16NoQuantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::CompressedF16Quantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
            GenericPostingsIterator::CompressedU8NoQuantized(iter) => {
                let longest_posting_iterator = &mut iter.posting_list_iterator;
                prune_longest_posting_list_inner(
                    longest_posting_iterator,
                    iter.query_weight,
                    min_score,
                    right_iters,
                )
            }
        }
    }
}

fn prune_longest_posting_list_inner<OW, TW, I>(
    longest_posting_iterator: &mut I,
    query_weight: f32,
    min_score: f32,
    right_iters: &mut [GenericPostingsIterator],
) -> bool
where
    OW: QuantizedWeight,
    TW: QuantizedWeight,
    I: PostingListIteratorTrait<OW, TW>,
{
    // 获得最左侧 longest posting iter 的首个未遍历的元素
    if let Some(element) = longest_posting_iterator.peek() {
        // 在 right iterators 中找到最小的 row_id
        let min_row_id_in_right = get_min_row_id(right_iters);
        match min_row_id_in_right {
            Some(min_row_id_in_right) => {
                match min_row_id_in_right.cmp(&element.row_id) {
                    std::cmp::Ordering::Less => {
                        // 当 right set 中 min row_id 比当前 longest posting 首个 row_id 小的时候, 不可以剪枝
                        return false;
                    }
                    std::cmp::Ordering::Equal => {
                        // 当 right set 中 min row_id 和当前 longest posting 首个 row_id 一样的时候, 也不能剪枝
                        return false;
                    }
                    std::cmp::Ordering::Greater => {
                        // 当 right set 中 min row_id 比当前 longest posting 首个 row_id 大的时候, 可以剪枝
                        // 最好的情形是 longest posting 中最小的 row_id 一直到 right set 中最小的 row_id 这个区间都能够被 cut 掉

                        // 获得 longest posting 能够贡献的最大分数
                        let max_weight_in_longest = element.weight.max(element.max_next_weight);
                        let max_score_contribution =
                            TW::to_f32(max_weight_in_longest) * query_weight;

                        // 根据贡献的最大分数判断是否能够剪枝
                        if max_score_contribution <= min_score {
                            let cursor_before_pruning = longest_posting_iterator.cursor();
                            longest_posting_iterator.skip_to(min_row_id_in_right);
                            let cursor_after_pruning = longest_posting_iterator.cursor();
                            return cursor_before_pruning != cursor_after_pruning;
                        }
                    }
                }
            }
            None => {
                // min_row_id_in_right 为 None 时, 表示仅剩余左侧 1 个 posting
                // 直接判断左侧 posting 是否能够全部剪掉就行
                let max_weight_in_longest = element.weight.max(element.max_next_weight);
                let max_score_contribution = TW::to_f32(max_weight_in_longest) * query_weight;
                if max_score_contribution <= min_score {
                    longest_posting_iterator.skip_to_end();
                    return true;
                }
            }
        }
    }
    false
}

fn get_min_row_id(iters: &mut [GenericPostingsIterator]) -> Option<RowId> {
    iters
        .iter_mut()
        .filter_map(|iter| match iter {
            GenericPostingsIterator::F32NoQuantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::F32Quantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::F16NoQuantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::F16Quantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::U8NoQuantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::CompressedF32NoQuantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::CompressedF32Quantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::CompressedF16NoQuantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::CompressedF16Quantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
            GenericPostingsIterator::CompressedU8NoQuantized(iter) => {
                iter.posting_list_iterator.peek().map(|e| e.row_id)
            }
        })
        .min()
}

fn process_posting_list_for_inverted_index<'a, OW, TW>(
    inverted_index_mmap: &'a InvertedIndexMmap<OW, TW>,
    id: &DimId,
    query_weight_offset: usize,
    query_values: &[DimWeight],
    postings_iterators: &mut Vec<
        IndexedPostingListIterator<OW, TW, PostingListIterator<'a, TW, OW>>,
    >,
    min_row_id: &mut RowId,
    max_row_id: &mut RowId,
) where
    OW: QuantizedWeight,
    TW: QuantizedWeight,
{
    if let Some(mut it) = inverted_index_mmap.iter(id) {
        if let (Some(first), Some(last_id)) = (it.peek(), it.last_id()) {
            *min_row_id = std::cmp::min(*min_row_id, first.row_id);
            *max_row_id = std::cmp::max(*max_row_id, last_id);

            let query_index = *id;
            let query_weight = query_values[query_weight_offset];

            let iterator = IndexedPostingListIterator {
                posting_list_iterator: it,
                query_index,
                query_weight,
                _ow: PhantomData,
                _tw: PhantomData,
            };
            postings_iterators.push(iterator);
        }
    }
}

fn process_posting_list_for_compressed_inverted_index<'a, OW, TW>(
    compressed_inverted_index_mmap: &'a CompressedInvertedIndexMmap<OW, TW>,
    id: &DimId,
    query_weight_offset: usize,
    query_values: &[DimWeight],
    compressed_postings_iterators: &mut Vec<
        IndexedPostingListIterator<OW, TW, CompressedPostingListIterator<'a, TW, OW>>,
    >,
    min_row_id: &mut RowId,
    max_row_id: &mut RowId,
) where
    OW: QuantizedWeight,
    TW: QuantizedWeight,
{
    if let Some(mut it) = compressed_inverted_index_mmap.iter(id) {
        if let (Some(first), Some(last_id)) = (it.peek(), it.last_id()) {
            *min_row_id = std::cmp::min(*min_row_id, first.row_id);
            *max_row_id = std::cmp::max(*max_row_id, last_id);

            let query_index = *id;
            let query_weight = query_values[query_weight_offset];

            let iterator = IndexedPostingListIterator {
                posting_list_iterator: it,
                query_index,
                query_weight,
                _ow: PhantomData,
                _tw: PhantomData,
            };
            compressed_postings_iterators.push(iterator);
        }
    }
}

impl GenericInvertedIndexMmapType {
    fn generate_postings_iterators(
        &self,
        dim_id: &u32,
        dim_id_idx: usize,
        query_values: &[DimWeight],
        min_row_id: &mut RowId,
        max_row_id: &mut RowId,
    ) -> Option<Vec<GenericPostingsIterator>> {
        let mut postings_iterators: Vec<GenericPostingsIterator> = vec![];
        match self {
            GenericInvertedIndexMmapType::InvertedIndexMmapF32NoQuantized(inverted_index_mmap) => {
                let mut postings_iterators_f32_no_quantized: Vec<
                    IndexedPostingListIterator<f32, f32, PostingListIterator<'_, f32, f32>>,
                > = Vec::new();
                process_posting_list_for_inverted_index(
                    &inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut postings_iterators_f32_no_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in postings_iterators_f32_no_quantized {
                    postings_iterators.push(GenericPostingsIterator::F32NoQuantized(item));
                }
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF32Quantized(inverted_index_mmap) => {
                let mut postings_iterators_f32_quantized: Vec<
                    IndexedPostingListIterator<f32, u8, PostingListIterator<'_, u8, f32>>,
                > = Vec::new();
                process_posting_list_for_inverted_index(
                    &inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut postings_iterators_f32_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in postings_iterators_f32_quantized {
                    postings_iterators.push(GenericPostingsIterator::F32Quantized(item));
                }
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF16NoQuantized(inverted_index_mmap) => {
                let mut postings_iterators_f16_no_quantized: Vec<
                    IndexedPostingListIterator<
                        half::f16,
                        half::f16,
                        PostingListIterator<'_, half::f16, half::f16>,
                    >,
                > = Vec::new();
                process_posting_list_for_inverted_index(
                    &inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut postings_iterators_f16_no_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in postings_iterators_f16_no_quantized {
                    postings_iterators.push(GenericPostingsIterator::F16NoQuantized(item));
                }
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF16Quantized(inverted_index_mmap) => {
                let mut postings_iterators_f16_quantized: Vec<
                    IndexedPostingListIterator<
                        half::f16,
                        u8,
                        PostingListIterator<'_, u8, half::f16>,
                    >,
                > = Vec::new();
                process_posting_list_for_inverted_index(
                    &inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut postings_iterators_f16_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in postings_iterators_f16_quantized {
                    postings_iterators.push(GenericPostingsIterator::F16Quantized(item));
                }
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapU8NoQuantized(inverted_index_mmap) => {
                let mut postings_iterators_u8_no_quantized: Vec<
                    IndexedPostingListIterator<u8, u8, PostingListIterator<'_, u8, u8>>,
                > = Vec::new();
                process_posting_list_for_inverted_index(
                    &inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut postings_iterators_u8_no_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in postings_iterators_u8_no_quantized {
                    postings_iterators.push(GenericPostingsIterator::U8NoQuantized(item));
                }
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32NoQuantized(
                compressed_inverted_index_mmap,
            ) => {
                let mut compressed_postings_iterators_f32_no_quantized: Vec<
                    IndexedPostingListIterator<
                        f32,
                        f32,
                        CompressedPostingListIterator<'_, f32, f32>,
                    >,
                > = Vec::new();
                process_posting_list_for_compressed_inverted_index(
                    &compressed_inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut compressed_postings_iterators_f32_no_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in compressed_postings_iterators_f32_no_quantized {
                    postings_iterators
                        .push(GenericPostingsIterator::CompressedF32NoQuantized(item));
                }
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32Quantized(
                compressed_inverted_index_mmap,
            ) => {
                let mut compressed_postings_iterators_f32_quantized: Vec<
                    IndexedPostingListIterator<f32, u8, CompressedPostingListIterator<'_, u8, f32>>,
                > = Vec::new();
                process_posting_list_for_compressed_inverted_index(
                    &compressed_inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut compressed_postings_iterators_f32_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in compressed_postings_iterators_f32_quantized {
                    postings_iterators.push(GenericPostingsIterator::CompressedF32Quantized(item));
                }
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16NoQuantized(
                compressed_inverted_index_mmap,
            ) => {
                let mut compressed_postings_iterators_f16_no_quantized: Vec<
                    IndexedPostingListIterator<
                        half::f16,
                        half::f16,
                        CompressedPostingListIterator<'_, half::f16, half::f16>,
                    >,
                > = Vec::new();
                process_posting_list_for_compressed_inverted_index(
                    &compressed_inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut compressed_postings_iterators_f16_no_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in compressed_postings_iterators_f16_no_quantized {
                    postings_iterators
                        .push(GenericPostingsIterator::CompressedF16NoQuantized(item));
                }
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16Quantized(
                compressed_inverted_index_mmap,
            ) => {
                let mut compressed_postings_iterators_f16_quantized: Vec<
                    IndexedPostingListIterator<
                        half::f16,
                        u8,
                        CompressedPostingListIterator<'_, u8, half::f16>,
                    >,
                > = Vec::new();
                process_posting_list_for_compressed_inverted_index(
                    &compressed_inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut compressed_postings_iterators_f16_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in compressed_postings_iterators_f16_quantized {
                    postings_iterators.push(GenericPostingsIterator::CompressedF16Quantized(item));
                }
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapU8NoQuantized(
                compressed_inverted_index_mmap,
            ) => {
                let mut compressed_postings_iterators_u8_no_quantized: Vec<
                    IndexedPostingListIterator<u8, u8, CompressedPostingListIterator<'_, u8, u8>>,
                > = Vec::new();
                process_posting_list_for_compressed_inverted_index(
                    &compressed_inverted_index_mmap,
                    dim_id,
                    dim_id_idx,
                    query_values,
                    &mut compressed_postings_iterators_u8_no_quantized,
                    min_row_id,
                    max_row_id,
                );
                for item in compressed_postings_iterators_u8_no_quantized {
                    postings_iterators.push(GenericPostingsIterator::CompressedU8NoQuantized(item));
                }
            }
            _ => postings_iterators = vec![],
        };
        if postings_iterators.len() == 0 {
            return None;
        } else {
            return Some(postings_iterators);
        }
    }

    pub fn metrics(&self) -> InvertedIndexMetrics {
        match self {
            GenericInvertedIndexMmapType::InvertedIndexMmapF32NoQuantized(inverted_index_mmap) => {
                inverted_index_mmap.metrics()
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF32Quantized(inverted_index_mmap) => {
                inverted_index_mmap.metrics()
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF16NoQuantized(inverted_index_mmap) => {
                inverted_index_mmap.metrics()
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapF16Quantized(inverted_index_mmap) => {
                inverted_index_mmap.metrics()
            }
            GenericInvertedIndexMmapType::InvertedIndexMmapU8NoQuantized(inverted_index_mmap) => {
                inverted_index_mmap.metrics()
            }
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32NoQuantized(
                compressed_inverted_index_mmap,
            ) => compressed_inverted_index_mmap.metrics(),
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF32Quantized(
                compressed_inverted_index_mmap,
            ) => compressed_inverted_index_mmap.metrics(),
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16NoQuantized(
                compressed_inverted_index_mmap,
            ) => compressed_inverted_index_mmap.metrics(),
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapF16Quantized(
                compressed_inverted_index_mmap,
            ) => compressed_inverted_index_mmap.metrics(),
            GenericInvertedIndexMmapType::CompressedInvertedIndexMmapU8NoQuantized(
                compressed_inverted_index_mmap,
            ) => compressed_inverted_index_mmap.metrics(),
        }
    }

    pub fn open_from(
        index_path: &PathBuf,
        segment_id: Option<&str>,
        index_settings: &IndexSettings,
    ) -> crate::Result<Self> {
        match (
            index_settings.config.storage_type,
            index_settings.config.weight_type,
            index_settings.config.quantized,
        ) {
            (StorageType::Mmap, IndexWeightType::Float32, true) => {
                let res = InvertedIndexMmap::<f32, u8>::open(&index_path, segment_id)?;
                Ok(Self::InvertedIndexMmapF32Quantized(res))
            }
            (StorageType::Mmap, IndexWeightType::Float32, false) => {
                let res = InvertedIndexMmap::<f32, f32>::open(&index_path, segment_id)?;
                Ok(Self::InvertedIndexMmapF32NoQuantized(res))
            }
            (StorageType::Mmap, IndexWeightType::Float16, true) => {
                let res = InvertedIndexMmap::<half::f16, u8>::open(&index_path, segment_id)?;
                Ok(Self::InvertedIndexMmapF16Quantized(res))
            }
            (StorageType::Mmap, IndexWeightType::Float16, false) => {
                let res = InvertedIndexMmap::<half::f16, half::f16>::open(&index_path, segment_id)?;
                Ok(Self::InvertedIndexMmapF16NoQuantized(res))
            }
            (StorageType::Mmap, IndexWeightType::UInt8, true) => panic!("U8 Can't Be Quantized!"),
            (StorageType::Mmap, IndexWeightType::UInt8, false) => {
                let res = InvertedIndexMmap::<u8, u8>::open(&index_path, segment_id)?;
                Ok(Self::InvertedIndexMmapU8NoQuantized(res))
            }
            (StorageType::CompressedMmap, IndexWeightType::Float32, true) => {
                let res = CompressedInvertedIndexMmap::<f32, u8>::open(&index_path, segment_id)?;
                Ok(Self::CompressedInvertedIndexMmapF32Quantized(res))
            }
            (StorageType::CompressedMmap, IndexWeightType::Float32, false) => {
                let res = CompressedInvertedIndexMmap::<f32, f32>::open(&index_path, segment_id)?;
                Ok(Self::CompressedInvertedIndexMmapF32NoQuantized(res))
            }
            (StorageType::CompressedMmap, IndexWeightType::Float16, true) => {
                let res =
                    CompressedInvertedIndexMmap::<half::f16, u8>::open(&index_path, segment_id)?;
                Ok(Self::CompressedInvertedIndexMmapF16Quantized(res))
            }
            (StorageType::CompressedMmap, IndexWeightType::Float16, false) => {
                let res = CompressedInvertedIndexMmap::<half::f16, half::f16>::open(
                    &index_path,
                    segment_id,
                )?;
                Ok(Self::CompressedInvertedIndexMmapF16NoQuantized(res))
            }
            (StorageType::CompressedMmap, IndexWeightType::UInt8, true) => {
                panic!("U8 Can't Be Quantized!")
            }
            (StorageType::CompressedMmap, IndexWeightType::UInt8, false) => {
                let res = CompressedInvertedIndexMmap::<u8, u8>::open(&index_path, segment_id)?;
                Ok(Self::CompressedInvertedIndexMmapU8NoQuantized(res))
            }
            (StorageType::Ram, IndexWeightType::Float32, true) => panic!("Ram Not Supported!"),
            (StorageType::Ram, IndexWeightType::Float32, false) => panic!("Ram Not Supported!"),
            (StorageType::Ram, IndexWeightType::Float16, true) => panic!("Ram Not Supported!"),
            (StorageType::Ram, IndexWeightType::Float16, false) => panic!("Ram Not Supported!"),
            (StorageType::Ram, IndexWeightType::UInt8, true) => panic!("Ram Not Supported!"),
            (StorageType::Ram, IndexWeightType::UInt8, false) => panic!("Ram Not Supported!"),
        }
    }
}
