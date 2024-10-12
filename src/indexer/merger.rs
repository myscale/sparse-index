use std::cmp::min;

use crate::{
    common::alive_bitset::AliveBitSet, core::{DimId, DimWeight, ElementOffsetType, InvertedIndexImmutableRam, InvertedIndexRam, PostingElementEx, PostingList, DEFAULT_MAX_NEXT_WEIGHT}, index::{Segment, SegmentReader}, RowId
};
use itertools::Itertools;
use log::debug;

/// Segment's max doc must be `< MAX_DOC_LIMIT`.
///
/// We do not allow segments with more than
pub const MAX_DOC_LIMIT: u32 = 1 << 31;

fn estimate_total_num_tokens_in_single_segment(reader: &SegmentReader) -> crate::Result<u64> {
    // TODO 通过 segment reader 拿到包含的 row 数量，是否需要细致到每个 dim 元素？

    return Ok(100);
}

// 获取一组 segment 所有的 tokens（所有的 dim 元素）
fn estimate_total_num_tokens(readers: &[SegmentReader]) -> crate::Result<u64> {
    let mut total_num_tokens: u64 = 0;
    for reader in readers {
        total_num_tokens += estimate_total_num_tokens_in_single_segment(reader)?;
    }
    Ok(total_num_tokens)
}

pub struct IndexMerger {
    pub(crate) readers: Vec<SegmentReader>,
}


fn merge_posting_lists(lists: Vec<PostingList>) -> PostingList {
    let mut merged: PostingList = PostingList { elements: Vec::new() };
    // 为每个 PostingList 维护一个当前索引，初始化为每个 PostingList 的最后一个位置
    let mut indices = lists.iter().map(|list| list.size()).collect::<Vec<_>>();
    // TODO 确认下 qdrant 默认的 max_next_weight 是怎么做的
    let mut cur_max_next_weight = DEFAULT_MAX_NEXT_WEIGHT;

    // 当所有 PostingList 的索引都为 0 时，表示合并完成
    while indices.iter().any(|&i| i > 0) {
        let mut max_index = None; // 记录是哪个 Posting List
        let mut max_row_id = RowId::MIN;    // 记录最大 row id

        // 找到所有 PostingList 中当前最大的 row_id 以及对应的 PostingList 下标
        for (i, &index) in indices.iter().enumerate() {
            if index > 0 && lists[i].get(index - 1).row_id > max_row_id {
                max_index = Some(i);
                max_row_id = lists[i].get(index - 1).row_id;
            }
        }

        if let Some(max_idx) = max_index {
            // 将当前最大 row_id 对应的元素添加到结果中
            let mut element = lists[max_idx].get(indices[max_idx] - 1).clone();
            element.max_next_weight = cur_max_next_weight;
            merged.elements.push(element);

            // 更新 cur_max_next_weight
            cur_max_next_weight = cur_max_next_weight.max(lists[max_idx].get(indices[max_idx] - 1).weight);

            indices[max_idx] -= 1;
        }
    }

    // 将结果反转，使其按照 row_id 从小到大排序
    merged.elements.reverse();
    merged
}

fn merge_segments(segments: Vec<Vec<PostingList>>) -> Vec<PostingList> {

    let mut min_row_id = RowId::MAX;
    let mut max_row_id = RowId::MIN;

    let max_dim = segments.iter().map(|s| s.len()).max().unwrap_or(0);
    let mut merged_segment: Vec<PostingList> = Vec::with_capacity(max_dim);

    for dim in 0..max_dim {
        let mut posting_lists = Vec::new();
        for segment in &segments {
            if dim < segment.len() {
                // TODO 这里的 clone 操作可能相当消耗资源
                posting_lists.push(segment[dim].clone());
            }
        }
        let res = merge_posting_lists(posting_lists);
        if res.elements.len()!=0 {
            min_row_id = min(min_row_id, res.elements[0].row_id);   
            max_row_id = min(max_row_id, res.elements[res.elements.len()-1].row_id);   
        }
        merged_segment.push(res);
    }

    merged_segment
}


impl IndexMerger {
    pub fn open(segments: &[Segment]) -> crate::Result<IndexMerger> {
        // WARN 编译器隐式转换, map 操作应该是返回 Vec<Result>，但是返回的是 Result<Vec>，这是编译器的隐式转换，如果所有的元素都是 Ok，就正常，若有一个是 Error，就会立刻返回 Error
        let segment_readers: Vec<SegmentReader> = segments.iter().map(|seg|{SegmentReader::open(seg)}).collect::<crate::Result<_>>()?;

        Ok(IndexMerger {
            readers: segment_readers
        })
    }

    /// merge
    pub fn merge(&self) -> crate::Result<InvertedIndexRam> {

        let total_rows = self.readers.iter().map(|reader|{
            reader.get_inverted_index().file_header.vector_count
        }).sum::<usize>();

        let min_row_id = self.readers.iter().map(|reader| {
            reader.get_inverted_index().file_header.min_row_id
        }).min();

        let max_row_id = self.readers.iter().map(|reader| {
            reader.get_inverted_index().file_header.max_row_id
        }).min();

        let postings = self.readers.iter().map(|reader: &SegmentReader|{
            let mmap_inv_idx = reader.get_inverted_index();
            let mut mmap_postings: Vec<PostingList> = Vec::with_capacity(mmap_inv_idx.file_header.posting_count);
            for i in 0..mmap_inv_idx.file_header.posting_count as DimId {
                let posting_list: &[PostingElementEx] = mmap_inv_idx.get(&i).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Posting list {} not found", i),
                    )
                })?;
                mmap_postings.push(PostingList {
                    elements: posting_list.to_owned(),
                });
            }
            Ok(mmap_postings)
        }).collect::<crate::Result<Vec<Vec<PostingList>>>>()?;

        let merged = merge_segments(postings);
        
        return Ok(
            InvertedIndexRam {
                postings: merged,
                vector_count: total_rows,
                min_row_id: min_row_id.unwrap_or(RowId::MAX),
                max_row_id: max_row_id.unwrap_or(0),
            }
        );
    }
}
