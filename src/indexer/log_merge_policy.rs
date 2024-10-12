use std::cmp;

use itertools::Itertools;
use log::debug;

use super::merge_policy::{MergeCandidate, MergePolicy};
use crate::{index::SegmentMeta, RowId};

// 默认的级别日志大小
const DEFAULT_LEVEL_LOG_SIZE: f64 = 0.75;
// 默认的最小层大小
const DEFAULT_MIN_LAYER_SIZE: u32 = 10_000;
// 默认的最小合并段数
const DEFAULT_MIN_NUM_SEGMENTS_IN_MERGE: usize = 8;
// 默认的合并前最大文档数
const DEFAULT_MAX_DOCS_BEFORE_MERGE: usize = 10_000_000;


/// `LogMergePolicy` tries to merge segments that have a similar number of
/// documents.
/// `LogMergePolicy` 尝试合并具有相似文档数量的段。
#[derive(Debug, Clone)]
pub struct LogMergePolicy {
    min_num_segments: usize,          // 最小段数
    max_docs_before_merge: usize,     // 合并前最大文档数
    min_layer_size: u32,              // 最小层大小
    level_log_size: f64,              // 级别日志大小
}

impl LogMergePolicy {
    fn clip_min_size(&self, size: u32) -> u32 {
        cmp::max(self.min_layer_size, size)
    }

    /// Set the minimum number of segments that may be merged together.
    pub fn set_min_num_segments(&mut self, min_num_segments: usize) {
        self.min_num_segments = min_num_segments;
    }

    /// Set the maximum number docs in a segment for it to be considered for
    /// merging. A segment can still reach more than max_docs, by merging many
    /// smaller ones.
    pub fn set_max_docs_before_merge(&mut self, max_docs_merge_size: usize) {
        self.max_docs_before_merge = max_docs_merge_size;
    }

    /// Set the minimum segment size under which all segment belong
    /// to the same level.
    pub fn set_min_layer_size(&mut self, min_layer_size: u32) {
        self.min_layer_size = min_layer_size;
    }

    /// Set the ratio between two consecutive levels.
    ///
    /// Segments are grouped in levels according to their sizes.
    /// These levels are defined as intervals of exponentially growing sizes.
    /// level_log_size define the factor by which one should multiply the limit
    /// to reach a level, in order to get the limit to reach the following
    /// level.
    pub fn set_level_log_size(&mut self, level_log_size: f64) {
        self.level_log_size = level_log_size;
    }
}


impl MergePolicy for LogMergePolicy {
    // 计算合并候选项
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        // 按大小排序的段
        let size_sorted_segments: Vec<&SegmentMeta> = segments
            .iter()
            .filter(|seg| seg.alive_rows_count() <= (self.max_docs_before_merge as RowId))
            .sorted_by_key(|seg| std::cmp::Reverse(seg.rows_count()))
            .collect::<Vec<&SegmentMeta>>();

        if size_sorted_segments.is_empty() {
            return vec![];
        }

        let mut current_max_log_size = f64::MAX;
        let mut levels = vec![];
        // 按日志大小分组
        for (_, merge_group) in &size_sorted_segments.into_iter().group_by(|segment| {
            let segment_log_size = f64::from(self.clip_min_size(segment.alive_rows_count())).log2();
            if segment_log_size < (current_max_log_size - self.level_log_size) {
                // update current_max_log_size to create a new group
                current_max_log_size = segment_log_size;
            }
            // return current_max_log_size to be grouped to the current group
            current_max_log_size
        }) {
            levels.push(merge_group.collect::<Vec<&SegmentMeta>>());
        }

        // 过滤并生成合并候选项
        let candidates: Vec<MergeCandidate> = levels
            .iter()
            .filter(|level| {
                level.len() >= self.min_num_segments
            })
            .map(|segments| MergeCandidate(segments.iter().map(|&seg| seg.id()).collect()))
            .collect();

        debug!("[compute_merge_candidates] input segments size:{}, generate candidates size:{}", segments.len(), candidates.clone().len());
        return candidates;
    }
}

impl Default for LogMergePolicy {
    // 默认的 LogMergePolicy 配置
    fn default() -> LogMergePolicy {
        LogMergePolicy {
            min_num_segments: DEFAULT_MIN_NUM_SEGMENTS_IN_MERGE,
            max_docs_before_merge: DEFAULT_MAX_DOCS_BEFORE_MERGE,
            min_layer_size: DEFAULT_MIN_LAYER_SIZE,
            level_log_size: DEFAULT_LEVEL_LOG_SIZE,
        }
    }
}
