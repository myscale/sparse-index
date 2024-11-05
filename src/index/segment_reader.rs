use super::{Segment, SegmentId};
use crate::core::index_searcer::IndexSearcher;
use crate::core::{InvertedIndex, InvertedIndexMmap, SparseVector, TopK};
use crate::directory::Directory;
use crate::sparse_index::SparseIndexType;
use crate::RowId;
use std::fmt;

/// 访问 Segment 的入口
// TODO 将 PostingList 相关的东西都放进去
// TODO PostingList 相关的东西甚至可以直接序列化为一些小文件，这样就不需要序列化成为一整个 .data 文件了
// TODO 可以考虑将所有的 sparse vector 作为原始数据直接存储到一个单独的文件里面，这样如果需要返回原始数据就不需要通过查询 inverted index 去拼凑了
#[derive(Clone)]
pub struct SegmentReader {
    index_searcher: IndexSearcher,

    segment_id: SegmentId,

    rows_count: RowId,
}

/// metrics
impl SegmentReader {
    /// segment 中包含的所有元素行数 (存活 + 标记删除)
    pub fn rows_count(&self) -> RowId {
        self.rows_count as RowId
    }

    /// Returns the segment id
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// 获得 SegmentReader 对应的底层索引
    pub fn get_inverted_index(&self) -> &InvertedIndexMmap {
        self.index_searcher.get_inverted_index()
    }
}

impl SegmentReader {
    /// 初始化 SegmentReader
    ///
    /// - `segment`: 为传入的 segment 创建 SegmentReader
    pub fn open(segment: &Segment) -> crate::Result<SegmentReader> {
        let rows_count: RowId = segment.meta().rows_count();
        let index_path = segment.index().directory().get_path();

        // TODO 目前仅允许 mmap 类型的 reader，后续扩充 内存类型等等，最底层进行向上的抽象封装
        assert_eq!(
            segment.index().index_settings.config.index_type,
            SparseIndexType::Mmap
        );

        let inverted_index: InvertedIndexMmap =
            InvertedIndexMmap::open(&index_path, Some(&segment.id().uuid_string()))?;

        Ok(SegmentReader {
            index_searcher: IndexSearcher::new(inverted_index),
            segment_id: segment.id(),
            rows_count,
        })
    }

    // 在 Segments 上执行 search
    pub fn search(&self, query: SparseVector, limits: u32) -> crate::Result<TopK> {
        Ok(self.index_searcher.search(query, limits))
    }

    // Segments 上执行 brute force search
    pub fn brute_force_search(&self, query: SparseVector, limits: u32) -> crate::Result<TopK> {
        Ok(self.index_searcher.plain_search(query, limits))
    }
}

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentReader")
            .field("segment_id", &self.segment_id())
            .field("rows_count", &self.rows_count())
            .finish()
    }
}
