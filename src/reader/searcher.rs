use std::collections::BTreeMap;
use std::sync::Arc;
use std::{fmt, io};

use census::TrackedObject;

use crate::common::executor::Executor;
use crate::core::{SparseRowContent, SparseVector, TopK};
use crate::ffi::ScoredPointOffset;
use crate::index::{Index, SegmentId, SegmentReader};
use crate::{Opstamp, RowId};

/// Identifies the searcher generation accessed by a [`Searcher`].
///
/// While this might seem redundant, a [`SearcherGeneration`] contains
/// both a `generation_id` AND a list of `(SegmentId, DeleteOpstamp)`.
///
/// This is on purpose. This object is used by the [`Warmer`](crate::reader::Warmer) API.
/// Having both information makes it possible to identify which
/// artifact should be refreshed or garbage collected.
///
/// Depending on the use case, `Warmer`'s implementers can decide to
/// produce artifacts per:
/// - `generation_id` (e.g. some searcher level aggregates)
/// - `(segment_id, delete_opstamp)` (e.g. segment level aggregates)
/// - `segment_id` (e.g. for immutable document level information)
/// - `(generation_id, segment_id)` (e.g. for consistent dynamic column)
/// - ...
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearcherGeneration {
    segments: BTreeMap<SegmentId, Option<Opstamp>>,
    generation_id: u64,
}

impl SearcherGeneration {
    /// 从一组 SegmentReader 创建 SearcherGeneration 对象
    ///
    /// `segment_readers`: 多个 SegmentReader
    /// `generation_id`: 当前 SearchGeneration 唯一标识符
    pub(crate) fn from_segment_readers(
        segment_readers: &[SegmentReader],
        generation_id: u64,
    ) -> Self {
        // 存储 segment_id 和 delete opstamp 的映射
        let mut segment_id_to_del_opstamp = BTreeMap::new();
        for segment_reader in segment_readers {
            segment_id_to_del_opstamp
                .insert(segment_reader.segment_id(), None);
        }
        Self {
            segments: segment_id_to_del_opstamp,
            generation_id,
        }
    }

    /// 返回 SearcherGeneration 的 generation_id
    pub fn generation_id(&self) -> u64 {
        self.generation_id
    }

    /// 返回 `(SegmentId -> DeleteOpstamp)` 的映射
    pub fn segments(&self) -> &BTreeMap<SegmentId, Option<Opstamp>> {
        &self.segments
    }
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
#[derive(Clone)]
pub struct Searcher {
    inner: Arc<SearcherInner>,
}

impl Searcher {
    /// 返回 Searcher 使用的 index
    pub fn index(&self) -> &Index {
        &self.inner.index
    }

    /// 返回当前 [`Searcher`] 对 [`SearcherGeneration`] 的引用, 即标识当前 Searcher 持有的快照 snapshot 版本
    pub fn generation(&self) -> &SearcherGeneration {
        self.inner.generation.as_ref()
    }

    /// TODO 从索引中获取 row_id 对应的 RowContent 内容
    pub fn row_content(&self, row_id: RowId) -> crate::Result<SparseRowContent> {
        todo!()
    }

    /// 返回 Index 存储数据的行数
    pub fn num_rows(&self) -> u64 {
        self.inner
            .segment_readers
            .iter()
            .map(|segment_reader| u64::from(segment_reader.rows_count()))
            .sum::<u64>()
    }

    /// 返回当前 `Searcher` 使用的所有 [`SegmentReader`]
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.inner.segment_readers
    }

    /// 提供 segment idx, 返回对应的 `SegmentReader`
    pub fn segment_reader(&self, segment_ord: u32) -> &SegmentReader {
        &self.inner.segment_readers[segment_ord as usize]
    }

    /// 执行暴力搜索
    ///
    /// - `sparse_vector`: 提供的用来 query 的 sparse_vector
    /// - `limits`: 搜索结果候选数量
    ///
    /// TODO 后续可以考虑将返回值和 ffi 内部的数据结构进行分离，减少依赖
    pub fn plain_search(
        &self,
        sparse_vector: SparseVector,
        limits: u32,
    ) -> crate::Result<Vec<ScoredPointOffset>> {
        let executor = self.inner.index.search_executor();
        self.search_with_executor(sparse_vector, limits, executor, true)
    }

    /// 执行优化的搜索
    ///
    /// - `sparse_vector`: 提供的用来 query 的 sparse_vector
    /// - `limits`: 搜索结果候选数量
    pub fn search(
        &self,
        sparse_vector: SparseVector,
        limits: u32,
    ) -> crate::Result<Vec<ScoredPointOffset>> {
        let executor = self.inner.index.search_executor();
        self.search_with_executor(sparse_vector, limits, executor, false)
    }

    /// Same as [`search(...)`](Searcher::search) but multithreaded.
    ///
    /// The current implementation is rather naive :
    /// multithreading is by splitting search into as many task
    /// as there are segments.
    ///
    /// It is powerless at making search faster if your index consists in
    /// one large segment.
    ///
    /// Also, keep in my multithreading a single query on several
    /// threads will not improve your throughput. It can actually
    /// hurt it. It will however, decrease the average response time.
    pub fn search_with_executor(
        &self,
        sparse_vector: SparseVector,
        limits: u32,
        executor: &Executor,
        brute_force: bool,
    ) -> crate::Result<Vec<ScoredPointOffset>> {

        let mut topk_combine = TopK::new(limits as usize);
        let results: Vec<TopK> = executor.map(|seg_reader|{
            if brute_force {
                seg_reader.brute_force_search(sparse_vector.clone(), limits)
            } else {
                seg_reader.search(sparse_vector.clone(), limits)
            }
        }, self.segment_readers().iter())?;
        for res in results {
            topk_combine.combine(&res);
        }

        Ok(topk_combine.into_vec())
    }
}

impl From<Arc<SearcherInner>> for Searcher {
    fn from(inner: Arc<SearcherInner>) -> Self {
        Searcher { inner }
    }
}

/// Holds a list of `SegmentReader`s ready for search.
///
/// It guarantees that the `Segment` will not be removed before
/// the destruction of the `Searcher`.
/// TODO SegmentReader 可以考虑将生命周期这些声明给丢弃掉
pub(crate) struct SearcherInner {
    index: Index,
    segment_readers: Vec<SegmentReader>,
    generation: TrackedObject<SearcherGeneration>,
}

impl SearcherInner {
    pub(crate) fn new(
        index: Index,
        segment_readers: Vec<SegmentReader>,
        generation: TrackedObject<SearcherGeneration>,
    ) -> io::Result<SearcherInner> {
        assert_eq!(
            &segment_readers
                .iter()
                .map(|reader| (reader.segment_id(), None))
                .collect::<BTreeMap<_, _>>(),
            generation.segments(),
            "Set of segments referenced by this Searcher and its SearcherGeneration must match"
        );

        Ok(SearcherInner {
            index,
            segment_readers,
            generation,
        })
    }
}

impl fmt::Debug for Searcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segment_ids = self
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect::<Vec<_>>();
        write!(f, "Searcher({segment_ids:?})")
    }
}
