use std::borrow::{BorrowMut, Cow};
use std::collections::HashSet;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use log::{debug, error, info, warn};
use rayon::{ThreadPool, ThreadPoolBuilder};

use super::segment_manager::SegmentManager;
use crate::core::{InvertedIndex, InvertedIndexMmap};
use crate::directory::{Directory, DirectoryClone, GarbageCollectionResult};
use crate::future_result::FutureResult;
use crate::index::{Index, IndexMeta, Segment, SegmentId, SegmentMeta};
use crate::indexer::merge_operation::MergeOperationInventory;
use crate::indexer::merger::IndexMerger;
use crate::indexer::segment_manager::SegmentsStatus;
use crate::indexer::stamper::Stamper;
use crate::indexer::{
    DefaultMergePolicy, MergeCandidate, MergeOperation, MergePolicy, SegmentEntry,
};
use crate::{Opstamp, META_FILEPATH};

const NUM_MERGE_THREADS: usize = 4;

/// 保存 index meta.json 文件
pub fn save_metas(metas: &IndexMeta, directory: &dyn Directory) -> crate::Result<()> {
    let mut buffer = serde_json::to_vec_pretty(metas)?;
    // Just adding a new line at the end of the buffer.
    writeln!(&mut buffer)?;
    directory.sync_directory()?;
    directory.atomic_write(&META_FILEPATH, &buffer[..])?;
    debug!(
        "[{}] - [save_metas] segments size: {}, opstamp: {}, payload: {:?}",
        thread::current().name().unwrap_or_default(),
        metas.segments.len(),
        metas.opstamp,
        metas.payload
    );
    Ok(())
}

/// 负责处理所有 segment 更新操作
/// 所有的处理均在 1 个 thread 上进行, 使用一个 共享队列 消费任务
#[derive(Clone)]
pub(crate) struct SegmentUpdater(Arc<InnerSegmentUpdater>);

pub(crate) struct InnerSegmentUpdater {
    /// 存储当前活动的 IndexMeta 副本, 避免在 SegmentUpdater 中每次需要时都从文件加载 </br>
    /// 该副本始终保持最新, 因为所有的更新都通过唯一活跃的 SegmentUpdater 进行
    active_index_meta: RwLock<Arc<IndexMeta>>,

    /// segment updater thread pool size = 1
    pool: ThreadPool,

    /// merge thread pool size = 4
    merge_thread_pool: ThreadPool,

    index: Index,

    /// 管理 Uncommitted 和 Committed 状态的 Segments
    segment_manager: SegmentManager,

    /// Merge 策略
    merge_policy: RwLock<Arc<dyn MergePolicy>>,
    killed: AtomicBool,
    stamper: Stamper,

    /// MergeOperation 的仓库
    merge_operations: MergeOperationInventory,
}

impl Deref for SegmentUpdater {
    type Target = InnerSegmentUpdater;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn garbage_collect_files(
    segment_updater: SegmentUpdater,
) -> crate::Result<GarbageCollectionResult> {
    info!(
        "[{}] - [garbage_collect_files] start running GC with `{}` files.",
        thread::current().name().unwrap_or_default(),
        segment_updater.list_files().len()
    );
    let mut index = segment_updater.index.clone();
    index
        .directory_mut()
        .garbage_collect(move || segment_updater.list_files())
}

/// Merges a list of segments the list of segment givens in the `segment_entries`.
/// This function happens in the calling thread and is computationally expensive.
fn merge(
    index: &Index,
    mut segment_entries: Vec<SegmentEntry>,
) -> crate::Result<Option<SegmentEntry>> {
    let total_rows_count = segment_entries
        .iter()
        .map(|segment| segment.meta().rows_count() as u64)
        .sum::<u64>();
    if total_rows_count == 0 {
        return Ok(None);
    }
    info!(
        "[start_merge][merge] future merge rows count: {:?}",
        total_rows_count
    );

    // 初始化 merge 后的 segment
    let merged_segment = index.new_segment();
    let segment_id = merged_segment.id().uuid_string();

    // 通过函数传入的一组 segment_entries 获取对应的一组 Segment 对象
    let segments: Vec<Segment> = segment_entries
        .iter()
        .map(|segment_entry| index.segment(segment_entry.meta().clone()))
        .collect();

    let merger = IndexMerger::open(&segments[..]);
    if merger.is_err() {
        error!("IndexMerger is error: {}", merger.err().unwrap());
        panic!("...")
    }
    info!(
        "[start_merge][merge] collect old segments, size: {:?}, will call IndexMerger -> merge",
        segments.len()
    );

    let inv_idx_mmap = merger.unwrap().merge_v2(
        merged_segment.index().directory().get_path(),
        Some(&segment_id),
    )?;

    let rows_count = inv_idx_mmap.vector_count() as u32;
    let merged_segment = merged_segment.clone().with_rows_count(rows_count);

    for file_path in inv_idx_mmap.files(Some(&segment_id)) {
        merged_segment
            .index()
            .directory()
            .register_file_as_managed(&file_path);
    }

    info!(
        "[{}] - [merge] origin segments size: {}, target segmengt-id: {}, rows_count: {}",
        thread::current().name().unwrap_or_default(),
        segment_entries.len(),
        merged_segment.clone().id(),
        rows_count
    );
    debug!(
        "[{}] - [merge] origin segments {:?}",
        thread::current().name().unwrap_or_default(),
        segment_entries
            .iter()
            .map(|entry| entry.segment_id())
            .collect::<Vec<_>>()
    );

    let meta: SegmentMeta = index.new_segment_meta(merged_segment.id(), rows_count);
    meta.untrack_temp_svstore();

    let segment_entry = SegmentEntry::new(meta, None);

    Ok(Some(segment_entry))
}

impl SegmentUpdater {
    /// 为 index 创建 segment updater
    pub fn create(index: Index, stamper: Stamper) -> crate::Result<SegmentUpdater> {
        let segments: Vec<SegmentMeta> = index.searchable_segment_metas()?;
        debug!("[create] load segment metas, size {}", segments.len());

        let segment_manager = SegmentManager::from_segments(segments);

        // SegmentUpdater 仅使用线程数为 1 的线程池
        let pool: ThreadPool = ThreadPoolBuilder::new()
            .thread_name(|_| "seg_updater".to_string())
            .num_threads(1)
            .build()
            .map_err(|_| {
                crate::SparseError::SystemError(
                    "Failed to spawn segment updater thread".to_string(),
                )
            })?;

        // 用于合并 Segment 的线程池
        let merge_thread_pool: ThreadPool = ThreadPoolBuilder::new()
            .thread_name(|i| format!("merge_thd_{i}"))
            .num_threads(NUM_MERGE_THREADS)
            .build()
            .map_err(|_| {
                crate::SparseError::SystemError(
                    "Failed to spawn segment merging thread".to_string(),
                )
            })?;

        // 从 disk 上加载 meta.json
        let index_meta: IndexMeta = index.load_metas()?;

        // 初始化 SegmentUpdater
        Ok(SegmentUpdater(Arc::new(InnerSegmentUpdater {
            active_index_meta: RwLock::new(Arc::new(index_meta)),
            pool,
            merge_thread_pool,
            index,
            segment_manager,
            merge_policy: RwLock::new(Arc::new(DefaultMergePolicy::default())),
            killed: AtomicBool::new(false),
            stamper,
            merge_operations: Default::default(),
        })))
    }

    pub fn get_merge_policy(&self) -> Arc<dyn MergePolicy> {
        self.merge_policy.read().unwrap().clone()
    }

    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        let arc_merge_policy = Arc::from(merge_policy);
        *self.merge_policy.write().unwrap() = arc_merge_policy;
    }

    /// [private] 用来调度异步任务
    fn schedule_task<T: 'static + Send, F: FnOnce() -> crate::Result<T> + 'static + Send>(
        &self,
        task: F,
    ) -> FutureResult<T> {
        if !self.is_alive() {
            return crate::SparseError::SystemError(
                "Segment updater was already killed".to_string(),
            )
            .into();
        }
        let (scheduled_result, sender) = FutureResult::create(
            "A segment_updater future did not succeed. This should never happen.",
        );
        // 异步执行任务，将任务放在后台线程中执行，允许主线进程继续运行而不被阻塞
        self.pool.spawn(|| {
            let task_result = task();
            let _ = sender.send(task_result);
        });
        // 结果传递，FutureResult 是一个用于异步操作的结构，允许调用者在任务完成时获得结果
        scheduled_result
    }

    // 把一个新的 SegmentEntry 放到段管理器内，并在添加后考虑合并选项
    pub fn schedule_add_segment(&self, segment_entry: SegmentEntry) -> FutureResult<()> {
        info!(
            "[{}] - [schedule_add_segment] segment-id: {}",
            thread::current().name().unwrap_or_default(),
            segment_entry.segment_id()
        );

        let segment_updater = self.clone();
        self.schedule_task(move || {
            segment_updater.segment_manager.add_segment(segment_entry);
            segment_updater.consider_merge_options();
            Ok(())
        })
    }

    /// 清理掉持有的 segment_manager 记录的 committed 和 uncommitted 集合中的所有 seg ids
    pub(crate) fn remove_all_segments(&self) {
        self.segment_manager.remove_all_segments();
    }

    /// 停止 segment updater 线程
    pub fn kill(&mut self) {
        self.killed.store(true, Ordering::Release);
    }

    /// 判断 segment updater 线程是否存活
    pub fn is_alive(&self) -> bool {
        !self.killed.load(Ordering::Acquire)
    }

    /// commit 后存储 meta 内容到文件
    pub fn save_metas(
        &self,
        opstamp: Opstamp,
        commit_message: Option<String>,
    ) -> crate::Result<()> {
        if self.is_alive() {
            let index = &self.index;
            let directory = index.directory();
            // 获得所有已经 committed 的 segment metas
            let mut commited_segment_metas = self.segment_manager.committed_segment_metas();
            // 将梯井提交的 segment 按照 rows 排序
            commited_segment_metas.sort_by_key(|segment_meta| -(segment_meta.rows_count() as i32));

            let index_meta: IndexMeta = IndexMeta {
                segments: commited_segment_metas,
                opstamp,
                payload: commit_message,
            };
            // TODO add context to the error.
            save_metas(&index_meta, directory.box_clone().borrow_mut())?;
            self.store_meta(&index_meta);
        }
        Ok(())
    }

    /// 执行 GC 操作
    pub fn schedule_garbage_collect(&self) -> FutureResult<GarbageCollectionResult> {
        info!(
            "[{}] - [schedule_garbage_collect] entry",
            thread::current().name().unwrap_or_default()
        );
        let self_clone = self.clone();
        self.schedule_task(move || garbage_collect_files(self_clone))
    }

    /// 获取对 Index 有用的文件 </br>
    /// 不包含锁文件以、暂未被 GC 删除的过时文件
    fn list_files(&self) -> HashSet<PathBuf> {
        let mut files: HashSet<PathBuf> = self
            .index
            .list_all_segment_metas()
            .into_iter()
            .flat_map(|segment_meta| segment_meta.list_files())
            .collect();
        files.insert(META_FILEPATH.to_path_buf());
        files
    }

    /// 执行索引 commit 操作
    pub(crate) fn schedule_commit(
        &self,
        opstamp: Opstamp,
        payload: Option<String>,
    ) -> FutureResult<Opstamp> {
        let segment_updater: SegmentUpdater = self.clone();
        self.schedule_task(move || {
            info!(
                "[{}] - [schedule_commit] schedule commit task, opstamp: {}",
                thread::current().name().unwrap_or_default(),
                opstamp
            );

            // 获得 segment management 记录的所有 seg entries
            let segment_entries = segment_updater.segment_manager.segment_entries();
            // 将所有的 seg entries 标记为提交状态
            segment_updater.segment_manager.commit(segment_entries);
            // commit 之后更新存储 meta.json 信息
            segment_updater.save_metas(opstamp, payload)?;
            // 手动 GC
            let _ = garbage_collect_files(segment_updater.clone());
            // commit 后考虑是否合并
            segment_updater.consider_merge_options();
            Ok(opstamp)
        })
    }

    /// [private] 更新当前 seg updater 持有的 active_index_meta
    fn store_meta(&self, index_meta: &IndexMeta) {
        *self.active_index_meta.write().unwrap() = Arc::new(index_meta.clone());
    }

    /// [private] 获取当前 seg updater 持有的 active_index_meta
    fn load_meta(&self) -> Arc<IndexMeta> {
        self.active_index_meta.read().unwrap().clone()
    }

    /// 生成一个 MergeOperation 合并操作 </br>
    /// 参数 segment_ids 表示需要进行 merge 的 seg ids </br>
    /// 该 MergeOperation 将会被记录在 seg updater 对应的仓库 inventory 中 </br>
    pub(crate) fn make_merge_operation(&self, segment_ids: &[SegmentId]) -> MergeOperation {
        let commit_opstamp = self.load_meta().opstamp;
        MergeOperation::new(&self.merge_operations, commit_opstamp, segment_ids.to_vec())
    }

    /// 开始执行一个 MergeOperation 合并操作, 函数将会阻塞直到 MergeOperation 实际开始, 但是函数不会等待 MergeOperation 结束 </br>
    /// 调用线程不应该被长时间阻塞, 因为这仅涉及等待 `SegmentUpdater` 队列, 该队列仅包含轻量级操作.</br>
    ///
    /// MergeOperation 合并操作发生在不同的线程 </br>
    ///
    /// 当执行成功时，函数返回 `Future`, 代表合并操作的实际结果, 即 `Result<SegmentMeta>`. </br>
    /// 如果无法启动合并操作, 将返回错误 </br>
    ///
    /// 函数返回的错误不一定代表发生了故障，也有可能是合并操作的瞬间和实际执行合并之间发生了回滚。
    pub fn start_merge(
        &self,
        merge_operation: MergeOperation,
    ) -> FutureResult<Option<SegmentMeta>> {
        assert!(
            !merge_operation.segment_ids().is_empty(),
            "Segment_ids cannot be empty."
        );

        let segment_updater = self.clone();
        let segment_entries: Vec<SegmentEntry> = match self
            .segment_manager
            .start_merge(merge_operation.segment_ids())
        {
            Ok(segment_entries) => segment_entries,
            Err(err) => {
                warn!(
                    "Starting the merge failed for the following reason. This is not fatal. {}",
                    err
                );
                return err.into();
            }
        };
        info!(
            "[start_merge] get segment_entries for merge, segment entries: {:?}",
            segment_entries
        );

        // 创建一个 FutureResult, 用于处理合并操作的结果
        let (scheduled_result, merging_future_send) =
            FutureResult::create("Merge operation failed.");

        self.merge_thread_pool.spawn(move || {
            info!(
                "[{}] - [start_merge] merging... size:{}, segment ids: {:?}",
                thread::current().name().unwrap_or_default(),
                merge_operation.segment_ids().len(),
                merge_operation.segment_ids()
            );

            // The fact that `merge_operation` is moved here is important.
            // Its lifetime is used to track how many merging thread are currently running,
            // as well as which segment is currently in merge and therefore should not be
            // candidate for another merge.
            match merge(&segment_updater.index, segment_entries) {
                Ok(after_merge_segment_entry) => {
                    let res = segment_updater.end_merge(merge_operation, after_merge_segment_entry);
                    let _send_result = merging_future_send.send(res);
                }
                Err(merge_error) => {
                    warn!(
                        "Merge of {:?} was cancelled: {:?}",
                        merge_operation.segment_ids().to_vec(),
                        merge_error
                    );
                    if cfg!(test) {
                        panic!("{merge_error:?}");
                    }
                    let _send_result = merging_future_send.send(Err(merge_error));
                }
            }
        });

        scheduled_result
    }

    /// 从 uncommitted 和 committeed 两个集合中获取对应的可以用来合并的 seg metas
    pub(crate) fn get_mergeable_segments(&self) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
        // 从 segment updater 持有的 merge operations 仓库中获取所有的 seg ids, 这些 ids 会被用来 merge
        let merge_segment_ids: HashSet<SegmentId> = self.merge_operations.segment_in_merge();
        self.segment_manager
            .get_mergeable_segments(&merge_segment_ids)
    }

    /// 获取 uncommitted 与 committed 两个集合中需要合并的 segments </br>
    /// 根据 merge policy 规则生成相应的 MergeOperation </br>
    /// 依次执行 MergeOperation 完成合并流程
    fn consider_merge_options(&self) {
        // 获取 committed 和 uncommitted 两种集合状态下的 seg ids, 它们之间不能 *混合合并*
        let (committed_segments, uncommitted_segments) = self.get_mergeable_segments();
        debug!("[{}] - [consider_merge_options] entry, committed_segs size:{}, uncommitted_segs size:{}", thread::current().name().unwrap_or_default(), committed_segments.len(), uncommitted_segments.len());

        let merge_policy: Arc<dyn MergePolicy> = self.get_merge_policy();

        // 根据 merge 策略, 在 uncommitted_segments 中挑选需要合并的 segs 并生成一组 MergeOperation
        let current_opstamp = self.stamper.stamp();
        let mut merge_candidates: Vec<MergeOperation> = merge_policy
            .compute_merge_candidates(&uncommitted_segments)
            .into_iter()
            .map(|merge_candidate| {
                MergeOperation::new(&self.merge_operations, current_opstamp, merge_candidate.0)
            })
            .collect();

        // 根据 merge 策略, 在 committed_segments 中挑选需要合并的 segs 并生成一组 MergeOperation
        let commit_opstamp = self.load_meta().opstamp;
        let committed_merge_candidates = merge_policy
            .compute_merge_candidates(&committed_segments)
            .into_iter()
            .map(|merge_candidate: MergeCandidate| {
                MergeOperation::new(&self.merge_operations, commit_opstamp, merge_candidate.0)
            });

        // 执行两个集合中生成的所有 MergeOperation
        merge_candidates.extend(committed_merge_candidates);

        debug!(
            "[{}] - [consider_merge_options] candidates size: {:?}",
            thread::current().name().unwrap_or_default(),
            merge_candidates.len()
        );

        for merge_operation in merge_candidates {
            // 如果 merge 不能进行, 这不是一个 Fatal 错误，我们会使用 warning 记录在 `start_merge`
            drop(self.start_merge(merge_operation));
        }
    }

    /// Queues a `end_merge` in the segment updater and blocks until it is successfully processed.
    ///
    /// 结束合并操作，在 Segment 合并完成后进行必要的清理和更新操作，确保合并之后的 Segment 正确的集成到索引中
    fn end_merge(
        &self,
        merge_operation: MergeOperation,
        after_merge_segment_entry: Option<SegmentEntry>,
    ) -> crate::Result<Option<SegmentMeta>> {
        let segment_updater: SegmentUpdater = self.clone();
        let after_merge_segment_meta: Option<SegmentMeta> = after_merge_segment_entry
            .as_ref()
            .map(|segment_entry| segment_entry.meta().clone());

        self.schedule_task(move || {
            info!(
                "[{}] - [end_merge] schedule end_merge task for {:?}",
                thread::current().name().unwrap_or_default(),
                after_merge_segment_entry.as_ref().map(|entry| entry.meta())
            );
            {
                let previous_metas: Arc<IndexMeta> = segment_updater.load_meta();
                // 更新 segment updater 内部两个集合的状态
                let segments_status: SegmentsStatus = segment_updater
                    .segment_manager
                    .end_merge(merge_operation.segment_ids(), after_merge_segment_entry)?;

                // 更新 disk 上存储的 meta.json 文件
                if segments_status == SegmentsStatus::Committed {
                    segment_updater
                        .save_metas(previous_metas.opstamp, previous_metas.payload.clone())?;
                }

                segment_updater.consider_merge_options();
            } // we drop all possible handle to a now useless `SegmentMeta`.

            // 手动 GC
            let _ = garbage_collect_files(segment_updater);
            Ok(())
        })
        .wait()?;
        Ok(after_merge_segment_meta)
    }

    /// Wait for current merging threads.
    ///
    /// Upon termination of the current merging threads,
    /// merge opportunity may appear.
    ///
    /// We keep waiting until the merge policy judges that
    /// no opportunity is available.
    ///
    /// Note that it is not required to call this
    /// method in your application.
    /// Terminating your application without letting
    /// merge terminate is perfectly safe.
    ///
    /// Obsolete files will eventually be cleaned up
    /// by the directory garbage collector.
    pub fn wait_merging_thread(&self) -> crate::Result<()> {
        // 用于阻塞所有的合并操作完成
        self.merge_operations.wait_until_empty();
        Ok(())
    }
}
