use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;

use log::{debug, error, info, warn};
use rayon::{ThreadPool, ThreadPoolBuilder};

use super::segment_manager::SegmentManager;
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
use crate::{Opstamp, RowId, META_FILEPATH};

const NUM_MERGE_THREADS: usize = 4;

/// store index meta.json into disk.
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

/// Responsible for handling all segment update operations.
/// All processing is done on a single thread, using a shared queue to consume tasks.
#[derive(Clone)]
pub(crate) struct SegmentUpdater(Arc<InnerSegmentUpdater>);

pub(crate) struct InnerSegmentUpdater {
    /// Stores the current active copy of IndexMeta to avoid loading from the file each time it's needed in SegmentUpdater.
    /// This copy is always kept up to date, as all updates are performed through the single active SegmentUpdater.
    active_index_meta: RwLock<Arc<IndexMeta>>,

    /// segment updater thread pool size = 1
    pool: ThreadPool,

    /// merge thread pool size = 4
    merge_thread_pool: ThreadPool,

    index: Index,

    /// Manages segments in Uncommitted and Committed states.
    segment_manager: SegmentManager,

    /// Merge policy.
    merge_policy: RwLock<Arc<dyn MergePolicy>>,
    killed: AtomicBool,
    stamper: Stamper,

    /// Repository for MergeOperations.
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
    index.directory_mut().garbage_collect(move || segment_updater.list_files())
}

/// Merges a list of segments the list of segment givens in the `segment_entries`.
/// This function happens in the calling thread and is computationally expensive.
fn merge(
    index: &Index,
    mut segment_entries: Vec<SegmentEntry>,
) -> crate::Result<Option<SegmentEntry>> {
    let total_rows_count =
        segment_entries.iter().map(|segment| segment.meta().rows_count() as u64).sum::<u64>();
    if total_rows_count == 0 {
        return Ok(None);
    }
    info!("[start_merge][merge] future merge rows count: {:?}", total_rows_count);

    // initialized a new segment for merged.
    let merged_segment = index.new_segment();
    let segment_id = merged_segment.id().uuid_string();

    // collect a group of segments need merged.
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

    let (rows_count, index_files) =
        merger.unwrap().merge(merged_segment.index().directory().get_path(), Some(&segment_id))?;

    let merged_segment = merged_segment.clone().with_rows_count(rows_count as RowId);

    for file_path in index_files {
        let _ = merged_segment.index().directory().register_file_as_managed(&file_path)?;
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
        segment_entries.iter().map(|entry| entry.segment_id()).collect::<Vec<_>>()
    );

    let meta: SegmentMeta = index.new_segment_meta(merged_segment.id(), rows_count as RowId);
    meta.untrack_temp_svstore();

    let segment_entry = SegmentEntry::new(meta, None);

    Ok(Some(segment_entry))
}

impl SegmentUpdater {
    /// create segment updater for index.
    pub fn create(index: Index, stamper: Stamper) -> crate::Result<SegmentUpdater> {
        let segments: Vec<SegmentMeta> = index.searchable_segment_metas()?;
        debug!("[create] load segment metas, size {}", segments.len());

        let segment_manager = SegmentManager::from_segments(segments);

        // SegmentUpdater thread is 1.
        let pool: ThreadPool = ThreadPoolBuilder::new()
            .thread_name(|_| "seg_updater".to_string())
            .num_threads(1)
            .build()
            .map_err(|_| {
                crate::SparseError::SystemError(
                    "Failed to spawn segment updater thread".to_string(),
                )
            })?;

        // For merge operation, we use pool size = 4.
        let merge_thread_pool: ThreadPool = ThreadPoolBuilder::new()
            .thread_name(|i| format!("merge_thd_{i}"))
            .num_threads(NUM_MERGE_THREADS)
            .build()
            .map_err(|_| {
                crate::SparseError::SystemError(
                    "Failed to spawn segment merging thread".to_string(),
                )
            })?;

        // load `meta.json` from disk.
        let index_meta: IndexMeta = index.load_metas()?;

        // initialize SegmentUpdater
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

    /// [private] It is used to schedule asynchronous tasks
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
        // Asynchronously execute the task, placing it in a background thread
        // to allow the main process to continue running without being blocked.
        self.pool.spawn(|| {
            let task_result = task();
            let _ = sender.send(task_result);
        });
        // Result passing; FutureResult is a structure for asynchronous operations,
        // allowing the caller to obtain the result when the task is completed.
        scheduled_result
    }

    // Place a new SegmentEntry into the segment manager and consider merge options after adding.
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

    /// Clean up all segment IDs in the committed and uncommitted collections held by the segment manager.
    pub(crate) fn remove_all_segments(&self) {
        self.segment_manager.remove_all_segments();
    }

    /// Stop the segment updater thread.
    pub fn kill(&mut self) {
        self.killed.store(true, Ordering::Release);
    }

    /// Check if the segment updater thread is alive.
    pub fn is_alive(&self) -> bool {
        !self.killed.load(Ordering::Acquire)
    }

    /// Store meta content to file after commit.
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

            let index_meta: IndexMeta =
                IndexMeta { segments: commited_segment_metas, opstamp, payload: commit_message };
            // TODO add context to the error.
            save_metas(&index_meta, directory.box_clone().borrow_mut())?;
            self.store_meta(&index_meta);
        }
        Ok(())
    }

    /// Starting GC thread.
    pub fn schedule_garbage_collect(&self) -> FutureResult<GarbageCollectionResult> {
        info!(
            "[{}] - [schedule_garbage_collect] entry",
            thread::current().name().unwrap_or_default()
        );
        let self_clone = self.clone();
        self.schedule_task(move || garbage_collect_files(self_clone))
    }

    /// Retrieve files useful to the Index.
    /// Does not include lock files or outdated files that have not yet been deleted by GC.
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

    /// Execute the index commit operation.
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

            // Obtain all segment entries from the segment management records.
            let segment_entries = segment_updater.segment_manager.segment_entries();
            // Mark all segment entries as committed.
            segment_updater.segment_manager.commit(segment_entries);
            // After commit, update and store meta.json information.
            segment_updater.save_metas(opstamp, payload)?;
            // Manually trigger garbage collection.
            let _ = garbage_collect_files(segment_updater.clone());
            // Consider merging after commit.
            segment_updater.consider_merge_options();
            Ok(opstamp)
        })
    }

    /// [private] Update the active_index_meta held by the current segment updater.
    fn store_meta(&self, index_meta: &IndexMeta) {
        *self.active_index_meta.write().unwrap() = Arc::new(index_meta.clone());
    }

    /// [private] Retrieve the active_index_meta held by the current segment updater.
    fn load_meta(&self) -> Arc<IndexMeta> {
        self.active_index_meta.read().unwrap().clone()
    }

    /// Generate a MergeOperation for merging.
    /// The parameter `segment_ids` indicates the segment IDs that need to be merged.
    /// This MergeOperation will be recorded in the corresponding inventory of the segment updater.
    pub(crate) fn make_merge_operation(&self, segment_ids: &[SegmentId]) -> MergeOperation {
        let commit_opstamp = self.load_meta().opstamp;
        MergeOperation::new(&self.merge_operations, commit_opstamp, segment_ids.to_vec())
    }

    /// Start executing a MergeOperation. The function will block until the MergeOperation actually begins,
    /// but it will not wait for the MergeOperation to finish.
    /// The calling thread should not be blocked for a long time, as this only involves waiting for the
    /// `SegmentUpdater` queue, which contains only lightweight operations.
    ///
    /// The MergeOperation occurs in a different thread.
    ///
    /// When executed successfully, the function returns a `Future`, representing the actual result of the
    /// merge operation, i.e., `Result<SegmentMeta>`.
    /// If the merge operation cannot be started, an error will be returned.
    ///
    /// The error returned by the function does not necessarily indicate a failure; it may also indicate
    /// a rollback that occurred between the moment of the merge operation and the actual execution of the merge.
    pub fn start_merge(
        &self,
        merge_operation: MergeOperation,
    ) -> FutureResult<Option<SegmentMeta>> {
        assert!(!merge_operation.segment_ids().is_empty(), "Segment_ids cannot be empty.");

        let segment_updater = self.clone();
        let segment_entries: Vec<SegmentEntry> =
            match self.segment_manager.start_merge(merge_operation.segment_ids()) {
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

        // Create a FutureResult to handle the result of the merge operation.
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

    /// Retrieve the corresponding segment metas that can be used for merging from the uncommitted and committed collections.
    pub(crate) fn get_mergeable_segments(&self) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
        // Get all segment IDs from the merge operations repository held by the segment updater; these IDs will be used for merging.
        let merge_segment_ids: HashSet<SegmentId> = self.merge_operations.segment_in_merge();
        self.segment_manager.get_mergeable_segments(&merge_segment_ids)
    }

    /// Get the segments that need to be merged from the uncommitted and committed collections.
    /// Generate the corresponding MergeOperation based on the merge policy rules.
    /// Execute the MergeOperation sequentially to complete the merge process.
    fn consider_merge_options(&self) {
        // Get segment IDs in both committed and uncommitted states; they cannot be *mixed* for merging.
        let (committed_segments, uncommitted_segments) = self.get_mergeable_segments();
        debug!(
            "[{}] - [consider_merge_options] entry, committed_segs size:{}, uncommitted_segs size:{}",
            thread::current().name().unwrap_or_default(),
            committed_segments.len(),
            uncommitted_segments.len()
        );

        let merge_policy: Arc<dyn MergePolicy> = self.get_merge_policy();

        // Based on the merge policy, select segments to merge from uncommitted_segments and generate a set of MergeOperations.
        let current_opstamp = self.stamper.stamp();
        let mut merge_candidates: Vec<MergeOperation> = merge_policy
            .compute_merge_candidates(&uncommitted_segments)
            .into_iter()
            .map(|merge_candidate| {
                MergeOperation::new(&self.merge_operations, current_opstamp, merge_candidate.0)
            })
            .collect();

        // Based on the merge policy, select segments to merge from committed_segments and generate a set of MergeOperations.
        let commit_opstamp = self.load_meta().opstamp;
        let committed_merge_candidates = merge_policy
            .compute_merge_candidates(&committed_segments)
            .into_iter()
            .map(|merge_candidate: MergeCandidate| {
                MergeOperation::new(&self.merge_operations, commit_opstamp, merge_candidate.0)
            });

        // Execute all MergeOperations generated from both collections.
        merge_candidates.extend(committed_merge_candidates);

        debug!(
            "[{}] - [consider_merge_options] candidates size: {:?}",
            thread::current().name().unwrap_or_default(),
            merge_candidates.len()
        );

        for merge_operation in merge_candidates {
            // TODO: If the merge cannot proceed, this is not a fatal error; we will log it as a warning in `start_merge`.
            drop(self.start_merge(merge_operation));
        }
    }

    /// Queues a `end_merge` in the segment updater and blocks until it is successfully processed.
    ///
    /// End the merge operation, performing necessary cleanup and updates after the segment merge is complete,
    /// ensuring that the merged segment is correctly integrated into the index.
    fn end_merge(
        &self,
        merge_operation: MergeOperation,
        after_merge_segment_entry: Option<SegmentEntry>,
    ) -> crate::Result<Option<SegmentMeta>> {
        let segment_updater: SegmentUpdater = self.clone();
        let after_merge_segment_meta: Option<SegmentMeta> =
            after_merge_segment_entry.as_ref().map(|segment_entry| segment_entry.meta().clone());

        self.schedule_task(move || {
            info!(
                "[{}] - [end_merge] schedule end_merge task for {:?}",
                thread::current().name().unwrap_or_default(),
                after_merge_segment_entry.as_ref().map(|entry| entry.meta())
            );
            {
                let previous_metas: Arc<IndexMeta> = segment_updater.load_meta();
                // Update the status of the two collections within the segment updater.
                let segments_status: SegmentsStatus = segment_updater
                    .segment_manager
                    .end_merge(merge_operation.segment_ids(), after_merge_segment_entry)?;

                // Update the meta.json file stored on disk.
                if segments_status == SegmentsStatus::Committed {
                    segment_updater
                        .save_metas(previous_metas.opstamp, previous_metas.payload.clone())?;
                }

                segment_updater.consider_merge_options();
            } // we drop all possible handle to a now useless `SegmentMeta`.

            // manually trigger GC
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
        self.merge_operations.wait_until_empty();
        Ok(())
    }
}
