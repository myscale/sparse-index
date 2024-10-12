use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use log::{debug, error, info, trace};
use smallvec::smallvec;

use super::operation::{AddOperation, UserOperation};
use super::segment_updater::SegmentUpdater;
use super::{AddBatch, AddBatchReceiver, AddBatchSender, PreparedCommit};
use crate::common::errors::SparseError;
use crate::core::{InvertedIndexConfig, SparseRowContent};
use crate::directory::{DirectoryLock, GarbageCollectionResult};

use crate::future_result::FutureResult;
use crate::index::{Index, Segment, SegmentId, SegmentMeta};
use crate::indexer::index_writer_status::IndexWriterStatus;
use crate::indexer::stamper::Stamper;
use crate::indexer::{MergePolicy, SegmentEntry, SegmentWriter};

use crate::Opstamp;


/// 用于设置 memory_arena 的边界大小, 当 memory_area 中剩余内存低于该值时(1MB), 关闭 segment
pub const MARGIN_IN_BYTES: usize = 1_000_000;

/// 定义每个线程的最小内存预算
pub const MEMORY_BUDGET_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 15u32) as usize;
/// 定义每个线程的最大内存预算
pub const MEMORY_BUDGET_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;

/// 并发写入索引的线程数（不建议超过 8）
pub const MAX_NUM_THREAD: usize = 8;

/// Add document will block if the number of docs waiting in the queue to be indexed reaches `PIPELINE_MAX_SIZE_IN_DOCS`
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;


fn error_in_index_worker_thread(context: &str) -> SparseError {
    SparseError::ErrorInThread(format!(
        "{context}. A worker thread encountered an error (io::Error most likely) or panicked."
    ))
}

/// `IndexWriter` 用于往一个 Index 中插入数据 </br>
/// 它管理了一些 indexing 线程, 以及一个共享的 indexing 队列 </br>
/// 
/// 每个 indexing 线程都在通过 `SegmentWriter` 去 构建 独立的 Segment
pub struct IndexWriter {
    // the lock is just used to bind the lifetime of the lock with that of the IndexWriter.
    _directory_lock: Option<DirectoryLock>,

    index: Index,

    // The memory budget per thread, after which a commit is triggered.
    // 每个线程的内存预算，超过这个内存预算就会触发 commit
    memory_budget_in_bytes_per_thread: usize,

    /// 存储多线程句柄
    workers_join_handle: Vec<JoinHandle<crate::Result<()>>>,

    index_writer_status: IndexWriterStatus,

    operation_sender: AddBatchSender,

    /// 用来更新 segment 的类（合并操作）
    segment_updater: SegmentUpdater,

    worker_id: usize,

    num_threads: usize,

    stamper: Stamper,
    committed_opstamp: Opstamp,
}


/// 在 index worker 内部会 loop 不断获取 AddBatch 并尝试调用 index_documents.
/// - memory_budget: 索引单个 segment 的内存预算
/// - grouped_sv_iterator: 从 Chanel 获取 sv
/// - segment_updater: 更新写入 segment 的类
fn index_documents(
    memory_budget: usize,
    segment: Segment,
    grouped_sv_iterator: &mut dyn Iterator<Item = AddBatch>,
    segment_updater: &SegmentUpdater,
) -> crate::Result<()> {
    info!("{} [index documents] enter", thread::current().name().unwrap_or_default());
    // 初始化 segment writer
    let mut segment_writer = SegmentWriter::for_segment(memory_budget, segment.clone())?;
    // TODO 优化文件写入
    let mut config: InvertedIndexConfig = InvertedIndexConfig::default();
    config.with_data_prefix(segment.id().uuid_string().as_str());
    config.with_meta_prefix(segment.id().uuid_string().as_str());
    let res = segment.index().directory().register_file_as_managed(Path::new(&config.data_file_name())).expect("msg");
    let res = segment.index().directory().register_file_as_managed(Path::new(&config.meta_file_name())).expect("msg");

    // 遍历接收到的 svs
    for sv_group in grouped_sv_iterator {
        // 逐行写入 sparse row content 到 segment 内部
        for sv in sv_group {
            trace!("{} [index_documents] index row content into segment, Add Operation row_id: {}, opstamp: {}", thread::current().name().unwrap_or_default(), sv.row_content.row_id, sv.opstamp);
            segment_writer.index_row_content(sv)?;
        }
        let mem_usage = segment_writer.mem_usage();
        trace!("{} [index_documents] mem_usage {}, true budget {}", thread::current().name().unwrap_or_default(), mem_usage, memory_budget - MARGIN_IN_BYTES);
        // 统计当前的内存超过了限制, 就停止继续索引, 后面会 构建 新的 segment
        if mem_usage >= memory_budget - MARGIN_IN_BYTES {
            info!(
                "[{}] [index_documents] memory limit reached, flushing segment {} with rows_count={}.",
                thread::current().name().unwrap_or_default(),
                segment.id(),
                segment_writer.rows_count()
            );
            break;
        }
    }

    if !segment_updater.is_alive() {
        return Ok(());
    }

    let rows_count = segment_writer.rows_count();

    // this is ensured by the call to peek before starting the worker thread.
    assert!(rows_count > 0);

    // 序列化存储
    // TODO 目前这个 doc_opstamps 不会直接使用到, 是和 delete 相关的，可以先直接删除了
    let _doc_opstamps: Vec<Opstamp> = segment_writer.finalize()?;

    let segment_with_rows_count = segment.clone().with_rows_count(rows_count);

    let meta: SegmentMeta = segment_with_rows_count.meta().clone();
    meta.untrack_temp_svstore();

    // update segment_updater inventory to remove tempstore
    // let segment_entry = SegmentEntry::new(meta, delete_cursor, alive_bitset_opt);
    let segment_entry = SegmentEntry::new(meta, None);
    // 将 segment 加入 merge 计划
    segment_updater.schedule_add_segment(segment_entry).wait()?;
    Ok(())
}


impl IndexWriter {
    pub(crate) fn new(
        index: &Index,
        num_threads: usize,
        memory_budget_in_bytes_per_thread: usize,
        directory_lock: DirectoryLock,
    ) -> crate::Result<Self> {
        if memory_budget_in_bytes_per_thread < MEMORY_BUDGET_NUM_BYTES_MIN {
            let err_msg = format!(
                "The memory arena in bytes per thread needs to be at least \
                 {MEMORY_BUDGET_NUM_BYTES_MIN}."
            );
            return Err(SparseError::InvalidArgument(err_msg));
        }
        if memory_budget_in_bytes_per_thread >= MEMORY_BUDGET_NUM_BYTES_MAX {
            let err_msg = format!(
                "The memory arena in bytes per thread cannot exceed {MEMORY_BUDGET_NUM_BYTES_MAX}"
            );
            return Err(SparseError::InvalidArgument(err_msg));
        }
        let (document_sender, document_receiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);

        let current_opstamp = index.load_metas()?.opstamp;

        let stamper = Stamper::new(current_opstamp);

        let segment_updater =
            SegmentUpdater::create(index.clone(), stamper.clone())?;

        let mut index_writer = Self {
            _directory_lock: Some(directory_lock),

            memory_budget_in_bytes_per_thread,
            index: index.clone(),
            index_writer_status: IndexWriterStatus::from(document_receiver),
            operation_sender: document_sender,

            segment_updater,

            workers_join_handle: vec![],
            num_threads,

            // delete_queue,

            committed_opstamp: current_opstamp,
            stamper,

            worker_id: 0,
        };
        index_writer.start_workers()?;
        Ok(index_writer)
    }

    fn drop_sender(&mut self) {
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        self.operation_sender = sender;
    }

    /// Accessor to the index.
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// 停止掉所有的 merge 线程
    /// 消耗 self 对象，最终使 IndexWriter 销毁
    pub fn wait_merging_threads(mut self) -> crate::Result<()> {
        // this will stop the indexing thread,
        // dropping the last reference to the segment_updater.
        // TODO 搞清楚为什么 drop sender 之后就能够停止继续索引了
        self.drop_sender();

        let former_workers_handles = std::mem::take(&mut self.workers_join_handle);
        for join_handle in former_workers_handles {
            // 阻塞当前线程, 直到对应的工作线程完成并返回一个 Result
            join_handle
                .join()
                .map_err(|_| error_in_index_worker_thread("Worker thread panicked."))?
                .map_err(|_| error_in_index_worker_thread("Worker thread failed."))?;
        }

        // 在 segment updater 停止掉 merge 线程
        let result = self
            .segment_updater
            .wait_merging_thread()
            .map_err(|_| error_in_index_worker_thread("Failed to join merging thread."));

        if let Err(ref e) = result {
            error!("Some merging thread failed {:?}", e);
        }

        result
    }

    #[doc(hidden)]
    pub fn add_segment(&self, segment_meta: SegmentMeta) -> crate::Result<()> {
        // let delete_cursor = self.delete_queue.cursor();
        let segment_entry = SegmentEntry::new(segment_meta, None);
        // segment updater 添加的是 segment entry
        self.segment_updater
            .schedule_add_segment(segment_entry)
            .wait()
    }

    /// Creates a new segment.
    ///
    /// This method is useful only for users trying to do complex
    /// operations, like converting an index format to another.
    ///
    /// It is safe to start writing file associated with the new `Segment`.
    /// These will not be garbage collected as long as an instance object of
    /// `SegmentMeta` object associated with the new `Segment` is "alive".
    pub fn new_segment(&self) -> Segment {
        self.index.new_segment()
    }

    fn operation_receiver(&self) -> crate::Result<AddBatchReceiver> {
        self.index_writer_status
            .operation_receiver()
            .ok_or_else(|| {
                crate::SparseError::ErrorInThread(
                    "The index writer was killed. It can happen if an indexing worker encountered \
                     an Io error for instance."
                        .to_string(),
                )
            })
    }

    /// Spawns a new worker thread for indexing.
    /// The thread consumes documents from the pipeline.
    fn add_indexing_worker(&mut self) -> crate::Result<()> {
        debug!("[add_indexing_worker] entry");
        let document_receiver_clone = self.operation_receiver()?;

        let index_writer_bomb = self.index_writer_status.create_bomb();

        let segment_updater = self.segment_updater.clone();

        let mem_budget = self.memory_budget_in_bytes_per_thread;

        let index = self.index.clone();

        let join_handle: JoinHandle<crate::Result<()>> = thread::Builder::new()
            .name(format!("thrd-sparse-index{}", self.worker_id))
            .spawn(move || {
                loop {
                    let mut document_iterator = document_receiver_clone
                        .clone()
                        .into_iter()
                        .filter(|batch| !batch.is_empty())
                        .peekable();

                    // The peeking here is to avoid creating a new segment's files
                    // if no document are available.
                    //
                    // This is a valid guarantee as the peeked document now belongs to
                    // our local iterator.
                    if let Some(batch) = document_iterator.peek() {
                        assert!(!batch.is_empty());
                    } else {
                        // No more documents.
                        // It happens when there is a commit, or if the `IndexWriter`
                        // was dropped.
                        index_writer_bomb.defuse();
                        return Ok(());
                    }

                    index_documents(
                        mem_budget,
                        index.new_segment(),
                        &mut document_iterator,
                        &segment_updater,
                    )?;
                }
            })?;
        self.worker_id += 1;
        self.workers_join_handle.push(join_handle);
        Ok(())
    }

    /// Accessor to the merge policy.
    pub fn get_merge_policy(&self) -> Arc<dyn MergePolicy> {
        self.segment_updater.get_merge_policy()
    }

    /// Setter for the merge policy.
    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        self.segment_updater.set_merge_policy(merge_policy);
    }

    fn start_workers(&mut self) -> crate::Result<()> {
        info!("index writer start workers");
        for _ in 0..self.num_threads {
            self.add_indexing_worker()?;
        }
        Ok(())
    }

    /// Detects and removes the files that are not used by the index anymore.
    pub fn garbage_collect_files(&self) -> FutureResult<GarbageCollectionResult> {
        self.segment_updater.schedule_garbage_collect()
    }

    /// 删除索引中的所有数据
    ///
    /// TODO 搞清楚为什么 revert 的对象是 self.committed_opstamp
    pub fn delete_all_documents(&self) -> crate::Result<Opstamp> {
        // Delete segments
        self.segment_updater.remove_all_segments();
        // Return new stamp - reverted stamp
        self.stamper.revert(self.committed_opstamp);
        Ok(self.committed_opstamp)
    }

    /// merge 给定的一组 segment_ids，并返回新的 SegmentMeta
    pub fn merge(&mut self, segment_ids: &[SegmentId]) -> FutureResult<Option<SegmentMeta>> {
        let merge_operation = self.segment_updater.make_merge_operation(segment_ids);
        let segment_updater = self.segment_updater.clone();
        segment_updater.start_merge(merge_operation)
    }

    /// Closes the current document channel send.
    /// and replace all the channels by new ones.
    ///
    /// The current workers will keep on indexing
    /// the pending document and stop
    /// when no documents are remaining.
    ///
    /// Returns the former segment_ready channel.
    /// TODO 旧通道中的数据大概率是会发生丢失的，这段注释应该是有点儿问题，被替换掉的通道也没有返回给用户
    fn recreate_document_channel(&mut self) {
        let (document_sender, document_receiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);
        self.operation_sender = document_sender;
        self.index_writer_status = IndexWriterStatus::from(document_receiver);
    }

    /// Rollback to the last commit
    ///
    /// This cancels all of the updates that
    /// happened after the last commit.
    /// After calling rollback, the index is in the same
    /// state as it was after the last commit.
    ///
    /// The opstamp at the last commit is returned.
    pub fn rollback(&mut self) -> crate::Result<Opstamp> {
        info!("Rolling back to opstamp {}", self.committed_opstamp);
        // marks the segment updater as killed. From now on, all
        // segment updates will be ignored.
        self.segment_updater.kill();
        let document_receiver_res = self.operation_receiver();

        // take the directory lock to create a new index_writer.
        let directory_lock = self
            ._directory_lock
            .take()
            .expect("The IndexWriter does not have any lock. This is a bug, please report.");

        let new_index_writer = IndexWriter::new(
            &self.index,
            self.num_threads,
            self.memory_budget_in_bytes_per_thread,
            directory_lock,
        )?;

        // the current `self` is dropped right away because of this call.
        //
        // This will drop the document queue, and the thread
        // should terminate.
        *self = new_index_writer;

        // Drains the document receiver pipeline :
        // Workers don't need to index the pending documents.
        //
        // This will reach an end as the only document_sender
        // was dropped with the index_writer.
        if let Ok(document_receiver) = document_receiver_res {
            for _ in document_receiver {}
        }

        Ok(self.committed_opstamp)
    }

    /// Prepares a commit.
    ///
    /// Calling `prepare_commit()` will cut the indexing
    /// queue. All pending documents will be sent to the
    /// indexing workers. They will then terminate, regardless
    /// of the size of their current segment and flush their
    /// work on disk.
    ///
    /// Once a commit is "prepared", you can either
    /// call
    /// * `.commit()`: to accept this commit
    /// * `.abort()`: to cancel this commit.
    ///
    /// In the current implementation, [`PreparedCommit`] borrows
    /// the [`IndexWriter`] mutably so we are guaranteed that no new
    /// document can be added as long as it is committed or is
    /// dropped.
    ///
    /// It is also possible to add a payload to the `commit`
    /// using this API.
    /// See [`PreparedCommit::set_payload()`].
    pub fn prepare_commit(&mut self) -> crate::Result<PreparedCommit> {
        // Here, because we join all of the worker threads,
        // all of the segment update for this commit have been
        // sent.
        //
        // No document belonging to the next commit have been
        // pushed too, because add_document can only happen
        // on this thread.
        //
        // This will move uncommitted segments to the state of
        // committed segments.
        info!("[{}] [prepare_commit] tring prepare commit", thread::current().name().unwrap_or_default());

        // this will drop the current document channel
        // and recreate a new one.
        self.recreate_document_channel();

        let former_workers_join_handle = std::mem::take(&mut self.workers_join_handle);

        // 阻塞等待旧的 index 线程结束掉
        for worker_handle in former_workers_join_handle {
            let indexing_worker_result = worker_handle
                .join()
                .map_err(|e| SparseError::ErrorInThread(format!("{e:?}")))?;
            indexing_worker_result?;
            // 结束一个 index 线程就重新创建一个新的
            self.add_indexing_worker()?;
        }

        let commit_opstamp = self.stamper.stamp();
        let prepared_commit = PreparedCommit::new(self, commit_opstamp);
        info!("[{}] [prepare_commit] commit has been finished, opstamp: {}", thread::current().name().unwrap_or_default(), commit_opstamp);

        Ok(prepared_commit)
    }

    /// Commits all of the pending changes
    ///
    /// A call to commit blocks.
    /// After it returns, all of the document that
    /// were added since the last commit are published
    /// and persisted.
    ///
    /// In case of a crash or an hardware failure (as
    /// long as the hard disk is spared), it will be possible
    /// to resume indexing from this point.
    ///
    /// Commit returns the `opstamp` of the last document
    /// that made it in the commit.
    pub fn commit(&mut self) -> crate::Result<Opstamp> {
        self.prepare_commit()?.commit()
    }

    pub(crate) fn segment_updater(&self) -> &SegmentUpdater {
        &self.segment_updater
    }

    /// Returns the opstamp of the last successful commit.
    ///
    /// This is, for instance, the opstamp the index will
    /// rollback to if there is a failure like a power surge.
    ///
    /// This is also the opstamp of the commit that is currently
    /// available for searchers.
    pub fn commit_opstamp(&self) -> Opstamp {
        self.committed_opstamp
    }

    /// Adds a document.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// The opstamp is an increasing `u64` that can
    /// be used by the client to align commits with its own
    /// document queue.
    pub fn add_document(&self, row_content: SparseRowContent) -> crate::Result<Opstamp> {
        let opstamp = self.stamper.stamp();
        self.send_add_documents_batch(smallvec![AddOperation {
            opstamp,
            row_content
        }])?;
        Ok(opstamp)
    }

    /// Gets a range of stamps from the stamper and "pops" the last stamp
    /// from the range returning a tuple of the last optstamp and the popped
    /// range.
    ///
    /// The total number of stamps generated by this method is `count + 1`;
    /// each operation gets a stamp from the `stamps` iterator and `last_opstamp`
    /// is for the batch itself.
    fn get_batch_opstamps(&self, count: Opstamp) -> (Opstamp, Range<Opstamp>) {
        let Range { start, end } = self.stamper.stamps(count + 1u64);
        let last_opstamp = end - 1;
        (last_opstamp, start..last_opstamp)
    }

    /// 执行一组文档操作，确保操作被分配连续的 u64 操作印戳，
    /// 并且同一组的添加操作被刷新到同一段中。
    ///
    /// 如果索引管道已满，此调用可能会阻塞。
    ///
    /// 给定的 `user_operations` 中的每个操作都会接收到按顺序的连续 u64 操作印戳。
    /// 整个批处理本身也会得到一个比最后一个操作印戳大 1 的印戳。
    /// 此 `batch_opstamp` 是 `run` 的返回值。即使是空的 `user_operations` 组，
    /// 一个空的 `Vec<UserOperation>`，也会收到一个有效的印戳，
    /// 即使实际上没有对索引进行更改。
    ///
    /// 与添加和删除操作类似（参见 `IndexWriter.add_document` 和
    /// `IndexWriter.delete_term`），调用 `run` 所做的更改只有在调用 `commit()` 后
    /// 对读者可见。
    pub fn run<I>(&self, user_operations: I) -> crate::Result<Opstamp>
    where
        I: IntoIterator<Item = UserOperation>,
        I::IntoIter: ExactSizeIterator,
    {
        let user_operations_it = user_operations.into_iter();
        let count = user_operations_it.len() as u64;
        if count == 0 {
            return Ok(self.stamper.stamp());
        }
        let (batch_opstamp, stamps) = self.get_batch_opstamps(count);

        let mut adds = AddBatch::default();

        for (user_op, opstamp) in user_operations_it.zip(stamps) {
            match user_op {
                UserOperation::Add(row_content) => {
                    let add_operation = AddOperation {
                        opstamp,
                        row_content,
                    };
                    adds.push(add_operation);
                }
            }
        }
        self.send_add_documents_batch(adds)?;
        Ok(batch_opstamp)
    }

    fn send_add_documents_batch(&self, add_ops: AddBatch) -> crate::Result<()> {
        if self.index_writer_status.is_alive() && self.operation_sender.send(add_ops).is_ok() {
            Ok(())
        } else {
            Err(error_in_index_worker_thread("An index writer was killed."))
        }
    }
}

impl Drop for IndexWriter {
    fn drop(&mut self) {
        self.segment_updater.kill();
        self.drop_sender();
        for work in self.workers_join_handle.drain(..) {
            let _ = work.join();
        }
    }
}
