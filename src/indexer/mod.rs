pub mod doc_opstamp_mapping;
pub mod index_writer;
pub mod index_writer_status;
pub mod log_merge_policy;
pub mod merge_operation;
pub mod merge_policy;
pub mod merger;
pub mod operation;
pub mod prepared_commit;
pub mod segment_entry;
pub mod segment_manager;
pub mod segment_register;
pub mod segment_updater;
pub mod segment_writer;
pub mod single_segment_index_writer;
pub mod stamper;

use crossbeam_channel as channel;
use smallvec::SmallVec;

pub use self::index_writer::IndexWriter;
pub use self::log_merge_policy::LogMergePolicy;
pub use self::merge_operation::MergeOperation;
pub use self::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
use self::operation::AddOperation;
pub use self::prepared_commit::PreparedCommit;
pub use self::segment_entry::SegmentEntry;
pub use self::segment_writer::SegmentWriter;

/// Alias for the default merge policy, which is the `LogMergePolicy`.
pub type DefaultMergePolicy = LogMergePolicy;

// Batch of documents.
// Most of the time, users will send operation one-by-one, but it can be useful to
// send them as a small block to ensure that
// - all docs in the operation will happen on the same segment and continuous doc_ids.
// - all operations in the group are committed at the same time, making the group
// atomic.
type AddBatch = SmallVec<[AddOperation; 4]>;
type AddBatchSender = channel::Sender<AddBatch>;
type AddBatchReceiver = channel::Receiver<AddBatch>;
