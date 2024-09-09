use crate::core::common::types::ScoreType;
use lazy_static::lazy_static;

mod pooled_scores_handle;
mod scores_memory_pool;
mod top_k;

type PooledScores = Vec<ScoreType>;

lazy_static! {
    /// Max number of pooled elements to preserve in memory.
    /// Scaled according to the number of logical CPU cores to account for concurrent operations.
    pub static ref POOL_KEEP_LIMIT: usize = num_cpus::get().clamp(8, 128);
}
pub use pooled_scores_handle::PooledScoresHandle;
pub use scores_memory_pool::ScoresMemoryPool;

pub use top_k::TopK;
