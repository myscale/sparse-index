use crate::core::scores::pooled_scores_handle::PooledScoresHandle;
use crate::core::scores::{PooledScores, POOL_KEEP_LIMIT};
use parking_lot::Mutex;

#[derive(Debug)]
pub struct ScoresMemoryPool {
    pool: Mutex<Vec<PooledScores>>,
}

impl ScoresMemoryPool {
    pub fn new() -> Self {
        ScoresMemoryPool { pool: Mutex::new(Vec::with_capacity(*POOL_KEEP_LIMIT)) }
    }

    pub fn get(&self) -> PooledScoresHandle {
        match self.pool.lock().pop() {
            None => PooledScoresHandle::new(self, vec![]),
            Some(data) => PooledScoresHandle::new(self, data),
        }
    }

    pub(super) fn return_back(&self, data: PooledScores) {
        let mut pool = self.pool.lock();
        if pool.len() < *POOL_KEEP_LIMIT {
            pool.push(data);
        }
    }
}

impl Default for ScoresMemoryPool {
    fn default() -> Self {
        Self::new()
    }
}
