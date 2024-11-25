use crate::core::scores::scores_memory_pool::ScoresMemoryPool;
use crate::core::scores::PooledScores;

#[derive(Debug)]
pub struct PooledScoresHandle<'a> {
    pool: &'a ScoresMemoryPool,
    pub scores: PooledScores,
}

impl<'a> PooledScoresHandle<'a> {
    pub fn new(pool: &'a ScoresMemoryPool, scores: PooledScores) -> Self {
        PooledScoresHandle { pool, scores }
    }
}

impl<'a> Drop for PooledScoresHandle<'a> {
    fn drop(&mut self) {
        self.pool.return_back(std::mem::take(&mut self.scores));
    }
}
