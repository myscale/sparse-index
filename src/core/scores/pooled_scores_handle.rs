use crate::core::common::types::ScoreType;
use crate::core::scores::PooledScores;
use crate::core::scores::scores_memory_pool::ScoresMemoryPool;

#[derive(Debug)]
pub struct PooledScoresHandle<'a> {
    pool: &'a ScoresMemoryPool,  // 引用的一个分数池实例
    pub scores: PooledScores,    // 实际持有的分数数据
}

impl<'a> PooledScoresHandle<'a> {
    pub fn new(pool: &'a ScoresMemoryPool, scores: PooledScores) -> Self {
        PooledScoresHandle { pool, scores }
    }
}

impl<'a> Drop for PooledScoresHandle<'a> {
    fn drop(&mut self) {
        // 将持有的分数向量归还给分数池, 实现资源自动管理
        self.pool.return_back(std::mem::take(&mut self.scores));
    }
}