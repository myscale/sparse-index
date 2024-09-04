use std::cmp::Ordering;
use ordered_float::OrderedFloat;

pub type ScoreType = f32;
pub type ElementOffsetType = u32;
//
// // 用来记录稀疏向量的得分
// #[derive(Copy, Clone, PartialEq, Debug, Default)]
// pub struct ScoredPointOffset {
//     pub row_id: ElementOffsetType,
//     pub score: ScoreType,
// }
//
// impl Eq for ScoredPointOffset {}
//
// impl Ord for ScoredPointOffset {
//     fn cmp(&self, other: &Self) -> Ordering {
//         OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
//     }
// }
//
// impl PartialOrd for ScoredPointOffset {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }