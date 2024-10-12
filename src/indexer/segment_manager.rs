use std::collections::hash_set::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use log::warn;

use super::segment_register::SegmentRegister;
use crate::common::errors::SparseError;
use crate::index::{SegmentId, SegmentMeta};
use crate::indexer::SegmentEntry;



#[derive(PartialEq, Eq)]
pub(crate) enum SegmentsStatus {
    Committed,
    Uncommitted,
}


#[derive(Default)]
struct SegmentRegisters {
    uncommitted: SegmentRegister,
    committed: SegmentRegister,
}

impl SegmentRegisters {
    /// 获取所有 seg ids 一致的提交状态 </br>
    /// 当找不到 seg id 时或者 seg ids 状态不一致时候就返回 None
    fn segments_status(&self, segment_ids: &[SegmentId]) -> Option<SegmentsStatus> {
        if self.uncommitted.contains_all(segment_ids) {
            Some(SegmentsStatus::Uncommitted)
        } else if self.committed.contains_all(segment_ids) {
            Some(SegmentsStatus::Committed)
        } else {
            warn!(
                "segment_ids: {:?}, committed_ids: {:?}, uncommitted_ids {:?}",
                segment_ids,
                self.committed.segment_ids(),
                self.uncommitted.segment_ids()
            );
            None
        }
    }
}

/// 用于管理 Segment 以及它们的状态（提交/未提交）</br>
/// 能够保证在合并过程中对 seg registers 的原子更改
#[derive(Default)]
pub struct SegmentManager {
    registers: RwLock<SegmentRegisters>,
}

impl Debug for SegmentManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let lock = self.read();
        write!(
            f,
            "{{ uncommitted: {:?}, committed: {:?} }}",
            lock.uncommitted, lock.committed
        )
    }
}

impl SegmentManager {
    // Lock poisoning should never happen :
    // The lock is acquired and released within this class,
    // and the operations cannot panic.
    fn read(&self) -> RwLockReadGuard<'_, SegmentRegisters> {
        self.registers
            .read()
            .expect("Failed to acquire read lock on SegmentManager.")
    }

    fn write(&self) -> RwLockWriteGuard<'_, SegmentRegisters> {
        self.registers
            .write()
            .expect("Failed to acquire write lock on SegmentManager.")
    }

    /// 初始化 SegmentManager </br>
    /// 提供的 seg metas 参数均被视为 committed
    pub fn from_segments(
        segment_metas: Vec<SegmentMeta>,
    ) -> SegmentManager {
        SegmentManager {
            registers: RwLock::new(SegmentRegisters {
                uncommitted: SegmentRegister::default(),
                committed: SegmentRegister::new(segment_metas),
            }),
        }
    }

    /// 从 committed 和 uncommitted 集合内获取可以进行合并的 seg ids </br>
    /// 提供的 `in_merge_segment_ids` 存储了正在合并的 ids
    /// 
    /// return (committed mergeable, uncommitted mergeable)
    pub fn get_mergeable_segments(
        &self,
        in_merge_segment_ids: &HashSet<SegmentId>,
    ) -> (Vec<SegmentMeta>, Vec<SegmentMeta>) {
        let registers_lock = self.read();
        (
            registers_lock
                .committed
                .get_mergeable_segments(in_merge_segment_ids),
            registers_lock
                .uncommitted
                .get_mergeable_segments(in_merge_segment_ids),
        )
    }
    /// 返回记录的所有 seg entries (committed and uncommitted)
    pub fn segment_entries(&self) -> Vec<SegmentEntry> {
        let registers_lock = self.read();
        let mut segment_entries = registers_lock.uncommitted.segment_entries();
        segment_entries.extend(registers_lock.committed.segment_entries());
        segment_entries
    }



    /// 删除 segment management 记录中所有空的 seg ids
    fn remove_empty_segments(&self) {
        let mut registers_lock = self.write();
        registers_lock
            .committed
            .segment_entries()
            .iter()
            .filter(|segment| segment.meta().alive_rows_count() == 0)
            .for_each(|segment| {
                registers_lock
                    .committed
                    .remove_segment(&segment.segment_id())
            });
    }

    /// 清空 commit 和 uncommitted 内所有的 seg ids
    pub(crate) fn remove_all_segments(&self) {
        let mut registers_lock = self.write();
        registers_lock.committed.clear();
        registers_lock.uncommitted.clear();
    }

    /// 清理 committed 和 uncommitted 并将给出的 segment_entris 全部添加到 committed 集合
    pub fn commit(&self, segment_entries: Vec<SegmentEntry>) {
        let mut registers_lock: RwLockWriteGuard<'_, SegmentRegisters> = self.write();
        registers_lock.committed.clear();
        registers_lock.uncommitted.clear();
        for segment_entry in segment_entries {
            registers_lock.committed.add_segment_entry(segment_entry);
        }
    }

    /// 给定一组要进行合并的 seg ids 并返回其对应的 SegmentEntry </br>
    /// 函数保证了这些 seg ids 均在 uncommitted 或者 committed 其中一个集合内部 </br>
    /// 如果 seg ids 不全部属于其中一个集合就报错
    pub fn start_merge(&self, segment_ids: &[SegmentId]) -> crate::Result<Vec<SegmentEntry>> {
        let registers_lock = self.read();
        let mut segment_entries = vec![];
        if registers_lock.uncommitted.contains_all(segment_ids) {
            for segment_id in segment_ids {
                let segment_entry = registers_lock.uncommitted.get(segment_id).expect(
                    "Segment id not found {}. Should never happen because of the contains all \
                     if-block.",
                );
                segment_entries.push(segment_entry);
            }
        } else if registers_lock.committed.contains_all(segment_ids) {
            for segment_id in segment_ids {
                let segment_entry = registers_lock.committed.get(segment_id).expect(
                    "Segment id not found {}. Should never happen because of the contains all \
                     if-block.",
                );
                segment_entries.push(segment_entry);
            }
        } else {
            let error_msg = "Merge operation sent for segments that are not all uncommitted or \
                             committed."
                .to_string();
            return Err(SparseError::InvalidArgument(error_msg));
        }

        Ok(segment_entries)
    }

    /// 将 seg entry 添加到 uncommitted 集合
    pub fn add_segment(&self, segment_entry: SegmentEntry) {
        let mut registers_lock = self.write();
        registers_lock.uncommitted.add_segment_entry(segment_entry);
    }
    /// 返回 seg ids 合并前的一致状态（committed or uncommitted）</br>
    /// 确定合并前的 seg ids 属于 committed 还是 uncommitted 集合，并将这组 seg ids 从对应集合移除，最后将合并后产生的新的 seg 放到对应集合
    pub(crate) fn end_merge(
        &self,
        before_merge_segment_ids: &[SegmentId],
        after_merge_segment_entry: Option<SegmentEntry>,
    ) -> crate::Result<SegmentsStatus> {
        let mut registers_lock = self.write();
        let segments_status = registers_lock
            .segments_status(before_merge_segment_ids)
            .ok_or_else(|| {
                warn!("couldn't find segment in SegmentManager");
                crate::SparseError::InvalidArgument(
                    "The segments that were merged could not be found in the SegmentManager. This \
                     is not necessarily a bug, and can happen after a rollback for instance."
                        .to_string(),
                )
            })?;

        let target_register: &mut SegmentRegister = match segments_status {
            SegmentsStatus::Uncommitted => &mut registers_lock.uncommitted,
            SegmentsStatus::Committed => &mut registers_lock.committed,
        };
        for segment_id in before_merge_segment_ids {
            target_register.remove_segment(segment_id);
        }
        if let Some(entry) = after_merge_segment_entry {
            target_register.add_segment_entry(entry);
        }
        Ok(segments_status)
    }

    /// 返回已经提交的 segment metas（committed 状态）
    pub fn committed_segment_metas(&self) -> Vec<SegmentMeta> {
        self.remove_empty_segments();
        let registers_lock = self.read();
        registers_lock.committed.segment_metas()
    }
}
