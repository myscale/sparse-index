use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display, Formatter};

use crate::index::{SegmentId, SegmentMeta};
use crate::indexer::segment_entry::SegmentEntry;

/// The segment register keeps track
/// of the list of segment, their size as well
/// as the state they are in.
///
/// It is consumed by indexes to get the list of
/// segments that are currently searchable,
/// and by the index merger to identify
/// merge candidates.
#[derive(Default)]
pub struct SegmentRegister {
    segment_states: HashMap<SegmentId, SegmentEntry>,
}

impl Debug for SegmentRegister {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "SegmentRegister(")?;
        for k in self.segment_states.keys() {
            write!(f, "{}, ", k.short_uuid_string())?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl Display for SegmentRegister {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "SegmentRegister(")?;
        for k in self.segment_states.keys() {
            write!(f, "{}, ", k.short_uuid_string())?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl SegmentRegister {
    /// clear all record in segment_states.
    pub fn clear(&mut self) {
        self.segment_states.clear();
    }

    /// Retrieve the segment metas that can be merged (those segment IDs that are not currently being merged).
    /// - `in_merge_segment_ids` contains the segment IDs that are currently being merged; these segment IDs should not be merged again.
    pub fn get_mergeable_segments(&self, in_merge_segment_ids: &HashSet<SegmentId>) -> Vec<SegmentMeta> {
        self.segment_states.values().filter(|segment_entry| !in_merge_segment_ids.contains(&segment_entry.segment_id())).map(|segment_entry| segment_entry.meta().clone()).collect()
    }

    /// Return all segment IDs stored in the register.
    pub fn segment_ids(&self) -> Vec<SegmentId> {
        self.segment_states.keys().cloned().collect()
    }

    /// Return all segment entries stored in the register.
    pub fn segment_entries(&self) -> Vec<SegmentEntry> {
        self.segment_states.values().cloned().collect()
    }

    /// Return all segment metas stored in the register.
    pub fn segment_metas(&self) -> Vec<SegmentMeta> {
        self.segment_states.values().map(|segment_entry| segment_entry.meta().clone()).collect()
    }

    /// Check if the register contains all the given segment IDs.
    pub fn contains_all(&self, segment_ids: &[SegmentId]) -> bool {
        segment_ids.iter().all(|segment_id| self.segment_states.contains_key(segment_id))
    }

    /// Add a segment entry to the register.
    pub fn add_segment_entry(&mut self, segment_entry: SegmentEntry) {
        let segment_id = segment_entry.segment_id();
        self.segment_states.insert(segment_id, segment_entry);
    }

    /// Remove a segment entry.
    pub fn remove_segment(&mut self, segment_id: &SegmentId) {
        self.segment_states.remove(segment_id);
    }

    /// Retrieve the SegmentEntry corresponding to a segment ID from the register.
    pub fn get(&self, segment_id: &SegmentId) -> Option<SegmentEntry> {
        self.segment_states.get(segment_id).cloned()
    }

    /// Initialize a register with a given set of segment metas.
    pub fn new(segment_metas: Vec<SegmentMeta>) -> SegmentRegister {
        let mut segment_states = HashMap::new();
        for segment_meta in segment_metas {
            let segment_id = segment_meta.id();
            let segment_entry = SegmentEntry::new(segment_meta, None);
            segment_states.insert(segment_id, segment_entry);
        }
        SegmentRegister { segment_states }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::index::SegmentMetaInventory;

    fn segment_ids(segment_register: &SegmentRegister) -> Vec<SegmentId> {
        segment_register.segment_metas().into_iter().map(|segment_meta| segment_meta.id()).collect()
    }

    #[test]
    fn test_segment_register() {
        let inventory = SegmentMetaInventory::default();

        let mut segment_register = SegmentRegister::default();
        let segment_id_a = SegmentId::generate_random();
        let segment_id_b = SegmentId::generate_random();

        let segment_id_merged = SegmentId::generate_random();

        {
            let segment_meta = inventory.new_segment_meta(PathBuf::default(), segment_id_a, 0u32);
            let segment_entry = SegmentEntry::new(segment_meta, None);
            segment_register.add_segment_entry(segment_entry);
        }
        assert_eq!(segment_ids(&segment_register), vec![segment_id_a]);
        {
            let segment_meta = inventory.new_segment_meta(PathBuf::default(), segment_id_b, 0u32);
            let segment_entry = SegmentEntry::new(segment_meta, None);
            segment_register.add_segment_entry(segment_entry);
        }
        segment_register.remove_segment(&segment_id_a);
        segment_register.remove_segment(&segment_id_b);
        {
            let segment_meta_merged = inventory.new_segment_meta(PathBuf::default(), segment_id_merged, 0u32);
            let segment_entry = SegmentEntry::new(segment_meta_merged, None);
            segment_register.add_segment_entry(segment_entry);
        }
        assert_eq!(segment_ids(&segment_register), vec![segment_id_merged]);
    }
}
