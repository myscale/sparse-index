use super::SegmentComponent;
use crate::core::{
    COMPRESSED_INVERTED_INDEX_HEADERS_SUFFIX, COMPRESSED_INVERTED_INDEX_POSTING_BLOCKS_SUFFIX,
    COMPRESSED_INVERTED_INDEX_ROW_IDS_SUFFIX, INVERTED_INDEX_HEADERS_SUFFIX,
    INVERTED_INDEX_META_FILE_SUFFIX, INVERTED_INDEX_POSTINGS_SUFFIX,
};
use crate::index::SegmentId;
use crate::{Opstamp, RowId};
use census::{Inventory, TrackedObject};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// SegmentMeta Inventory
#[derive(Clone, Default)]
pub(crate) struct SegmentMetaInventory {
    inventory: Inventory<InnerSegmentMeta>,
}

impl SegmentMetaInventory {
    /// return all segment_metas in inventory.
    pub fn all(&self) -> Vec<SegmentMeta> {
        self.inventory.list().into_iter().map(SegmentMeta::from).collect::<Vec<_>>()
    }

    /// create new segment_meta and record it into inventory.
    pub fn new_segment_meta(
        &self,
        directory: PathBuf,
        segment_id: SegmentId,
        rows_count: u32,
    ) -> SegmentMeta {
        let inner = InnerSegmentMeta {
            directory,
            segment_id,
            rows_count,
            include_temp_sv_store: Arc::new(AtomicBool::new(true)),
        };
        SegmentMeta::from(self.inventory.track(inner))
    }
}

#[derive(Clone)]
pub struct SegmentMeta {
    tracked: TrackedObject<InnerSegmentMeta>,
}

impl fmt::Debug for SegmentMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.tracked.fmt(f)
    }
}

impl serde::Serialize for SegmentMeta {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error> {
        self.tracked.serialize(serializer)
    }
}

impl From<TrackedObject<InnerSegmentMeta>> for SegmentMeta {
    fn from(tracked: TrackedObject<InnerSegmentMeta>) -> SegmentMeta {
        SegmentMeta { tracked }
    }
}

impl SegmentMeta {
    /// return current segment id
    pub fn id(&self) -> SegmentId {
        self.tracked.segment_id
    }

    /// remove `Component::TempInvertedIndex`
    ///
    /// It makes `.tmp` file can get GC.
    pub fn untrack_temp_svstore(&self) {
        self.tracked.include_temp_sv_store.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Return all file names used by this segment_meta.(files related in a single segment.)
    /// Note: Some of the returned files may not exist depending on the state of the segment.
    ///
    /// It's important for these files will not used by segment anymore.
    pub fn list_files(&self) -> HashSet<PathBuf> {
        SegmentComponent::iterator()
            .map(|component| self.relative_path(*component))
            .collect::<HashSet<PathBuf>>()
    }

    // TODO: refine for different version.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        let mut path = self.id().uuid_string();
        path.push_str(&match component {
            SegmentComponent::InvertedIndexMeta => INVERTED_INDEX_META_FILE_SUFFIX.to_string(),
            SegmentComponent::InvertedIndexHeaders => INVERTED_INDEX_HEADERS_SUFFIX.to_string(),
            SegmentComponent::InvertedIndexPostings => INVERTED_INDEX_POSTINGS_SUFFIX.to_string(),
            SegmentComponent::CompressedInvertedIndexHeaders => {
                COMPRESSED_INVERTED_INDEX_HEADERS_SUFFIX.to_string()
            }
            SegmentComponent::CompressedInvertedIndexRowIds => {
                COMPRESSED_INVERTED_INDEX_ROW_IDS_SUFFIX.to_string()
            }
            SegmentComponent::CompressedInvertedIndexBlocks => {
                COMPRESSED_INVERTED_INDEX_POSTING_BLOCKS_SUFFIX.to_string()
            } // SegmentComponent::Delete => ".delete".to_string(),
        });
        PathBuf::from(path)
    }

    pub fn rows_count(&self) -> RowId {
        self.tracked.rows_count
    }

    // TODO: support delete operation.
    pub fn alive_rows_count(&self) -> RowId {
        self.rows_count()
    }

    /// This function will only be called when SegmentMeta.rows_count is ZERO.
    /// It usually called after a new segment was serialized.
    pub(crate) fn with_rows_count(self, rows_count: RowId) -> SegmentMeta {
        assert_eq!(self.tracked.rows_count, 0);
        // assert!(self.tracked.deletes.is_none());
        let tracked = self.tracked.map(move |inner_meta: &InnerSegmentMeta| InnerSegmentMeta {
            directory: inner_meta.directory.clone(),
            segment_id: inner_meta.segment_id,
            rows_count,
            // deletes: None,
            include_temp_sv_store: Arc::new(AtomicBool::new(true)),
        });
        SegmentMeta { tracked }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InnerSegmentMeta {
    // directory of segment, is also index's path
    directory: PathBuf,
    segment_id: SegmentId,
    rows_count: RowId,

    /// If want avoid GC temp file, set it to true.
    #[serde(skip)]
    #[serde(default = "default_temp_store")]
    pub(crate) include_temp_sv_store: Arc<AtomicBool>,
}
fn default_temp_store() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

impl InnerSegmentMeta {
    /// consume InnerSegmentMeta and put it into inventory.
    pub fn track(self, inventory: &SegmentMetaInventory) -> SegmentMeta {
        SegmentMeta { tracked: inventory.inventory.track(self) }
    }
}

/// Metadata information for the Index
///
/// This metadata is stored in a `meta.json` file on disk and records a set of segment metadata
/// that can be used for searching.
#[derive(Clone, Serialize)]
pub struct IndexMeta {
    /// Stores a set of segment metadata which already serialized.
    pub segments: Vec<SegmentMeta>,
    /// Opstamp of the last `commit` operation
    pub opstamp: Opstamp,
    /// User-provided comment information at the time of `commit`, which is meaningless at the code level
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl fmt::Debug for IndexMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::ser::to_string(self)
                .expect("JSON serialization for IndexMeta should never fail.")
        )
    }
}

impl IndexMeta {
    /// Create a brand new IndexMeta, doesn't contain any segment.
    pub fn default() -> Self {
        Self { segments: Vec::new(), opstamp: 0u64, payload: None }
    }

    /// parse meta.json into IndexMeta obj.
    pub(crate) fn deserialize(
        meta_json: &str,
        inventory: &SegmentMetaInventory,
    ) -> serde_json::Result<IndexMeta> {
        let untracked_meta_json: UntrackedIndexMeta = serde_json::from_str(meta_json)?;
        Ok(untracked_meta_json.track(inventory))
    }
}

/// When loading the Index, UntrackedIndexMeta is needed to parse all the segment files.
#[derive(Deserialize, Debug)]
pub struct UntrackedIndexMeta {
    pub segments: Vec<InnerSegmentMeta>,

    /// Opstamp of last `commit`
    pub opstamp: Opstamp,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl UntrackedIndexMeta {
    /// Hand over the untracked `segments` to the `inventory` for management.
    /// Consume `UntrackedIndexMeta` and generate a new `IndexMeta` object.
    pub fn track(self, inventory: &SegmentMetaInventory) -> IndexMeta {
        IndexMeta {
            segments: self
                .segments
                .into_iter()
                .map(|inner_seg_meta| inner_seg_meta.track(inventory))
                .collect::<Vec<SegmentMeta>>(),
            opstamp: self.opstamp,
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::index::index_meta::{IndexMeta, UntrackedIndexMeta};

    #[test]
    fn test_serialize_metas() {
        let index_metas = IndexMeta { segments: Vec::new(), opstamp: 0u64, payload: None };
        let json = serde_json::ser::to_string(&index_metas).expect("serialization failed");
        assert_eq!(json, r#"{"segments":[],"opstamp":0}"#);

        let deser_meta: UntrackedIndexMeta = serde_json::from_str(&json).unwrap();

        assert_eq!(index_metas.opstamp, deser_meta.opstamp);
    }
}
