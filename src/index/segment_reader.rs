use super::{Segment, SegmentId};
use crate::core::searcher::Searcher;
use crate::core::{GenericInvertedIndex, SparseBitmap, SparseVector, TopK, StorageType};
use crate::directory::Directory;
use crate::RowId;
use std::fmt;

#[derive(Clone)]
pub struct SegmentReader {
    index_searcher: Searcher,
    segment_id: SegmentId,
    rows_count: RowId,
}

/// metrics
impl SegmentReader {
    pub fn rows_count(&self) -> RowId {
        self.rows_count as RowId
    }

    /// Returns the segment id
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    pub fn get_inverted_index(&self) -> &GenericInvertedIndex {
        self.index_searcher.get_inverted_index()
    }
}

impl SegmentReader {
    pub fn open(segment: &Segment) -> crate::Result<SegmentReader> {
        let rows_count: RowId = segment.meta().rows_count();
        let index_path = segment.index().directory().get_path().unwrap();

        assert_ne!(segment.index().index_settings.inverted_index_config.storage_type, StorageType::Ram);

        let inverted_index: GenericInvertedIndex = GenericInvertedIndex::open_from(
            &index_path,
            Some(&segment.id().uuid_string()),
            &segment.index().index_settings,
        )?;

        Ok(SegmentReader {
            index_searcher: Searcher::new(inverted_index),
            segment_id: segment.id(),
            rows_count,
        })
    }

    pub fn search(&self, query: &SparseVector, sparse_bitmap: &Option<SparseBitmap>, limits: u32) -> crate::Result<TopK> {
        Ok(self.index_searcher.search(query, sparse_bitmap, limits))
    }

    pub fn brute_force_search(&self, query: &SparseVector, sparse_bitmap: &Option<SparseBitmap>, limits: u32) -> crate::Result<TopK> {
        Ok(self.index_searcher.plain_search(query, sparse_bitmap, limits))
    }
}

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentReader")
            .field("segment_id", &self.segment_id())
            .field("rows_count", &self.rows_count())
            .finish()
    }
}
