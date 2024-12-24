use std::path::PathBuf;

use log::{debug, info};

use crate::{
    common::errors::SparseError,
    core::GenericInvertedIndex,
    core::InvertedIndexConfig,
    index::{Segment, SegmentReader},
};

pub struct IndexMerger {
    pub(crate) readers: Vec<SegmentReader>,
    pub(crate) index_config: InvertedIndexConfig,
}

impl IndexMerger {
    pub fn open(segments: &[Segment]) -> crate::Result<IndexMerger> {
        if segments.len() == 0 {
            return Err(SparseError::Error(
                "Can't create IndexMerger with given ZERO segments.".to_string(),
            ));
        }
        let segment_readers: Vec<SegmentReader> = segments
            .iter()
            .map(|seg| SegmentReader::open(seg))
            .collect::<crate::Result<Vec<SegmentReader>>>()?;

        // make sure all index_settings are same.
        let index_settings_vec: Vec<_> =
            segments.iter().map(|seg| seg.index().index_settings()).collect();

        if !index_settings_vec.windows(2).all(|w| w[0] == w[1]) {
            return Err(crate::SparseError::Error(
                "index_settings should be same in IndexMerger".to_string(),
            ));
        } else {
            let index_config = segments[0].index().index_settings().inverted_index_config;
            Ok(Self { readers: segment_readers, index_config })
        }
    }

    pub fn merge(
        &self,
        directory: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<(usize, Vec<PathBuf>)> {
        if self.readers.len() == 0 {
            return Ok((0, vec![]));
        }

        let generic_inverted_indexes: Vec<&GenericInvertedIndex> = self
            .readers
            .iter()
            .map(|segment_reader| segment_reader.get_inverted_index())
            .collect::<Vec<&GenericInvertedIndex>>();

        info!(
            ">> try call generic_inverted_index merge, indexes size:{}",
            generic_inverted_indexes.len()
        );
        GenericInvertedIndex::merge(
            generic_inverted_indexes,
            directory,
            segment_id,
            Some(self.index_config.element_type),
        )
    }
}
