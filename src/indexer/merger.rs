use std::path::PathBuf;

use crate::{
    common::errors::SparseError,
    core::GenericInvertedIndex,
    index::{Segment, SegmentReader},
    sparse_index::SparseIndexConfig,
};

pub struct IndexMerger {
    pub(crate) readers: Vec<SegmentReader>,
    pub(crate) cfg: SparseIndexConfig,
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
            let cfg = segments[0].index().index_settings().config;
            Ok(Self { readers: segment_readers, cfg })
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

        // TODO 在 config 里面写入 element type
        GenericInvertedIndex::merge(
            generic_inverted_indexes,
            directory,
            segment_id,
            Some(self.cfg.element_type()),
        )
    }
}
