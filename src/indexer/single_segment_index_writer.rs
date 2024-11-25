use crate::directory::directory::Directory;
use std::marker::PhantomData;

use crate::core::{SparseRowContent, SparseVector};
use crate::index::{Index, IndexMeta, Segment};
use crate::indexer::operation::AddOperation;
use crate::indexer::segment_updater::save_metas;
use crate::indexer::SegmentWriter;
use crate::Opstamp;

#[doc(hidden)]
pub struct SingleSegmentIndexWriter {
    segment_writer: SegmentWriter,
    segment: Segment,
    opstamp: Opstamp,
    _phantom: PhantomData<SparseRowContent>,
}

impl SingleSegmentIndexWriter {
    pub fn new(index: Index, mem_budget: usize) -> crate::Result<Self> {
        let segment = index.new_segment();
        let segment_writer = SegmentWriter::for_segment(mem_budget, segment.clone())?;
        Ok(Self {
            segment_writer,
            segment,
            opstamp: 0,
            _phantom: PhantomData,
        })
    }

    pub fn mem_usage(&self) -> usize {
        self.segment_writer.mem_usage()
    }

    pub fn add_row_content(&mut self, row_content: SparseRowContent) -> crate::Result<()> {
        let opstamp = self.opstamp;
        self.opstamp += 1;
        self.segment_writer.index_row_content(AddOperation {
            opstamp,
            row_content,
        });
        Ok(())
    }

    pub fn finalize(self) -> crate::Result<Index> {
        let max_doc = self.segment_writer.rows_count();
        self.segment_writer.finalize()?;
        let segment: Segment = self.segment.with_rows_count(max_doc);
        let index = segment.index();
        let index_meta = IndexMeta {
            segments: vec![segment.meta().clone()],
            opstamp: 0,
            payload: None,
        };
        save_metas(&index_meta, index.directory())?;
        index.directory().sync_directory()?;
        Ok(segment.index().clone())
    }
}
