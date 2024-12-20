use std::path::PathBuf;
use std::thread;

use super::operation::AddOperation;
use crate::core::GenericInvertedIndexRamBuilder;
use crate::directory::Directory;
use crate::index::Segment;
use crate::sparse_index::SparseIndexConfig;
use crate::RowId;
use log::debug;

pub struct SegmentWriter {
    pub(crate) num_rows_count: RowId,
    pub(crate) memory_budget_in_bytes: usize,
    pub(crate) segment: Segment,
    pub(crate) index_ram_builder: GenericInvertedIndexRamBuilder,
    pub(crate) cfg: SparseIndexConfig,
}

impl SegmentWriter {
    pub fn for_segment(memory_budget_in_bytes: usize, segment: Segment) -> crate::Result<Self> {
        let cfg = &segment.index().index_settings().config;
        let index_ram_builder = GenericInvertedIndexRamBuilder::new(cfg.weight_type, cfg.quantized);
        Ok(Self {
            num_rows_count: 0,
            memory_budget_in_bytes,
            segment,
            index_ram_builder,
            cfg: *cfg,
        })
    }

    pub fn finalize(self) -> crate::Result<Vec<PathBuf>> {
        debug!(
            "[{}] - [finalize] segment: {}, rows_count: {}",
            thread::current().name().unwrap_or_default(),
            self.segment.clone().id(),
            self.num_rows_count
        );

        let directory = self.segment.index().directory().get_path();
        let segment_id = self.segment.id().uuid_string();
        self.index_ram_builder.build_and_flush(
            self.cfg.storage_type,
            self.cfg.weight_type,
            self.cfg.quantized,
            &directory,
            Some(&segment_id),
        )
    }

    pub fn mem_usage(&self) -> usize {
        self.index_ram_builder.memory_usage()
    }

    /// TODO: Refine the way we compute num_rows_count.
    pub fn index_row_content(&mut self, add_operation: AddOperation) -> crate::Result<bool> {
        let AddOperation { opstamp: _, row_content } = add_operation;
        let is_insert_operation =
            self.index_ram_builder.add(row_content.row_id, row_content.sparse_vector);
        if is_insert_operation {
            self.num_rows_count += 1;
        }
        Ok(is_insert_operation)
    }

    pub fn rows_count(&self) -> RowId {
        self.num_rows_count
    }
}
