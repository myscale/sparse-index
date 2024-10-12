use std::borrow::Cow;
use std::path::Path;
use std::thread;

use super::operation::AddOperation;
use crate::core::{
    InvertedIndex, InvertedIndexBuilder, InvertedIndexConfig, InvertedIndexMmap,
};
use crate::directory::Directory;
use crate::index::Segment;

use crate::RowId;
use log::debug;


pub struct SegmentWriter {
    pub(crate) num_rows_count: RowId,
    pub(crate) memory_budget_in_bytes: usize,
    pub(crate) segment: Segment,
    // TODO 需要能够检测到 ram builder 的内存, 超过限制之后就写入到 Segment, 然后创建新的 Segment
    // 直接在 InvertedIndexBuilder 增加一个函数，用来获取当前 ram 的估计值
    pub(crate) index_ram_builder: InvertedIndexBuilder,
    // TODO 增加一个 data writer 的内容用来写入数据
}

impl SegmentWriter {
    /// Segment 级别创建一个索引文件
    pub fn for_segment(memory_budget_in_bytes: usize, segment: Segment) -> crate::Result<Self> {
        Ok(Self {
            num_rows_count: 0,
            memory_budget_in_bytes,
            segment,
            index_ram_builder: InvertedIndexBuilder::new(),
        })
    }

    pub fn finalize(self) -> crate::Result<Vec<u64>> {
        debug!("[{}] - [finalize] segment: {}, rows_count: {}", thread::current().name().unwrap_or_default(), self.segment.clone().id(), self.num_rows_count);

        let index_path = self.segment.index().directory().get_path();

        // 使用 segment uuid 作为 index 的名字
        let mut config: InvertedIndexConfig = InvertedIndexConfig::default();
        config.with_data_prefix(self.segment.id().uuid_string().as_str());
        config.with_meta_prefix(self.segment.id().uuid_string().as_str());

        // let _index = InvertedIndexCompressedMmap::<f32>::from_ram_index(
        let _index = InvertedIndexMmap::from_ram_index(
            Cow::Owned(self.index_ram_builder.build()),
            index_path,
            Some(config.clone())
        )?;

        return Ok(Vec::new());
    }

    /// 检查 memory 使用
    pub fn mem_usage(&self) -> usize {
        self.index_ram_builder.memory_usage()
    }

    /// 索引一行数据
    pub fn index_row_content(
        &mut self,
        add_operation: AddOperation,
    ) -> crate::Result<bool> {
        let AddOperation {
            opstamp: _,
            row_content,
        } = add_operation;
        self.index_ram_builder.add(row_content.row_id, row_content.sparse_vector);
        self.num_rows_count += 1;
        Ok(true)
    }

    pub fn rows_count(&self) -> RowId {
        self.num_rows_count
    }
}
