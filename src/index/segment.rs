use std::fmt;
use std::path::PathBuf;

use super::SegmentComponent;
use crate::directory::error::{OpenReadError, OpenWriteError};
use crate::directory::{Directory, FileSlice, WritePtr};
use crate::index::{Index, SegmentId, SegmentMeta};
use crate::RowId;

/// A segment is a piece of the index.
#[derive(Clone)]
pub struct Segment {
    index: Index,
    meta: SegmentMeta,
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Segment({:?})", self.id().uuid_string())
    }
}

impl Segment {
    /// 根据给定的 `Index` 和 `SegmentMeta` 初始化一个 `Segment` 对象
    pub(crate) fn for_index(index: Index, meta: SegmentMeta) -> Segment {
        Segment { index, meta }
    }


    /// 获得 `Segment` 的 uuid
    pub fn id(&self) -> SegmentId {
        self.meta.id()
    }

    /// 获得 Segment 所属的 Index
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// 获得 SegmentMeta 信息
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    /// 序列化存储 Segment 之前, 需要更新它的 `SegmentMeta` 内记录的 rows_count </br>
    /// 这个函数只能在序列化存储 Segment 前更新元数据时被调用一次
    pub(crate) fn with_rows_count(self, rows_count: RowId) -> Segment {
        Segment {
            index: self.index,
            meta: self.meta.with_rows_count(rows_count),
        }
    }


    /// 返回 segment component 的相对路径 </br>
    /// 可以将相对路径视为 Index 内相关文件的名字, uuid + suffix
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        self.meta.relative_path(component)
    }

    /// Open one of the component file for a *regular* read.
    // TODO 应该不再需要了
    pub fn open_read(&self, component: SegmentComponent) -> Result<FileSlice, OpenReadError> {
        let path = self.relative_path(component);
        self.index.directory().open_read(&path)
    }

    /// Open one of the component file for *regular* write.
    // TODO 需要它写入数据吗？
    pub fn open_write(&mut self, component: SegmentComponent) -> Result<WritePtr, OpenWriteError> {
        let path = self.relative_path(component);
        let write = self.index.directory_mut().open_write(&path)?;
        Ok(write)
    }
}
