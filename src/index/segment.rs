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
    pub(crate) fn for_index(index: Index, meta: SegmentMeta) -> Segment {
        Segment { index, meta }
    }

    pub fn id(&self) -> SegmentId {
        self.meta.id()
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    /// Before serializing and storing the Segment, the `rows_count` recorded in its `SegmentMeta` needs to be updated.
    /// This function can only be called once to update the metadata before serializing and storing the Segment.
    pub(crate) fn with_rows_count(self, rows_count: RowId) -> Segment {
        Segment { index: self.index, meta: self.meta.with_rows_count(rows_count) }
    }

    /// Returns the relative path of the segment component.
    /// The relative path can be viewed as the name of the related files within the Index, consisting of uuid + suffix.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        self.meta.relative_path(component)
    }

    /// Open one of the component file for a *regular* read.
    // TODO: we can use it to read mmap file.
    pub fn open_read(&self, component: SegmentComponent) -> Result<FileSlice, OpenReadError> {
        let path = self.relative_path(component);
        self.index.directory().open_read(&path)
    }

    /// Open one of the component file for *regular* write.
    // TODO: we can use it to store mmap file.
    pub fn open_write(&mut self, component: SegmentComponent) -> Result<WritePtr, OpenWriteError> {
        let path = self.relative_path(component);
        let write = self.index.directory_mut().open_write(&path)?;
        Ok(write)
    }
}
