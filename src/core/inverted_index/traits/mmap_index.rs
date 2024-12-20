use crate::core::dispatch::InvertedIndexWrapper;
use crate::core::CompressedInvertedIndexMmap;
use crate::core::InvertedIndexMmap;
use enum_dispatch::enum_dispatch;

use crate::core::inverted_index::common::InvertedIndexMetrics;
use crate::core::inverted_index::InvertedIndexRam;
use crate::core::{DimId, PostingListIter, QuantizedWeight};
use std::borrow::Cow;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

// OW: weight type before quantized.
// TW: weight type after quantized. stored in disk.
pub trait PostingListIterAccess<OW: QuantizedWeight, TW: QuantizedWeight> {
    // iterator should return original weight type when calling peek func.
    type Iter<'a>: PostingListIter<OW, TW> + Clone
    where
        Self: 'a;

    /// Get posting list for dimension id
    fn iter(&self, dim_id: &DimId) -> Option<Self::Iter<'_>>;
}

pub trait InvertedIndexMmapInit<OW: QuantizedWeight, TW: QuantizedWeight>: Sized + Debug {
    fn open(path: &Path, segment_id: Option<&str>) -> std::io::Result<Self>;
    /// convert from a simple ram index.
    fn from_ram_index(
        // ram index can be quantized type.
        ram_index: Cow<InvertedIndexRam<TW>>,
        path: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Self>;
}

#[enum_dispatch]
pub trait InvertedIndexMmapAccess<OW: QuantizedWeight, TW: QuantizedWeight> {
    fn metrics(&self) -> InvertedIndexMetrics;

    // fn check_exists(&self, path: &Path, segment_id: Option<&str>) -> std::io::Result<()>;

    /// Get number of posting lists
    fn size(&self) -> usize;

    /// Check if the core is empty
    fn empty(&self) -> bool {
        self.size() == 0
    }

    /// Get number of posting lists for dimension id
    fn posting_len(&self, dim_id: &DimId) -> Option<usize>;

    /// Files used by this core
    fn files(&self, segment_id: Option<&str>) -> Vec<PathBuf>;
}
