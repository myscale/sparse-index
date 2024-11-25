use crate::core::inverted_index::InvertedIndexRam;
use crate::core::{DimId, PostingListIteratorTrait, QuantizedWeight};
use std::borrow::Cow;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use super::InvertedIndexMetrics;

// OW 是量化前的类型
// TW 是量化后的类型
pub trait InvertedIndexMmapAccess<OW: QuantizedWeight, TW: QuantizedWeight>: Sized + Debug {
    // Posting List Iterator 真正遍历的时候，Posting 内部元素应该是量化后的存储，所以需要用 TW
    // 遍历过程中，外界获得内部数据时，需要反量化，即 OW
    type Iter<'a>: PostingListIteratorTrait<TW, OW> + Clone
    where
        Self: 'a;

    /// 获得 mmap 类型的 index metrics
    fn metrics(&self) -> InvertedIndexMetrics;

    /// 打开一个 inverted index 文件，提供 segment_id 的时候，会从 segment 下面打开，否则就会正常打开一个文件
    fn open(path: &Path, segment_id: Option<&str>) -> std::io::Result<Self>;

    /// 判断 mmap index 所需要的文件是否都存在
    fn check_exists(&self, path: &Path, segment_id: Option<&str>) -> std::io::Result<()>;

    /// Get posting list for dimension id
    fn iter(&self, dim_id: &DimId) -> Option<Self::Iter<'_>>;

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

    /// 从 ram index 转换而来
    fn from_ram_index(
        // ram index 可能是量化后的版本(u8 类型)
        ram_index: Cow<InvertedIndexRam<TW>>,
        path: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Self>;
}
