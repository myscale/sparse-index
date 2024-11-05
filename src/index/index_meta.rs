use super::SegmentComponent;
use crate::core::{
    INVERTED_INDEX_META_FILE_SUFFIX, INVERTED_INDEX_OFFSETS_SUFFIX, INVERTED_INDEX_POSTINGS_SUFFIX,
};
use crate::index::SegmentId;
use crate::{Opstamp, RowId};
use census::{Inventory, TrackedObject};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// SegmentMeta 仓库
#[derive(Clone, Default)]
pub(crate) struct SegmentMetaInventory {
    inventory: Inventory<InnerSegmentMeta>,
}

impl SegmentMetaInventory {
    /// 返回 inventory 仓库中记录的所有 SegmentMeta
    pub fn all(&self) -> Vec<SegmentMeta> {
        self.inventory
            .list()
            .into_iter()
            .map(SegmentMeta::from)
            .collect::<Vec<_>>()
    }

    /// 创建新的 SegmentMeta 并记录到 inventory 仓库
    pub fn new_segment_meta(
        &self,
        directory: PathBuf,
        segment_id: SegmentId,
        rows_count: u32,
    ) -> SegmentMeta {
        let inner = InnerSegmentMeta {
            directory,
            segment_id,
            rows_count,
            include_temp_sv_store: Arc::new(AtomicBool::new(true)),
        };
        SegmentMeta::from(self.inventory.track(inner))
    }
}

/// `SegmentMeta` 包含一个 `Segment` 相关的元数据信息, 如 `rows_count`, `deleted_count` 等 </br>
/// *目前不考虑实现删除功能*
#[derive(Clone)]
pub struct SegmentMeta {
    tracked: TrackedObject<InnerSegmentMeta>,
}

impl fmt::Debug for SegmentMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.tracked.fmt(f)
    }
}

impl serde::Serialize for SegmentMeta {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error> {
        self.tracked.serialize(serializer)
    }
}

impl From<TrackedObject<InnerSegmentMeta>> for SegmentMeta {
    fn from(tracked: TrackedObject<InnerSegmentMeta>) -> SegmentMeta {
        SegmentMeta { tracked }
    }
}

impl SegmentMeta {
    /// 返回当前 segment id
    pub fn id(&self) -> SegmentId {
        self.tracked.segment_id
    }

    /// 移除掉 `Component::TempInvertedIndex`
    ///
    /// 这可使得 `.tmp` 文件被垃圾回收机制收集清理
    pub fn untrack_temp_svstore(&self) {
        self.tracked
            .include_temp_sv_store
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// 返回 SegmentMeta 中需要的所有 segment 文件路径（文件名）
    /// Note: Some of the returned files may not exist depending on the state of the segment.
    ///
    /// 这对于移除那些不再被 segment 使用的文件是非常必要的
    pub fn list_files(&self) -> HashSet<PathBuf> {
        // if self
        //     .tracked
        //     .include_temp_sv_store
        //     .load(std::sync::atomic::Ordering::Relaxed)
        // {
        //     SegmentComponent::iterator()
        //         .map(|component| self.relative_path(*component))
        //         .collect::<HashSet<PathBuf>>()
        // } else {
        //     SegmentComponent::iterator()
        //         .filter(|comp| *comp != &SegmentComponent::TempInvertedIndex)
        //         .map(|component| self.relative_path(*component))
        //         .collect::<HashSet<PathBuf>>()
        // }
        SegmentComponent::iterator()
            .map(|component| self.relative_path(*component))
            .collect::<HashSet<PathBuf>>()
    }

    /// 返回一个特定类型 segment 的文件名称
    /// TODO 在参数里面加上 version 参数，函数内部根据不同的 version 去返回每个版本需要的文件路径
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        let mut path = self.id().uuid_string();
        path.push_str(&match component {
            // TODO 怎么处理比较好？
            SegmentComponent::InvertedIndexPostings => INVERTED_INDEX_POSTINGS_SUFFIX.to_string(),
            SegmentComponent::InvertedIndexOffsets => INVERTED_INDEX_OFFSETS_SUFFIX.to_string(),
            SegmentComponent::InvertedIndexMeta => INVERTED_INDEX_META_FILE_SUFFIX.to_string(),
            // SegmentComponent::Delete => ".delete".to_string(),
        });
        PathBuf::from(path)
    }

    /// 返回当前 segment 存储的 sparse vectors 的数量
    pub fn rows_count(&self) -> RowId {
        self.tracked.rows_count
    }

    /// 返回当前 segment 存储的 sparse vectors 数量
    /// *后续支持 delete rows 时会使用到 alive rows count*
    pub fn alive_rows_count(&self) -> RowId {
        self.rows_count()
    }

    /// 更新当前 `SegmentMeta` 的 rows_count 字段
    ///
    /// 该方法只有在 SegmentMeta 的 rows_count 为 0 的时候才会被调用，目的是序列化一个新的 segment
    pub(crate) fn with_rows_count(self, rows_count: RowId) -> SegmentMeta {
        assert_eq!(self.tracked.rows_count, 0);
        // assert!(self.tracked.deletes.is_none());
        let tracked = self
            .tracked
            .map(move |inner_meta: &InnerSegmentMeta| InnerSegmentMeta {
                directory: inner_meta.directory.clone(),
                segment_id: inner_meta.segment_id,
                rows_count,
                // deletes: None,
                // TODO 理解这里为什么加上了 temp 类型的 store
                include_temp_sv_store: Arc::new(AtomicBool::new(true)),
            });
        SegmentMeta { tracked }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InnerSegmentMeta {
    // 当前 segment 所处的路径, 即 index 所在的路径
    directory: PathBuf,
    segment_id: SegmentId,
    rows_count: RowId,

    /// 如果要避免合并过程产生的 temp 文件被 GC, 就将这个字段设置为 true
    #[serde(skip)]
    #[serde(default = "default_temp_store")]
    pub(crate) include_temp_sv_store: Arc<AtomicBool>,
}
fn default_temp_store() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

impl InnerSegmentMeta {
    /// 消耗当前 InnerSegmentMeta 并交给 inventory 管理
    pub fn track(self, inventory: &SegmentMetaInventory) -> SegmentMeta {
        SegmentMeta {
            tracked: inventory.inventory.track(self),
        }
    }
}

/// Index 的 Meta 元数据信息
///
/// 该 Meta 数据存储在磁盘上的 meta.json 文件，记录了可被用来搜索的一组 segments 元数据等信息。
#[derive(Clone, Serialize)]
pub struct IndexMeta {
    /// 存储一组已经 `serialized` 的 `segment` 元数据信息
    pub segments: Vec<SegmentMeta>,
    /// 最后一次 `commit` 的操作戳
    pub opstamp: Opstamp,
    /// 用户在 `commit` 时给出的注释信息, 在代码层面它是无意义的
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl fmt::Debug for IndexMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::ser::to_string(self)
                .expect("JSON serialization for IndexMeta should never fail.")
        )
    }
}

impl IndexMeta {
    /// 创建一个全新的 IndexMeta，不包含任何 segment
    pub fn default() -> Self {
        Self {
            segments: Vec::new(),
            opstamp: 0u64,
            payload: None,
        }
    }

    /// 将 meta.json 字符串内容转换为 IndexMeta 对象
    pub(crate) fn deserialize(
        meta_json: &str,
        inventory: &SegmentMetaInventory,
    ) -> serde_json::Result<IndexMeta> {
        let untracked_meta_json: UntrackedIndexMeta = serde_json::from_str(meta_json)?;
        Ok(untracked_meta_json.track(inventory))
    }
}

/// 在加载 Index 时，需要使用 UntrackedIndexMeta 解析所有的 Segments 文件
#[derive(Deserialize, Debug)]
pub struct UntrackedIndexMeta {
    pub segments: Vec<InnerSegmentMeta>,

    /// 最后一次 `commit` 的操作戳
    pub opstamp: Opstamp,

    /// 用户可选的 commit 备注信息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl UntrackedIndexMeta {
    /// 将未被追踪的 `segments` 交给 `inventory` 管理 </br>
    /// 消耗 `UntrackedIndexMeta` 并生成新的 `IndexMeta` 对象
    pub fn track(self, inventory: &SegmentMetaInventory) -> IndexMeta {
        IndexMeta {
            segments: self
                .segments
                .into_iter()
                .map(|inner_seg_meta| inner_seg_meta.track(inventory))
                .collect::<Vec<SegmentMeta>>(),
            opstamp: self.opstamp,
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::index::index_meta::{IndexMeta, UntrackedIndexMeta};

    #[test]
    fn test_serialize_metas() {
        let index_metas = IndexMeta {
            segments: Vec::new(),
            opstamp: 0u64,
            payload: None,
        };
        let json = serde_json::ser::to_string(&index_metas).expect("serialization failed");
        assert_eq!(json, r#"{"segments":[],"opstamp":0}"#);

        let deser_meta: UntrackedIndexMeta = serde_json::from_str(&json).unwrap();

        assert_eq!(index_metas.opstamp, deser_meta.opstamp);
    }
}
