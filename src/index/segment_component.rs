use std::slice;

/// Enum describing each component of a tantivy segment.
/// Each component is stored in its own file,
/// using the pattern `segment_uuid`.`component_extension`,
/// except the delete component that takes an `segment_uuid`.`delete_opstamp`.`component_extension`
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum SegmentComponent {
    /// 倒排索引数据部分
    /// TODO 后续可以考虑拆分成多个文件存储，这样说不定访问效率会大大增加
    InvertedIndexData,
    InvertedIndexMeta,

    // 临时索引文件存储
    // TempInvertedIndex,

    // 删除掉的 row_id，用来构造 alive bitset
    // Delete,
}

impl SegmentComponent {
    /// Iterates through the components.
    pub fn iterator() -> slice::Iter<'static, SegmentComponent> {
        static SEGMENT_COMPONENTS: [SegmentComponent; 2] = [
            SegmentComponent::InvertedIndexData,
            SegmentComponent::InvertedIndexMeta,
        ];
        SEGMENT_COMPONENTS.iter()
    }
}
