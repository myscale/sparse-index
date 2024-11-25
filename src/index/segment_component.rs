use std::slice;

/// Enum describing each component of a tantivy segment.
/// Each component is stored in its own file,
/// using the pattern `segment_uuid`.`component_extension`,
/// except the delete component that takes an `segment_uuid`.`delete_opstamp`.`component_extension`
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum SegmentComponent {
    InvertedIndexMeta,
    // For simple inverted index.
    InvertedIndexHeaders,
    InvertedIndexPostings,
    // For compressed inverted index.
    CompressedInvertedIndexHeaders,
    CompressedInvertedIndexRowIds,
    CompressedInvertedIndexBlocks,
    // TODO 追加一些 merge 过程中产生的临时文件

    // 临时索引文件存储
    // TempInvertedIndex,

    // 删除掉的 row_id，用来构造 alive bitset
    // Delete,
}

impl SegmentComponent {
    /// Iterates through the components.
    pub fn iterator() -> slice::Iter<'static, SegmentComponent> {
        static SEGMENT_COMPONENTS: [SegmentComponent; 6] = [
            SegmentComponent::InvertedIndexMeta,
            SegmentComponent::InvertedIndexHeaders,
            SegmentComponent::InvertedIndexPostings,
            SegmentComponent::CompressedInvertedIndexHeaders,
            SegmentComponent::CompressedInvertedIndexRowIds,
            SegmentComponent::CompressedInvertedIndexBlocks,
        ];
        SEGMENT_COMPONENTS.iter()
    }
}
