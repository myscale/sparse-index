use super::BitPackerImpl;
use crate::core::common::types::ElementOffsetType;
use bitpacking::BitPacker;

#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub struct CompressedPostingChunk<W> {
    /// Initial data point id. Used for decompression.
    pub(super) initial: ElementOffsetType,

    /// An offset within id_data
    pub(super) offset: u32,

    /// Weight values for the chunk. 128
    pub(super) weights: [W; BitPackerImpl::BLOCK_LEN],
}
