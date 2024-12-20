use serde::{Deserialize, Serialize};

use crate::core::inverted_index::common::InvertedIndexMeta;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct CompressedMmapInvertedIndexMeta {
    #[serde(flatten)]
    pub inverted_index_meta: InvertedIndexMeta,

    pub row_ids_storage_size: u64,
    pub headers_storage_size: u64,
    pub total_blocks_count: u64,
    pub blocks_storage_size: u64,
}
