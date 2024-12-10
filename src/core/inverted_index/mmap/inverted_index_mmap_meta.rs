use serde::{Deserialize, Serialize};

use crate::core::inverted_index::common::InvertedIndexMeta;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct MmapInvertedIndexMeta {
    #[serde(flatten)]
    pub inverted_index_meta: InvertedIndexMeta,

    pub headers_storage_size: u64,
    pub postings_storage_size: u64,
}
