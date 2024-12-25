use crate::core::{
    COMPRESSED_INVERTED_INDEX_HEADERS_SUFFIX, COMPRESSED_INVERTED_INDEX_POSTING_BLOCKS_SUFFIX, COMPRESSED_INVERTED_INDEX_ROW_IDS_SUFFIX, INVERTED_INDEX_FILE_NAME,
    INVERTED_INDEX_META_FILE_SUFFIX,
};

pub struct CompressedInvertedIndexMmapConfig;

impl CompressedInvertedIndexMmapConfig {
    pub fn headers_file_name(segment_id: Option<&str>) -> String {
        format!("{}{}", segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME), COMPRESSED_INVERTED_INDEX_HEADERS_SUFFIX)
    }
    pub fn row_ids_file_name(segment_id: Option<&str>) -> String {
        format!("{}{}", segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME), COMPRESSED_INVERTED_INDEX_ROW_IDS_SUFFIX)
    }

    pub fn row_ids_temp_file_name(segment_id: Option<&str>) -> String {
        format!("{}{}.tmp", segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME), COMPRESSED_INVERTED_INDEX_ROW_IDS_SUFFIX)
    }

    pub fn blocks_file_name(segment_id: Option<&str>) -> String {
        format!("{}{}", segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME), COMPRESSED_INVERTED_INDEX_POSTING_BLOCKS_SUFFIX)
    }

    pub fn blocks_temp_file_name(segment_id: Option<&str>) -> String {
        format!("{}{}.tmp", segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME), COMPRESSED_INVERTED_INDEX_POSTING_BLOCKS_SUFFIX)
    }

    pub fn meta_file_name(segment_id: Option<&str>) -> String {
        format!("{}{}", segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME), INVERTED_INDEX_META_FILE_SUFFIX)
    }
    pub fn get_all_files(segment_id: Option<&str>) -> Vec<String> {
        vec![Self::headers_file_name(segment_id), Self::row_ids_file_name(segment_id), Self::blocks_file_name(segment_id), Self::meta_file_name(segment_id)]
    }
}
