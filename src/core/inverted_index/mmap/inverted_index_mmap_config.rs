use crate::core::{
    INVERTED_INDEX_FILE_NAME, INVERTED_INDEX_HEADERS_SUFFIX, INVERTED_INDEX_META_FILE_SUFFIX,
    INVERTED_INDEX_POSTINGS_SUFFIX,
};

pub struct InvertedIndexMmapFileConfig;

impl InvertedIndexMmapFileConfig {
    pub fn headers_file_name(segment_id: Option<&str>) -> String {
        format!(
            "{}{}",
            segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME),
            INVERTED_INDEX_HEADERS_SUFFIX
        )
    }
    pub fn postings_file_name(segment_id: Option<&str>) -> String {
        format!(
            "{}{}",
            segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME),
            INVERTED_INDEX_POSTINGS_SUFFIX
        )
    }
    pub fn inverted_meta_file_name(segment_id: Option<&str>) -> String {
        format!(
            "{}{}",
            segment_id.unwrap_or(INVERTED_INDEX_FILE_NAME),
            INVERTED_INDEX_META_FILE_SUFFIX
        )
    }
    pub fn get_all_files(segment_id: Option<&str>) -> Vec<String> {
        vec![
            Self::headers_file_name(segment_id),
            Self::postings_file_name(segment_id),
            Self::inverted_meta_file_name(segment_id),
        ]
    }
}
