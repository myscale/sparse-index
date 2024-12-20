mod common;
mod compressed_mmap;
mod compressed_ram;
mod mmap;
mod ram;
mod traits;

pub use common::*;
pub use compressed_mmap::*;
pub use compressed_ram::*;
pub use mmap::*;
pub use ram::*;
pub use traits::*;

// COMMON META FILE
pub const INVERTED_INDEX_META_FILE_SUFFIX: &str = ".meta.json";
pub const INVERTED_INDEX_FILE_NAME: &str = "inverted_index";

// FOR SIMPLE INVERTED INDEX
pub const INVERTED_INDEX_HEADERS_SUFFIX: &str = ".headers";
pub const INVERTED_INDEX_POSTINGS_SUFFIX: &str = ".postings";

// FOR COMPRESSED BLOCKS
pub const COMPRESSED_INVERTED_INDEX_HEADERS_SUFFIX: &str = ".cmp.headers";
pub const COMPRESSED_INVERTED_INDEX_ROW_IDS_SUFFIX: &str = ".cmp.row_ids";
pub const COMPRESSED_INVERTED_INDEX_POSTING_BLOCKS_SUFFIX: &str = ".cmp.blocks";
