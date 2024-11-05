mod traits;

mod inverted_index_mmap;
mod inverted_index_ram;
mod inverted_index_ram_builder;

pub use inverted_index_mmap::InvertedIndexMmap;
pub use inverted_index_mmap::InvertedIndexMmapFileConfig;
pub use inverted_index_mmap::PostingListOffset;
pub use inverted_index_mmap::POSTING_OFFSET_SIZE;
pub use inverted_index_ram::InvertedIndexRam;
pub use inverted_index_ram_builder::InvertedIndexBuilder;
pub use traits::*;

pub const INVERTED_INDEX_FILE_NAME: &str = "inverted_index";
pub const INVERTED_INDEX_OFFSETS_SUFFIX: &str = ".offsets";
pub const INVERTED_INDEX_POSTINGS_SUFFIX: &str = ".postings";
pub const INVERTED_INDEX_META_FILE_SUFFIX: &str = ".meta.json";
