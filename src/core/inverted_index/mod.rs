mod enums;
mod traits;

// mod inverted_index_compressed_immutable_ram;
// mod inverted_index_compressed_mmap;
mod inverted_index_immutable_ram;
mod inverted_index_mmap;
mod inverted_index_ram;
mod inverted_index_ram_builder;

pub use enums::*;
// pub use inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
// pub use inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
pub use inverted_index_immutable_ram::InvertedIndexImmutableRam;
pub use inverted_index_mmap::InvertedIndexMmap;
pub use inverted_index_ram::InvertedIndexRam;
pub use inverted_index_ram_builder::InvertedIndexBuilder;
pub use traits::*;

pub const INVERTED_INDEX_FILE_DATA_PREFIX: &str = "inverted_index";
pub const INVERTED_INDEX_FILE_DATA_SUFFIX: &str = ".inv.data";

pub const INVERTED_INDEX_FILE_META_PREFIX: &str = "inverted_index_meta";
pub const INVERTED_INDEX_FILE_META_SUFFIX: &str = ".meta.json";

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq)]
pub struct InvertedIndexConfig {
    pub inv_file_data_prefix: String,
    pub inv_file_data_suffix: String,

    pub inv_file_meta_prefix: String,
    pub inv_file_meta_suffix: String,
}

impl Default for InvertedIndexConfig {
    fn default() -> Self {
        Self { 
            inv_file_data_prefix: INVERTED_INDEX_FILE_DATA_PREFIX.to_owned(), 
            inv_file_data_suffix: INVERTED_INDEX_FILE_DATA_SUFFIX.to_owned(), 
            inv_file_meta_prefix: INVERTED_INDEX_FILE_META_PREFIX.to_owned(), 
            inv_file_meta_suffix: INVERTED_INDEX_FILE_META_SUFFIX.to_owned() 
         }
    }
}

impl InvertedIndexConfig {
    pub fn with_data_prefix(&mut self, prefix: &str) {
        self.inv_file_data_prefix = prefix.to_owned();
    }

    pub fn with_meta_prefix(&mut self, prefix: &str) {
        self.inv_file_meta_prefix = prefix.to_owned();
    }

    pub fn data_file_name(&self) -> String {
        return format!("{}{}", self.inv_file_data_prefix, self.inv_file_data_suffix);
    }

    pub fn meta_file_name(&self) -> String {
        return format!("{}{}", self.inv_file_meta_prefix, self.inv_file_meta_suffix);
    }
}