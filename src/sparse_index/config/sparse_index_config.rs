use crate::core::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const SPARSE_INDEX_CONFIG_FILE: &str = "sparse_index_config.json";

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Clone, Copy)]
pub enum SparseIndexType {
    #[serde(rename = "mutable_ram")]
    MutableRam,

    #[serde(rename = "immutable_ram")]
    ImmutableRam,

    #[default]
    #[serde(rename = "mmap")]
    Mmap,
}

impl SparseIndexType {
    pub fn is_appendable(&self) -> bool {
        *self == Self::MutableRam
    }

    pub fn is_immutable(&self) -> bool {
        *self != Self::MutableRam
    }

    pub fn is_on_disk(&self) -> bool {
        *self == Self::Mmap
    }

    pub fn is_persisted(&self) -> bool {
        *self == Self::Mmap || *self == Self::ImmutableRam
    }
}

// TODO Copy 和 Clone 啥关系
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum VectorStorageDatatype {
    #[default]
    #[serde(rename = "f32")]
    Float32,
    #[serde(rename = "f16")]
    Float16,
    #[serde(rename = "u8")]
    UInt8,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SparseIndexConfig {
    #[serde(default)]
    pub index_type: SparseIndexType,

    #[serde(default)]
    pub datatype: VectorStorageDatatype,

    #[serde(default = "default_compressed")]
    pub compressed: bool,
}

fn default_compressed() -> bool {
    true
}

impl SparseIndexConfig {
    pub fn new(
        index_type: SparseIndexType,
        datatype: VectorStorageDatatype,
        compressed: bool,
    ) -> Self {
        SparseIndexConfig {
            index_type,
            datatype,
            compressed,
        }
    }

    pub fn load(index_path: &Path) -> Result<Self, FileOperationError> {
        let file_path = index_path.join(SPARSE_INDEX_CONFIG_FILE);
        read_json(&file_path)
    }

    pub fn save(&self, index_path: &Path) -> Result<(), FileOperationError> {
        let file_path = index_path.join(SPARSE_INDEX_CONFIG_FILE);
        Ok(atomic_save_json(&file_path, self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sparse_index_type() {
        assert!(SparseIndexType::MutableRam.is_appendable());
        assert!(SparseIndexType::ImmutableRam.is_immutable());
        assert!(!SparseIndexType::ImmutableRam.is_on_disk());
        assert!(SparseIndexType::Mmap.is_persisted());
    }

    #[test]
    fn test_parse_config() {
        let empty_config = "{}";
        let empty_config: SparseIndexConfig = serde_json::from_str(empty_config).expect("");
        assert_eq!(
            empty_config,
            SparseIndexConfig::new(
                SparseIndexType::default(),
                VectorStorageDatatype::default(),
                true
            )
        );

        let config = "{\"index_type\":\"mmap\",\"datatype\":\"f16\",\"compressed\":true}";
        let config: SparseIndexConfig = serde_json::from_str(config).expect("");
        assert_eq!(
            config,
            SparseIndexConfig::new(SparseIndexType::Mmap, VectorStorageDatatype::Float16, true)
        );
    }

    #[test]
    fn test_load_and_save() {
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let index_path = temp_dir.path();

        let config =
            SparseIndexConfig::new(SparseIndexType::Mmap, VectorStorageDatatype::Float16, true);

        config.save(index_path).expect("Failed to save config");

        let loaded_config = SparseIndexConfig::load(index_path).expect("Failed to load config");
        assert_eq!(config, loaded_config);
    }
}
