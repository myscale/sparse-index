use crate::core::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const SPARSE_INDEX_CONFIG_FILE: &str = "sparse_index_config.json";

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum StorageType {
    #[default]
    #[serde(rename = "mmap")]
    Mmap,

    #[serde(rename = "compressed_mmap")]
    CompressedMmap,

    #[serde(rename = "ram")]
    Ram,
}

// TODO Copy 和 Clone 啥关系
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum IndexWeightType {
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
    #[serde(rename = "storage")]
    pub storage_type: StorageType,

    #[serde(default)]
    #[serde(rename = "weight")]
    pub weight_type: IndexWeightType,

    #[serde(default)]
    #[serde(rename = "quantized")]
    pub quantized: bool,
}

impl SparseIndexConfig {
    pub fn new(storage_type: StorageType, weight_type: IndexWeightType, quantized: bool) -> Self {
        SparseIndexConfig { storage_type, weight_type, quantized }
    }

    pub fn load(index_path: &Path) -> Result<Self, FileOperationError> {
        let file_path = index_path.join(SPARSE_INDEX_CONFIG_FILE);
        read_json(&file_path)
    }

    pub fn save(&self, index_path: &Path) -> Result<(), FileOperationError> {
        let file_path = index_path.join(SPARSE_INDEX_CONFIG_FILE);
        if !index_path.exists() {
            std::fs::create_dir_all(index_path).map_err(|e| FileOperationError::IoError(e))?;
        }
        Ok(atomic_save_json(&file_path, self)?)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tempfile::tempdir;

//     #[test]
//     fn test_parse_config() {
//         let empty_config = "{}";
//         let empty_config: SparseIndexConfig = serde_json::from_str(empty_config).expect("");
//         assert_eq!(
//             empty_config,
//             SparseIndexConfig::new(
//                 StorageType::default(),
//                 WeightType::default(),
//                 false,
//             )
//         );

//         let config = "{\"storage\":\"mmap\",\"weight\":\"f32\"}";
//         let config: SparseIndexConfig = serde_json::from_str(config).expect("");
//         assert_eq!(
//             config,
//             SparseIndexConfig::new(StorageType::Mmap, WeightType::Float32, false)
//         );
//     }

//     #[test]
//     fn test_load_and_save() {
//         let temp_dir = tempdir().expect("Failed to create temporary directory");
//         let index_path = temp_dir.path();

//         let config =
//             SparseIndexConfig::new(StorageType::Mmap, WeightType::Float32, false);

//         config.save(index_path).expect("Failed to save config");

//         let loaded_config = SparseIndexConfig::load(index_path).expect("Failed to load config");
//         assert_eq!(config, loaded_config);
//     }
// }
