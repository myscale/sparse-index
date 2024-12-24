use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::core::{atomic_save_json, read_json, FileOperationError, InvertedIndexConfig};

pub const INDEX_SETTINGS: &str = "index_settings.json";

/// Search Index Settings.
///
/// Contains settings which are applied on the whole
/// index, like presort documents.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct IndexSettings {
    pub inverted_index_config: InvertedIndexConfig,
}

impl From<InvertedIndexConfig> for IndexSettings {
    fn from(value: InvertedIndexConfig) -> Self {
        Self { inverted_index_config: value }
    }
}

impl IndexSettings {
    pub fn load(index_path: &Path) -> Result<Self, FileOperationError> {
        let file_path = index_path.join(INDEX_SETTINGS);
        read_json(&file_path)
    }

    pub fn save(&self, index_path: &Path) -> Result<(), FileOperationError> {
        let file_path = index_path.join(INDEX_SETTINGS);
        if !index_path.exists() {
            std::fs::create_dir_all(index_path).map_err(|e| FileOperationError::IoError(e))?;
        }
        Ok(atomic_save_json(&file_path, self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // #[test]
    // fn test_parse_config() {
    //     let empty_config = "{}";
    //     let empty_config: SparseIndexConfig = serde_json::from_str(empty_config).expect("");
    //     assert_eq!(
    //         empty_config,
    //         SparseIndexConfig::new(
    //             StorageType::default(),
    //             WeightType::default(),
    //             false,
    //         )
    //     );

    //     let config = "{\"storage\":\"mmap\",\"weight\":\"f32\"}";
    //     let config: SparseIndexConfig = serde_json::from_str(config).expect("");
    //     assert_eq!(
    //         config,
    //         SparseIndexConfig::new(StorageType::Mmap, WeightType::Float32, false)
    //     );
    // }

    // #[test]
    // fn test_load_and_save() {
    //     let temp_dir = tempdir().expect("Failed to create temporary directory");
    //     let index_path = temp_dir.path();

    //     let config =
    //         SparseIndexConfig::new(StorageType::Mmap, WeightType::Float32, false);

    //     config.save(index_path).expect("Failed to save config");

    //     let loaded_config = SparseIndexConfig::load(index_path).expect("Failed to load config");
    //     assert_eq!(config, loaded_config);
    // }
}
