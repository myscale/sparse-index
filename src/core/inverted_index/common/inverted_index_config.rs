use crate::core::*;
use serde::{Deserialize, Serialize};

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
pub struct InvertedIndexConfig {
    #[serde(default)]
    #[serde(rename = "storage")]
    pub storage_type: StorageType,

    #[serde(default)]
    #[serde(rename = "weight")]
    pub weight_type: IndexWeightType,

    #[serde(default)]
    #[serde(rename = "element_type")]
    pub element_type: ElementType,

    #[serde(default)]
    #[serde(rename = "quantized")]
    pub quantized: bool,
}

impl InvertedIndexConfig {
    pub fn new(
        storage_type: StorageType,
        weight_type: IndexWeightType,
        element_type: ElementType,
        enable_quantized: bool,
    ) -> Result<Self, InvertedIndexError> {
        let config = InvertedIndexConfig {
            storage_type,
            weight_type,
            quantized: enable_quantized,
            element_type,
        };
        let _check_valid = config.is_valid()?;
        return Ok(config);
    }

    pub fn is_valid(&self) -> Result<bool, InvertedIndexError> {
        if self.quantized && self.element_type == ElementType::EXTENDED {
            return Err(InvertedIndexError::InvalidIndexConfig(
                "When quantized is enabled, element type can only be `SIMPLE`.".to_string(),
            ));
        }
        if self.weight_type == IndexWeightType::UInt8 && self.quantized {
            return Err(InvertedIndexError::InvalidIndexConfig(
                "When IndexWeightType is u8, you can't quantize it.".to_string(),
            ));
        }
        Ok(true)
    }

    pub fn element_type(&self) -> ElementType {
        match (self.weight_type, self.quantized) {
            (IndexWeightType::Float32, true) => ElementType::SIMPLE,
            (IndexWeightType::Float32, false) => self.element_type,
            (IndexWeightType::Float16, true) => ElementType::SIMPLE,
            (IndexWeightType::Float16, false) => self.element_type,
            (IndexWeightType::UInt8, true) => ElementType::SIMPLE,
            (IndexWeightType::UInt8, false) => self.element_type,
        }
    }
}
