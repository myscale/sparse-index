use serde::{Deserialize, Serialize};

use crate::{core::{DimId, ElementType}, RowId};
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename = "index_storage_type")]
pub enum IndexStorageType {
    #[serde(rename = "memory")]
    Memory,
    #[serde(rename = "mmap")]
    Mmap,
    #[serde(rename = "compressed_mmap")]
    CompressedMmap,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename = "revision")]
pub enum Revision {
    #[serde(rename = "v1")]
    V1,
    #[serde(rename = "v2")]
    V2,
    #[serde(rename = "v3")]
    V3,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename = "version")]
pub struct Version {
    pub index_storage_type: IndexStorageType,
    pub revision: Revision,
}

impl Default for Version {
    fn default() -> Self {
        Self { index_storage_type: IndexStorageType::Memory, revision: Revision::V1 }
    }
}

impl Version {
    pub fn memory(revision: Revision) -> Self {
        Self { index_storage_type: IndexStorageType::Memory, revision }
    }
    pub fn mmap(revision: Revision) -> Self {
        Self { index_storage_type: IndexStorageType::Mmap, revision }
    }
    pub fn compressed_mmap(revision: Revision) -> Self {
        Self { index_storage_type: IndexStorageType::CompressedMmap, revision }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct InvertedIndexMeta {
    #[serde(rename = "posting_count")]
    pub posting_count: usize,

    #[serde(rename = "vector_count")]
    pub vector_count: usize,

    #[serde(rename = "min_row_id")]
    pub min_row_id: RowId,

    #[serde(rename = "max_row_id")]
    pub max_row_id: RowId,

    #[serde(rename = "min_dim_id")]
    pub min_dim_id: DimId,

    #[serde(rename = "max_dim_id")]
    pub max_dim_id: DimId,

    // #[serde(rename = "index_storage")]
    // index_storage: usize,
    #[serde(rename = "quantized")]
    pub quantized: bool,

    #[serde(rename = "element_type")]
    pub element_type: ElementType,

    #[serde(rename = "version")]
    pub version: Version,
}

impl InvertedIndexMeta {
    pub fn new(
        posting_count: usize,
        vector_count: usize,
        min_row_id: RowId,
        max_row_id: RowId,
        min_dim_id: DimId,
        max_dim_id: DimId,
        quantized: bool,
        element_type: ElementType,
        version: Version,
    ) -> Self {
        Self {
            posting_count,
            vector_count,
            min_row_id,
            max_row_id,
            min_dim_id,
            max_dim_id,
            quantized,
            element_type,
            version,
        }
    }

    /// get inverted index total postings count.
    pub fn posting_count(&self) -> usize {
        return self.posting_count;
    }

    /// how many rows(vectors) stored in inverted index.
    pub fn vector_count(&self) -> usize {
        return self.vector_count;
    }

    /// min row_id stored in inverted index.
    pub fn min_row_id(&self) -> RowId {
        return self.min_row_id;
    }

    /// max row_id stored in inverted index.
    pub fn max_row_id(&self) -> RowId {
        return self.max_row_id;
    }

    /// min dim_id stored in inverted index, it's should always be ZERO.
    pub fn min_dim_id(&self) -> RowId {
        return self.min_dim_id;
    }

    /// max dim_id stored in inverted index.
    pub fn max_dim_id(&self) -> RowId {
        return self.max_dim_id;
    }

    /// get current inverted index version.
    pub fn version(&self) -> Version {
        return self.version.clone();
    }

    pub fn element_type(&self) -> ElementType {
        return self.element_type;
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use serde_json::{from_str, to_string};

//     #[test]
//     fn test_serialize_and_deserialize() {
//         // test for `Version`
//         {
//             let version = Version::memory(Revision::V1);
//             let serialized = to_string(&version).expect("Failed to serialize");
//             assert_eq!(
//                 serialized,
//                 r#"{"index_storage_type":"memory","revision":"v1"}"#
//             );

//             let json = r#"{"revision":"v1","index_storage_type":"memory"}"#;
//             let result = from_str::<Version>(&json).expect("Failed to deserialize");
//             assert_eq!(result.index_storage_type, IndexStorageType::Memory);
//             assert_eq!(result.revision, Revision::V1);
//         }
//         // test for `InvertedIndexMeta`
//         {
//             let meta = InvertedIndexMeta::new(
//                 100,
//                 50,
//                 1,
//                 100,
//                 12,
//                 220,
//                 65536,
//                 Version::mmap(Revision::V1),
//             );
//             let serialized = to_string(&meta).expect("Failed to serialize");
//             assert_eq!(
//                 serialized,
//                 r#"{"posting_count":100,"vector_count":50,"min_row_id":1,"max_row_id":100,"min_dim_id":12,"max_dim_id":220,"index_storage":65536,"version":{"index_storage_type":"mmap","revision":"v1"}}"#
//             );

//             let json = r#"{"posting_count":100,"min_row_id":0,"vector_count":101,"max_row_id":100,"min_dim_id":12,"max_dim_id":220,"index_storage":65536,"version":{"index_storage_type":"mmap","revision":"v2"}}"#;
//             let result = from_str::<InvertedIndexMeta>(&json).expect("Failed to deserialize");
//             assert_eq!(
//                 result,
//                 InvertedIndexMeta::new(
//                     100,
//                     101,
//                     0,
//                     100,
//                     12,
//                     220,
//                     65536,
//                     Version::mmap(Revision::V2)
//                 )
//             );
//         }
//     }

//     #[test]
//     fn test_missing_and_extra() {
//         // test for `Version`
//         {
//             let json =
//                 r#"{"index_storage_type":"memory","revision":"v1","extra_field":"extra_value"}"#;
//             let result = from_str::<Version>(&json);
//             assert!(
//                 result.is_err(),
//                 "Deserialization should fail due to extra fields"
//             );

//             let json = r#"{"revision":"v1"}"#; // Missing `index_storage_type`
//             let result = from_str::<Version>(&json);
//             assert!(
//                 result.is_err(),
//                 "Deserialization should fail due to missing fields"
//             );
//         }
//         // test for `InvertedIndexMeta`
//         {
//             let json = r#"{"extra":20, "posting_count":100,"vector_count":50,"min_row_id":1,"max_row_id":100,"min_dim_id":12,"max_dim_id":220,"index_storage":65536,"version":{"index_storage_type":"mmap","revision":"v1"}}"#;
//             let result = from_str::<InvertedIndexMeta>(&json);
//             assert!(result.is_ok());

//             let json = r#"{"posting_count":100,"vector_count":50,"max_row_id":100,"min_dim_id":12,"max_dim_id":220,"index_storage":65536,"version":{"index_storage_type":"mmap","revision":"v1"}}"#; // Missing `min_row_id`
//             let result = from_str::<InvertedIndexMeta>(&json);
//             assert!(
//                 result.is_err(),
//                 "Deserialization should fail due to missing fields"
//             );
//         }
//     }
// }
