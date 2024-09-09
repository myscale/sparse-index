use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::path::Path;

use atomicwrites::{AtomicFile, OverwriteBehavior};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

#[allow(dead_code)]
pub fn atomic_save_bin<T: Serialize>(path: &Path, object: &T) -> Result<(), FileOperationError> {
    let af = AtomicFile::new(path, OverwriteBehavior::AllowOverwrite);
    af.write(|f| bincode::serialize_into(BufWriter::new(f), object))?;
    Ok(())
}

pub fn atomic_save_json<T: Serialize>(path: &Path, object: &T) -> Result<(), FileOperationError> {
    let af = AtomicFile::new(path, OverwriteBehavior::AllowOverwrite);
    af.write(|f| serde_json::to_writer(BufWriter::new(f), object))?;
    Ok(())
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, FileOperationError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let data = serde_json::from_reader(reader)?;
    Ok(data)
}

#[allow(dead_code)]
pub fn read_bin<T: DeserializeOwned>(path: &Path) -> Result<T, FileOperationError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let data = bincode::deserialize_from(reader)?;
    Ok(data)
}

#[derive(Debug, Error)]
pub enum FileOperationError {
    #[error(transparent)]
    IoError(#[from] io::Error),

    #[error(transparent)]
    BinCodeError(#[from] bincode::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    AtomicWriteError(#[from] atomicwrites::Error<io::Error>),

    #[error(transparent)]
    AtomicWriteBinCodeError(#[from] atomicwrites::Error<bincode::Error>),

    #[error(transparent)]
    AtomicWriteSerdeJsonError(#[from] atomicwrites::Error<serde_json::Error>),

    #[error("'{0}'")]
    FileOperationError(String),
}

use std::io::{Error as IoError, ErrorKind};

impl From<FileOperationError> for IoError {
    fn from(error: FileOperationError) -> Self {
        match error {
            FileOperationError::IoError(e) => e,
            _ => IoError::new(ErrorKind::Other, error.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for FileOperationError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(FileOperationError::FileOperationError(s))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestData {
        name: String,
        age: u32,
    }

    #[test]
    fn test_atomic_save_and_read_json() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.json");

        let data = TestData {
            name: "John".to_string(),
            age: 30,
        };

        // Test atomic_save_json
        atomic_save_json(&file_path, &data).unwrap();
        assert!(file_path.exists());

        // Test read_json
        let loaded_data: TestData = read_json(&file_path).unwrap();
        assert_eq!(data, loaded_data);
    }

    #[test]
    fn test_atomic_save_and_read_bin() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.bin");

        let data = TestData {
            name: "Alice".to_string(),
            age: 25,
        };

        // Test atomic_save_bin
        atomic_save_bin(&file_path, &data).unwrap();
        assert!(file_path.exists());

        // Test read_bin
        let loaded_data: TestData = read_bin(&file_path).unwrap();
        assert_eq!(data, loaded_data);
    }

    #[test]
    fn test_file_operation_error() {
        let non_existent_path = Path::new("non_existent_file.json");
        
        // Test read_json with non-existent file
        let result = read_json::<TestData>(non_existent_path);
        assert!(result.is_err());
        
        // Test read_bin with non-existent file
        let result = read_bin::<TestData>(non_existent_path);
        assert!(result.is_err());
    }
}