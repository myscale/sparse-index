use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::path::Path;
use std::result;

use atomicwrites::{AtomicFile, OverwriteBehavior};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

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
    // Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}

pub fn read_bin<T: DeserializeOwned>(path: &Path) -> Result<T, FileOperationError> {
    Ok(bincode::deserialize_from(BufReader::new(File::open(
        path,
    )?))?)
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
    New(String),
}

use std::io::{Error as IoError, ErrorKind};

impl From<FileOperationError> for IoError {
    fn from(error: FileOperationError) -> Self {
        match error {
            FileOperationError::IoError(e) => e,
            FileOperationError::BinCodeError(e) => IoError::new(ErrorKind::InvalidData, e),
            FileOperationError::SerdeJsonError(e) => IoError::new(ErrorKind::InvalidData, e),
            _ => IoError::new(ErrorKind::Other, "Unknown error occurred")
        }
    }
}

impl<'de> Deserialize<'de> for FileOperationError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(FileOperationError::New(s))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use crate::index::sparse_index_config::SparseIndexConfig;

    #[test]
    fn test_deserialize_file_operation_error() {
        // let json_data = "\"Some error occurred\"";
        let json_data = "\"{}567\"";
        let res:FileOperationError = serde_json::from_str(json_data).unwrap();
        println!("{:?}", res);
        // match error {
        //     FileOperationError::New(msg) => assert_eq!(msg, "Some error occurred"),
        //     _ => panic!("Unexpected error type"),
        // }
    }
}