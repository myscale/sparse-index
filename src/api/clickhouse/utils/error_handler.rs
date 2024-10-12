use crate::ffi::*;

pub trait FFIResult<T> {
    fn from_error(error_message: String) -> Self;
}

impl FFIResult<bool> for FFIBoolResult {
    fn from_error(error_message: String) -> Self {
        FFIBoolResult {
            result: false,
            error: FFIError {
                is_error: true,
                message: error_message,
            },
        }
    }
}

impl FFIResult<Vec<u8>> for FFIVecU8Result {
    fn from_error(error_message: String) -> Self {
        FFIVecU8Result {
            result: Vec::new(),
            error: FFIError {
                is_error: true,
                message: error_message,
            },
        }
    }
}

impl FFIResult<Vec<u8>> for FFIU64Result {
    fn from_error(error_message: String) -> Self {
        FFIU64Result {
            result: 0u64,
            error: FFIError {
                is_error: true,
                message: error_message,
            },
        }
    }
}

impl FFIResult<Vec<ScoredPointOffset>> for FFIScoreResult {
    fn from_error(error_message: String) -> Self {
        FFIScoreResult {
            result: vec![],
            error: FFIError {
                is_error: true,
                message: error_message,
            },
        }
    }
}

pub struct ApiUtils;

impl ApiUtils {
    pub fn handle_error<T, E>(func_name: &str, prefix: &str, error: String) -> T
    where
        T: FFIResult<E>,
    {
        let error_message = format!("{}, {}", prefix, error);
        T::from_error(error_message)
    }

    
}
