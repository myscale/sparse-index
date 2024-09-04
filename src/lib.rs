mod api;
mod index;
mod core;
mod common;

use std::cmp::Ordering;
use ordered_float::OrderedFloat;

use crate::api::cpp::*;
use crate::ffi::ScoredPointOffset;

#[cxx::bridge(namespace = "SPARSE")]
pub mod ffi {

    #[derive(Debug, Clone)]
    pub struct FFIError {
        pub is_error: bool,
        pub message: String,
    }

    #[derive(Debug, Clone)]
    pub struct ScoredPointOffset {
        pub row_id: u32,
        pub score: f32,
    }

    #[derive(Debug, Clone)]
    pub struct FFIScoreResult {
        pub result: Vec<ScoredPointOffset>,
        pub error: FFIError,
    }
    #[derive(Debug, Clone)]
    pub struct FFIBoolResult {
        pub result: bool,
        pub error: FFIError,
    }

    #[derive(Debug, Clone)]
    pub struct FFIU64Result {
        pub result: u64,
        pub error: FFIError,
    }

    #[derive(Debug, Clone)]
    pub struct FFIVecU8Result {
        pub result: Vec<u8>,
        pub error: FFIError,
    }



    /// value_type: `0 - f32`, `1 - u8`, `2 - u32`
    #[derive(Debug, Clone)]
    pub struct TupleElement {
        pub dim_id: u32,
        pub weight_f32: f32,
        pub weight_u8: u8,
        pub weight_u32: u32,
        pub value_type: u8,
    }


    extern "Rust" {
        pub fn ffi_create_index(
            index_path: &CxxString,
        ) -> FFIBoolResult;

        pub fn ffi_create_index_with_parameter(
            index_path: &CxxString,
            index_json_parameter: &CxxString,
        ) -> FFIBoolResult;

        pub fn ffi_commit_index(
            index_path: &CxxString
        ) -> FFIBoolResult;

        pub fn ffi_insert_sparse_vector(
            index_path: &CxxString,
            row_id: u32,
            sparse_vector: &Vec<TupleElement>,
        ) -> FFIBoolResult;
    }
}

impl Eq for ScoredPointOffset {}

impl PartialEq<Self> for ScoredPointOffset {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Ord for ScoredPointOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPointOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::ffi::ScoredPointOffset;


    #[test]
    fn test_equality() {
        let a = ScoredPointOffset { row_id: 1, score: 1.0 };
        let b = ScoredPointOffset { row_id: 2, score: 1.0 };
        let c = ScoredPointOffset { row_id: 3, score: 2.0 };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_ordering() {
        let a = ScoredPointOffset { row_id: 1, score: 1.0 };
        let b = ScoredPointOffset { row_id: 2, score: 2.0 };
        assert!(a < b);
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(b.cmp(&a), Ordering::Greater);
    }

    #[test]
    fn test_partial_cmp() {
        let a = ScoredPointOffset { row_id: 1, score: 1.0 };
        let b = ScoredPointOffset { row_id: 2, score: 1.0 };
        let c = ScoredPointOffset { row_id: 3, score: 2.0 };
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));
        assert_eq!(a.partial_cmp(&c), Some(Ordering::Less));
        assert_eq!(c.partial_cmp(&a), Some(Ordering::Greater));
    }
}