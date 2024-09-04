use super::utils::*;
use crate::core::common::types::{DimId, DimOffset, DimWeight, ScoreType};
use validator::{Validate, ValidationErrors};
use crate::ffi::TupleElement;

/// Same as `SparseVector` but with `DimOffset` indices.
/// Meaning that is uses internal segment-specific indices. // 表示内部 segment 特定的索引？
#[derive(Debug, PartialEq, Clone, Default)]
pub struct RemappedSparseVector {
    /// indices must be unique
    pub indices: Vec<DimOffset>,
    /// values and indices must be the same length
    pub values: Vec<DimWeight>,
}

impl RemappedSparseVector {
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> Result<Self, ValidationErrors> {
        let vector = Self { indices, values };
        vector.validate()?;
        Ok(vector)
    }

    pub fn sort_by_indices(&mut self) {
        double_sort(&mut self.indices, &mut self.values);
    }

    /// Check if this vector is sorted by indices.
    pub fn is_sorted(&self) -> bool {
        self.indices.windows(2).all(|w| w[0] < w[1])
    }

    /// Score this vector against another vector using dot product.
    /// Warning: Expects both vectors to be sorted by indices.
    ///
    /// Return None if the vectors do not overlap.
    pub fn score(&self, other: &RemappedSparseVector) -> Option<ScoreType> {
        // TODO 需要避免运行时的 Panic 操作
        debug_assert!(self.is_sorted());
        debug_assert!(other.is_sorted());
        score_vectors(&self.indices, &self.values, &other.indices, &other.values)
    }
}

impl TryFrom<Vec<(u32, f32)>> for RemappedSparseVector {
    type Error = ValidationErrors;

    fn try_from(tuples: Vec<(u32, f32)>) -> Result<Self, Self::Error> {
        let (indices, values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        RemappedSparseVector::new(indices, values)
    }
}

impl TryFrom<Vec<TupleElement>> for RemappedSparseVector {
    type Error = ValidationErrors;

    fn try_from(tuples: Vec<TupleElement>) -> Result<Self, Self::Error> {
        let mut indices = Vec::new();
        let mut values = Vec::new();

        for element in tuples {
            let weight = match element.value_type {
                0 => element.weight_f32,      // f32 直接使用
                1 => element.weight_u8 as f32, // u8 转换为 f32
                2 => element.weight_u32 as f32, // u32 转换为 f32
                _ => 0.0f32,
            };
            indices.push(element.dim_id);
            values.push(weight);
        }

        RemappedSparseVector::new(indices, values)
    }
}

impl<const N: usize> From<[(u32, f32); N]> for RemappedSparseVector {
    fn from(value: [(u32, f32); N]) -> Self {
        value.to_vec().try_into().unwrap()
    }
}

impl<const N: usize> From<[TupleElement; N]> for RemappedSparseVector {
    fn from(value: [TupleElement; N]) -> Self {
        let mut indices = Vec::with_capacity(N);
        let mut values = Vec::with_capacity(N);

        for element in value {
            let weight = match element.value_type {
                0 => element.weight_f32,      // f32 直接使用
                1 => element.weight_u8 as f32, // u8 转换为 f32
                2 => element.weight_u32 as f32, // u32 转换为 f32
                _ => 0.0f32,

            };
            indices.push(element.dim_id);
            values.push(weight);
        }

        RemappedSparseVector {
            indices,
            values,
        }
    }
}

impl Validate for RemappedSparseVector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_sparse_vector_impl(&self.indices, &self.values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_aligned_same_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(14.0));
    }

    #[test]
    fn test_score_not_aligned_same_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![2, 3, 4], vec![2.0, 3.0, 4.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(13.0));
    }

    #[test]
    fn test_score_aligned_different_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![1, 2, 3, 4], vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(14.0));
    }

    #[test]
    fn test_score_not_aligned_different_size() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![2, 3, 4, 5], vec![2.0, 3.0, 4.0, 5.0]).unwrap();
        assert_eq!(v1.score(&v2), Some(13.0));
    }

    #[test]
    fn test_score_no_overlap() {
        let v1 = RemappedSparseVector::new(vec![1, 2, 3], vec![1.0, 2.0, 3.0]).unwrap();
        let v2 = RemappedSparseVector::new(vec![4, 5, 6], vec![2.0, 3.0, 4.0]).unwrap();
        assert!(v1.score(&v2).is_none());
    }
}
