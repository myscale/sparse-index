use std::borrow::Cow;

use super::utils::*;
use crate::core::common::types::{DimId, DimWeight, ScoreType};
use crate::ffi::TupleElement;
use crate::RowId;
use validator::{Validate, ValidationErrors};

/// Sparse vector structure
#[derive(Debug, Clone, Default)]
// #[serde(rename_all = "snake_case")]
pub struct SparseVector {
    pub indices: Vec<DimId>,
    pub values: Vec<DimWeight>,
}

impl Eq for SparseVector {}

impl PartialEq for SparseVector {
    fn eq(&self, other: &Self) -> bool {
        self.indices == other.indices
    }
}

impl SparseVector {
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> Result<Self, ValidationErrors> {
        let vector = SparseVector { indices, values };
        vector.validate()?;
        Ok(vector)
    }

    /// Sort this vector by indices.
    ///
    /// Sorting is required for scoring and overlap checks.
    pub fn sort_by_indices(&mut self) {
        double_sort(&mut self.indices, &mut self.values);
    }

    /// Check if this vector is sorted by indices.
    pub fn is_sorted(&self) -> bool {
        self.indices.windows(2).all(|w| w[0] < w[1])
    }

    /// Check if this vector is empty.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty() && self.values.is_empty()
    }

    /// Score this vector against another vector using dot product.
    /// Warning: Expects both vectors to be sorted by indices.
    ///
    /// Return None if the vectors do not overlap.
    pub fn score(&self, other: &SparseVector) -> Option<ScoreType> {
        // TODO: Avoid Panic in run time.
        debug_assert!(self.is_sorted());
        debug_assert!(other.is_sorted());
        score_vectors(&self.indices, &self.values, &other.indices, &other.values)
    }

    /// Construct a new vector that is the result of performing all indices-wise operations.
    /// Automatically sort input vectors if necessary.
    pub fn combine_aggregate(&self, other: &SparseVector, op: impl Fn(DimWeight, DimWeight) -> DimWeight) -> Self {
        // Copy and sort `self` vector if not already sorted
        let this: Cow<SparseVector> = if !self.is_sorted() {
            let mut this = self.clone();
            this.sort_by_indices();
            Cow::Owned(this)
        } else {
            Cow::Borrowed(self)
        };
        // TODO: refine
        assert!(this.is_sorted());

        // Copy and sort `other` vector if not already sorted
        let cow_other: Cow<SparseVector> = if !other.is_sorted() {
            let mut other = other.clone();
            other.sort_by_indices();
            Cow::Owned(other)
        } else {
            Cow::Borrowed(other)
        };
        let other = &cow_other;
        // TODO: refine
        assert!(other.is_sorted());

        let mut result = SparseVector::default();
        let mut i = 0;
        let mut j = 0;
        while i < this.indices.len() && j < other.indices.len() {
            match this.indices[i].cmp(&other.indices[j]) {
                std::cmp::Ordering::Less => {
                    result.indices.push(this.indices[i]);
                    result.values.push(op(this.values[i], 0.0));
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    result.indices.push(other.indices[j]);
                    result.values.push(op(0.0, other.values[j]));
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    result.indices.push(this.indices[i]);
                    result.values.push(op(this.values[i], other.values[j]));
                    i += 1;
                    j += 1;
                }
            }
        }
        while i < this.indices.len() {
            result.indices.push(this.indices[i]);
            result.values.push(op(this.values[i], 0.0));
            i += 1;
        }
        while j < other.indices.len() {
            result.indices.push(other.indices[j]);
            result.values.push(op(0.0, other.values[j]));
            j += 1;
        }
        // TODO 避免运行时 Panic
        debug_assert!(result.is_sorted());
        debug_assert!(result.validate().is_ok());
        result
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct SparseRowContent {
    pub row_id: RowId,

    pub sparse_vector: SparseVector,
}

impl SparseRowContent {
    fn new(row_id: RowId, sparse_vector: SparseVector) -> Self {
        Self { row_id, sparse_vector }
    }
}

impl TryFrom<Vec<(u32, f32)>> for SparseVector {
    type Error = ValidationErrors;

    fn try_from(tuples: Vec<(u32, f32)>) -> Result<Self, Self::Error> {
        let (indices, values): (Vec<_>, Vec<_>) = tuples.into_iter().unzip();
        SparseVector::new(indices, values)
    }
}

impl Validate for SparseVector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_sparse_vector_impl(&self.indices, &self.values)
    }
}

impl TryFrom<Vec<TupleElement>> for SparseVector {
    type Error = ValidationErrors;

    fn try_from(tuples: Vec<TupleElement>) -> Result<Self, Self::Error> {
        let mut indices = Vec::new();
        let mut values = Vec::new();

        for element in tuples {
            let weight = match element.value_type {
                0 => element.weight_f32,
                1 => element.weight_u8 as f32,
                2 => element.weight_u32 as f32,
                _ => 0.0f32,
            };
            indices.push(element.dim_id);
            values.push(weight);
        }

        SparseVector::new(indices, values)
    }
}

impl<const N: usize> From<[(u32, f32); N]> for SparseVector {
    fn from(value: [(u32, f32); N]) -> Self {
        value.to_vec().try_into().unwrap()
    }
}

impl<const N: usize> From<[TupleElement; N]> for SparseVector {
    fn from(value: [TupleElement; N]) -> Self {
        let mut indices = Vec::with_capacity(N);
        let mut values = Vec::with_capacity(N);

        for element in value {
            let weight = match element.value_type {
                0 => element.weight_f32,
                1 => element.weight_u8 as f32,
                2 => element.weight_u32 as f32,
                _ => 0.0f32,
            };
            indices.push(element.dim_id);
            values.push(weight);
        }

        SparseVector { indices, values }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_test() {
        let fully_empty = SparseVector::new(vec![], vec![]);
        assert!(fully_empty.is_ok());
        assert!(fully_empty.unwrap().is_empty());

        let different_length = SparseVector::new(vec![1, 2, 3], vec![1.0, 2.0]);
        assert!(different_length.is_err());

        let not_sorted = SparseVector::new(vec![1, 3, 2], vec![1.0, 2.0, 3.0]);
        assert!(not_sorted.is_ok());

        let not_unique = SparseVector::new(vec![1, 2, 3, 2], vec![1.0, 2.0, 3.0, 4.0]);
        assert!(not_unique.is_err());
    }

    #[test]
    fn sorting_test() {
        let mut not_sorted = SparseVector::new(vec![1, 3, 2], vec![1.0, 2.0, 3.0]).unwrap();
        assert!(!not_sorted.is_sorted());
        not_sorted.sort_by_indices();
        assert!(not_sorted.is_sorted());
    }

    #[test]
    fn combine_aggregate_test() {
        // Test with missing core
        let a = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2, 0.3]).unwrap();
        let b = SparseVector::new(vec![2, 3, 4], vec![2.0, 3.0, 4.0]).unwrap();
        let sum = a.combine_aggregate(&b, |x, y| x + 2.0 * y);
        assert_eq!(sum.indices, vec![1, 2, 3, 4]);
        assert_eq!(sum.values, vec![0.1, 4.2, 6.3, 8.0]);

        // reverse arguments
        let sum = b.combine_aggregate(&a, |x, y| x + 2.0 * y);
        assert_eq!(sum.indices, vec![1, 2, 3, 4]);
        assert_eq!(sum.values, vec![0.2, 2.4, 3.6, 4.0]);

        // Test with non-sorted input
        let a = SparseVector::new(vec![1, 2, 3], vec![0.1, 0.2, 0.3]).unwrap();
        let b = SparseVector::new(vec![4, 2, 3], vec![4.0, 2.0, 3.0]).unwrap();
        let sum = a.combine_aggregate(&b, |x, y| x + 2.0 * y);
        assert_eq!(sum.indices, vec![1, 2, 3, 4]);
        assert_eq!(sum.values, vec![0.1, 4.2, 6.3, 8.0]);
    }
}
