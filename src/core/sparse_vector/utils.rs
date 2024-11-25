use crate::core::common::types::{DimWeight, ScoreType};
use crate::core::sparse_vector::sparse_vector::SparseVector;
use itertools::Itertools;
use rand::Rng;
use std::hash::Hash;
use std::ops::Range;
use validator::{ValidationError, ValidationErrors};

const VALUE_RANGE: Range<f64> = -100.0..100.0;
// Realistic sizing based on experiences with SPLADE
const MAX_VALUES_PER_VECTOR: usize = 300;

/// Sort two arrays by the first array.
/// 根据 indices 数组对 values 数组进行排序
pub(super) fn double_sort<T: Ord + Copy, V: Copy>(indices: &mut [T], values: &mut [V]) {
    // Check if the indices are already sorted
    if indices.windows(2).all(|w| w[0] < w[1]) {
        return;
    }

    let mut indexed_values: Vec<(T, V)> =
        indices.iter().zip(values.iter()).map(|(&i, &v)| (i, v)).collect();

    // Sort the vector of tuples by indices
    indexed_values.sort_unstable_by_key(|&(i, _)| i);

    for (i, (index, value)) in indexed_values.into_iter().enumerate() {
        indices[i] = index;
        values[i] = value;
    }
}

/// 计算两个稀疏向量之间的点积
pub(super) fn score_vectors<T: Ord + Eq>(
    self_indices: &[T],
    self_values: &[DimWeight],
    other_indices: &[T],
    other_values: &[DimWeight],
) -> Option<ScoreType> {
    let mut score = 0.0;
    // track whether there is any overlap
    let mut overlap = false;
    let mut i = 0;
    let mut j = 0;
    while i < self_indices.len() && j < other_indices.len() {
        match self_indices[i].cmp(&other_indices[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                overlap = true;
                score += self_values[i] * other_values[j];
                i += 1;
                j += 1;
            }
        }
    }
    if overlap {
        Some(score)
    } else {
        None // 两个向量没有发生重叠
    }
}

pub(super) fn validate_sparse_vector_impl<T: Clone + Eq + Hash>(
    indices: &[T],
    values: &[DimWeight],
) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrors::default();

    if indices.len() != values.len() {
        errors.add("values", ValidationError::new("must be the same length as indices"));
    }
    if indices.iter().unique().count() != indices.len() {
        errors.add("indices", ValidationError::new("must be unique"));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Generates a random and not empty sparse vector
/// 生成一个非空的随机稀疏向量
pub fn random_sparse_vector<R: Rng + ?Sized>(rnd_gen: &mut R, max_dim_size: usize) -> SparseVector {
    let size = rnd_gen.gen_range(1..max_dim_size);
    let mut tuples: Vec<(u32, f32)> = vec![];

    for i in 1..=size {
        // make sure the vector is not too large (for performance reasons)
        if tuples.len() == MAX_VALUES_PER_VECTOR {
            break;
        }
        // high probability of skipping a dimension to make the vectors more sparse
        let skip = rnd_gen.gen_bool(0.98);
        if !skip {
            tuples.push((i as u32, rnd_gen.gen_range(VALUE_RANGE) as f32));
        }
    }

    // make sure we have at least one vector
    if tuples.is_empty() {
        tuples.push((
            rnd_gen.gen_range(1..max_dim_size) as u32,
            rnd_gen.gen_range(VALUE_RANGE) as f32,
        ));
    }

    SparseVector::try_from(tuples).unwrap()
}

/// Generates a sparse vector with all dimensions filled
/// 生成一个满的稀疏向量
pub fn random_full_sparse_vector<R: Rng + ?Sized>(
    rnd_gen: &mut R,
    max_size: usize,
) -> SparseVector {
    let mut tuples: Vec<(u32, f32)> = Vec::with_capacity(max_size);

    for i in 1..=max_size {
        tuples.push((i as u32, rnd_gen.gen_range(VALUE_RANGE) as f32));
    }

    SparseVector::try_from(tuples).unwrap()
}

/// Generates a sparse vector with only positive values
/// 生成一个只包含正值的随机稀疏向量
pub fn random_positive_sparse_vector<R: Rng + ?Sized>(
    rnd_gen: &mut R,
    max_dim_size: usize,
) -> SparseVector {
    let mut vec = random_sparse_vector(rnd_gen, max_dim_size);
    for value in vec.values.iter_mut() {
        *value = value.abs();
    }
    vec
}
