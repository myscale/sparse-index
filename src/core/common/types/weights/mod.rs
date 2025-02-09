mod weight_f16;
mod weight_f32;
mod weight_u8;

#[allow(unused_imports)]
pub use weight_f16::*;
#[allow(unused_imports)]
pub use weight_f32::*;
#[allow(unused_imports)]
pub use weight_u8::*;

use core::f32;
use std::fmt::Debug;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum WeightType {
    #[default]
    WeightF32,
    WeightF16,
    WeightU8,
}

#[derive(Copy, Clone, Debug)]
pub struct QuantizedParam {
    min: f32,
    diff256: f32,
}

impl PartialEq for QuantizedParam {
    fn eq(&self, other: &Self) -> bool {
        self.min == other.min && self.diff256 == other.diff256
    }
}

impl Eq for QuantizedParam {}

impl QuantizedParam {
    pub fn from_minmax(min: f32, max: f32) -> Self {
        Self { min, diff256: (max - min) / 255.0 }
    }

    pub fn min_precision(&self) -> f32 {
        self.diff256
    }

    pub fn approximately_eq<W: QuantizedWeight>(&self, left: W, right: W) -> bool {
        let u8_right = W::to_u8(right);
        let u8_left = W::to_u8(left);

        if u8_right.abs_diff(u8_left) > 1 {
            return false;
        }

        let f32_right = f32::unquantize_with_param(u8_right, self.clone());
        let f32_left = f32::unquantize_with_param(u8_left, self.clone());

        // In certain cases, even if the difference between `u8` values equals 1, the `f32` unquantized value may exceed one step.
        // We introduced a 0.5% precision loss to ensure accurate comparisons.
        (f32_right - f32_left) <= (self.min_precision() * 1.005)
    }
}

impl Default for QuantizedParam {
    fn default() -> Self {
        Self { min: 0.0, diff256: (0.0 - 0.0) / 255.0 }
    }
}

pub trait QuantizedWeight: Clone + Copy + Debug + PartialEq + PartialOrd + 'static {
    /// Return current [`Weight`] minimum value.
    #[allow(non_snake_case)]
    fn MINIMUM() -> Self;

    /// Return current [`Weight`] type.
    fn weight_type() -> WeightType;

    /// Get quantized params for current [`Weight`]
    fn gen_quantized_param(min_weight: Self, max_weight: Self) -> QuantizedParam;

    /// Convert from f32 to current [`Weight`]
    fn quantize_with_param(value: Self, params: QuantizedParam) -> u8;
    fn unquantize_with_param(value: u8, params: QuantizedParam) -> Self;

    fn from_f32(value: f32) -> Self;
    fn to_f32(self) -> f32;

    fn from_u8(value: u8) -> Self;
    fn to_u8(self) -> u8;

    /// Compare with the other [`Weight`], return the smaller one.
    fn min(self, other: Self) -> Self;
    /// Compare with the other [`Weight`], return the bigger one.
    fn max(self, other: Self) -> Self;
}

// pub trait Weight: Clone + Copy + Debug + PartialEq + PartialOrd + 'static {
//     type QuantizationParams: Clone + Copy + PartialEq + Debug;

//     /// Return current [`Weight`] minimum value.
//     #[allow(non_snake_case)]
//     fn MINIMUM() -> Self;

//     /// Return current [`Weight`] type.
//     fn weight_type() -> WeightType;

//     /// Get quantization params for current [`Weight`]
//     fn quantization_params_for(
//         values: impl ExactSizeIterator<Item = DimWeight> + Clone,
//     ) -> Self::QuantizationParams;

//     /// Get quantization params for current [`Weight`]
//     fn quantization_with_minmax(
//         min_weight: DimWeight, max_weight: DimWeight
//     ) -> Self::QuantizationParams;

//     /// Get default quantization params for current [`Weight`]
//     /// For `f32`, `f16` and `u8`, it will be `()`
//     /// For `QuantizedU8`, it will be [`DEFAULT_U8_QUANTIZED_PARAMS`]
//     fn default_quant_params() -> Self::QuantizationParams;

//     /// Convert from f32 to current [`Weight`]
//     fn from_f32(params: Option<Self::QuantizationParams>, value: f32) -> Self;

//     /// Convert from current [`Weight`] to f32
//     fn to_f32(self, params: Option<Self::QuantizationParams>) -> f32;

//     /// Convert a slice of [`Weight`] values into f32 slice.
//     fn into_f32_slice<'a>(
//         params: Option<Self::QuantizationParams>,
//         weights: &'a [Self],
//         buffer: &'a mut [f32],
//     ) -> &'a [f32];

//     /// Compare with the other [`Weight`], return the smaller one.
//     fn min(self, other: Self) -> Self;

//     /// Compare with the other [`Weight`], return the bigger one.
//     fn max(self, other: Self) -> Self;
// }
