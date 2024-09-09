use half::slice::HalfFloatSliceExt;
use itertools::{Itertools, MinMaxResult};
use std::fmt::Debug;
use super::DimWeight;


pub trait Weight: PartialEq + Copy + Debug + 'static {
    type QuantizationParams: Copy + PartialEq + Debug;

    fn quantization_params_for(
        values: impl ExactSizeIterator<Item = DimWeight> + Clone,
    ) -> Self::QuantizationParams;

    /// 使用指定的量化参数将浮点数 f32 转换为具体实现类型
    fn from_f32(params: Self::QuantizationParams, value: f32) -> Self;

    /// 使用量化参数将实现类型转换为浮点数
    fn to_f32(self, params: Self::QuantizationParams) -> f32;

    /// 接受实现类型的切片, 将它转换为浮点数切片, 结果存储在提供的缓冲区 buffer 内部
    fn into_f32_slice<'a>(
        params: Self::QuantizationParams,
        weights: &'a [Self],
        buffer: &'a mut [f32],
    ) -> &'a [f32];
}

impl Weight for f32 {
    type QuantizationParams = ();

    fn quantization_params_for(
        _values: impl ExactSizeIterator<Item = DimWeight> + Clone,
    ) -> Self::QuantizationParams {
    }

    fn from_f32(_params: Self::QuantizationParams, value: f32) -> Self {
        value
    }

    fn to_f32(self, _params: Self::QuantizationParams) -> f32 {
        self
    }

    fn into_f32_slice<'a>(
        _params: Self::QuantizationParams,
        weights: &'a [Self],
        _buffer: &'a mut [f32],
    ) -> &'a [f32] {
        weights
    }
}

impl Weight for half::f16 {
    type QuantizationParams = ();

    fn quantization_params_for(
        _values: impl ExactSizeIterator<Item = DimWeight> + Clone,
    ) -> Self::QuantizationParams {
    }

    fn from_f32(_params: Self::QuantizationParams, value: f32) -> Self {
        half::f16::from_f32(value)
    }

    fn to_f32(self, _params: Self::QuantizationParams) -> f32 {
        self.to_f32()
    }

    // TODO
    fn into_f32_slice<'a>(
        _params: Self::QuantizationParams,
        weights: &'a [Self],
        buffer: &'a mut [f32],
    ) -> &'a [f32] {
        weights.convert_to_f32_slice(buffer);
        buffer
    }
}

impl Weight for u8 {
    type QuantizationParams = ();

    fn quantization_params_for(
        _values: impl ExactSizeIterator<Item = DimWeight> + Clone,
    ) -> Self::QuantizationParams {
    }

    fn from_f32(_params: Self::QuantizationParams, value: f32) -> Self {
        value as u8
    }

    fn to_f32(self, _params: Self::QuantizationParams) -> f32 {
        self as f32
    }

    fn into_f32_slice<'a>(
        _params: Self::QuantizationParams,
        weights: &'a [Self],
        buffer: &'a mut [f32],
    ) -> &'a [f32] {
        for (idx, &weight) in weights.iter().enumerate() {
            buffer[idx] = weight as f32;
        }
        buffer
    }
}

#[derive(PartialEq, Copy, Clone, Debug)]
pub struct QuantizedU8(u8);

impl From<QuantizedU8> for DimWeight {
    fn from(value: QuantizedU8) -> Self {
        value.0 as DimWeight
    }
}

#[derive(PartialEq, Default, Copy, Clone, Debug)]
pub struct QuantizedU8Params {
    min: DimWeight,
    diff256: DimWeight,
}

impl Weight for QuantizedU8 {
    type QuantizationParams = QuantizedU8Params;

    fn quantization_params_for(
        values: impl ExactSizeIterator<Item = DimWeight> + Clone,
    ) -> Self::QuantizationParams {
        let (min, max) = match values.minmax() {
            MinMaxResult::NoElements => return QuantizedU8Params::default(),
            MinMaxResult::OneElement(e) => (e, e),
            MinMaxResult::MinMax(min, max) => (min, max),
        };
        QuantizedU8Params {
            min,
            diff256: (max - min) / 255.0,
        }
    }

    fn from_f32(params: Self::QuantizationParams, value: f32) -> Self {
        QuantizedU8(
            ((value - params.min) / params.diff256) // 当前 value 与起点的距离, 共计多少个 diff256
                .round() // 四舍五入
                .clamp(0.0, 255.0) as u8, // clamp 进一步限制最终结果范围, 避免 u8 溢出
        )
    }

    fn to_f32(self, params: Self::QuantizationParams) -> f32 {
        params.min + self.0 as f32 * params.diff256
    }

    fn into_f32_slice<'a>(
        params: Self::QuantizationParams,
        weights: &'a [Self],
        buffer: &'a mut [f32],
    ) -> &'a [f32] {
        if weights.len() != buffer.len() {
            panic!("weights length isn't equal with buffer");
        }
        for (i, &weight) in weights.iter().enumerate() {
            buffer[i] = weight.to_f32(params);
        }
        buffer
    }
}
