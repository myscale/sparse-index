use super::{QuantizedParam, QuantizedWeight};

impl QuantizedWeight for f32 {
    fn gen_quantized_param(min_weight: Self, max_weight: Self) -> QuantizedParam {
        QuantizedParam {
            min: min_weight,
            diff256: (max_weight - min_weight) / 255.0,
        }
    }

    fn quantize_with_param(value: Self, params: QuantizedParam) -> u8 {
        ((value - params.min) / params.diff256)
            .round()
            .clamp(0.0, 255.0) as u8
    }

    fn unquantize_with_param(value: u8, params: QuantizedParam) -> Self {
        params.min + value.to_f32() * params.diff256
    }

    fn from_f32(value: f32) -> Self {
        value
    }

    fn to_f32(self) -> f32 {
        self
    }

    fn from_u8(value: u8) -> Self {
        value as f32
    }

    fn to_u8(self) -> u8 {
        if self > 255.0 {
            255
        } else if self < 0.0 {
            0
        } else {
            self as u8
        }
    }
    fn min(self, other: Self) -> Self {
        // std::cmp::min(self, other)
        if self < other {
            return self;
        } else {
            return other;
        }
    }

    fn max(self, other: Self) -> Self {
        if self < other {
            return other;
        } else {
            return self;
        }
        // std::cmp::max(self, other)
    }

    fn MINIMUM() -> Self {
        Self::NEG_INFINITY
    }

    fn weight_type() -> super::WeightType {
        super::WeightType::WeightF32
    }
}
