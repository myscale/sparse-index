use super::{QuantizedParam, QuantizedWeight};

#[derive(PartialEq, Default, Copy, Clone, Debug)]
pub struct QuantizedF16Param {
    min: half::f16,
    diff256: half::f16,
}

impl QuantizedWeight for half::f16 {
    fn gen_quantized_param(min_weight: half::f16, max_weight: half::f16) -> QuantizedParam {
        QuantizedParam {
            min: min_weight.to_f32(),
            diff256: (max_weight - min_weight).to_f32() / 255.0,
        }
    }

    fn quantize_with_param(value: Self, params: QuantizedParam) -> u8 {
        ((value.to_f32() - params.min) / params.diff256).round().clamp(0.0, 255.0) as u8
    }

    fn unquantize_with_param(value: u8, params: QuantizedParam) -> Self {
        half::f16::from_f32(params.min + value.to_f32() * params.diff256)
    }

    fn from_f32(value: f32) -> Self {
        half::f16::from_f32(value)
    }

    fn to_f32(self) -> f32 {
        self.to_f32()
    }
    fn from_u8(value: u8) -> Self {
        half::f16::from_f32(value as f32)
    }

    fn to_u8(self) -> u8 {
        let val = half::f16::to_f32(self);
        if val > 255.0 {
            255
        } else if val < 0.0 {
            0
        } else {
            val as u8
        }
    }

    fn min(self, other: Self) -> Self {
        if self < other {
            return self;
        } else {
            return other;
        }
        // std::cmp::min(self, other)
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
        super::WeightType::WeightF16
    }
}
