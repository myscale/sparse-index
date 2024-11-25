use super::{QuantizedParam, QuantizedWeight};

impl QuantizedWeight for u8 {
    fn gen_quantized_param(_min_weight: Self, _max_weight: Self) -> QuantizedParam {
        panic!("u8 can't be quantized")
    }

    fn quantize_with_param(value: Self, _params: QuantizedParam) -> u8 {
        value
    }

    fn unquantize_with_param(value: u8, _params: QuantizedParam) -> Self {
        value
    }

    fn from_f32(value: f32) -> Self {
        if value > 255.0 {
            255
        } else if value < 0.0 {
            0
        } else {
            value as u8
        }
    }

    fn to_f32(self) -> f32 {
        self as f32
    }

    fn from_u8(value: u8) -> Self {
        value
    }

    fn to_u8(self) -> u8 {
        self
    }

    fn min(self, other: Self) -> Self {
        std::cmp::min(self, other)
    }

    fn max(self, other: Self) -> Self {
        std::cmp::max(self, other)
    }

    fn MINIMUM() -> Self {
        0
    }

    fn weight_type() -> super::WeightType {
        super::WeightType::WeightU8
    }
}
