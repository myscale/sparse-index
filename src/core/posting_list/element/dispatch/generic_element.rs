use enum_dispatch::enum_dispatch;
use log::error;

use crate::core::{QuantizedParam, QuantizedWeight, WeightType};

#[allow(unused_imports)]
use super::super::{ElementRead, ElementType, ElementWrite, ExtendedElement, SimpleElement};

#[derive(Clone, PartialEq, PartialOrd)]
#[enum_dispatch(ElementWrite<W>, ElementRead<W>)]
pub enum GenericElement<W: QuantizedWeight> {
    SimpleElement(SimpleElement<W>),
    ExtendedElement(ExtendedElement<W>),
}

impl<W: QuantizedWeight> std::fmt::Display for GenericElement<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GenericElement::SimpleElement(simple_element) => write!(f, "{}", simple_element),
            GenericElement::ExtendedElement(extended_element) => write!(f, "{}", extended_element),
        }
    }
}

impl<W: QuantizedWeight> std::fmt::Debug for GenericElement<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GenericElement::SimpleElement(simple_element) => write!(f, "{}", simple_element),
            GenericElement::ExtendedElement(extended_element) => write!(f, "{}", extended_element),
        }
    }
}

impl<W: QuantizedWeight> GenericElement<W> {
    pub fn element_type(&self) -> ElementType {
        match self {
            GenericElement::SimpleElement(_) => ElementType::SIMPLE,
            GenericElement::ExtendedElement(_) => ElementType::EXTENDED,
        }
    }

    pub fn as_simple(&self) -> Option<&SimpleElement<W>> {
        if let GenericElement::SimpleElement(simple) = self {
            Some(simple)
        } else {
            let error_msg = "Can't call this `GenericElement` func `as_simple`.";
            error!("{}", error_msg);
            None
        }
    }

    pub fn as_extended(&self) -> Option<&ExtendedElement<W>> {
        if let GenericElement::ExtendedElement(extended) = self {
            Some(extended)
        } else {
            let error_msg = "Can't call this `GenericElement` func `as_extended`.";
            error!("{}", error_msg);
            None
        }
    }

    pub fn quantize_with_param<TW: QuantizedWeight>(&self, quantized_param: QuantizedParam) -> GenericElement<TW> {
        // Boundary
        #[cfg(debug_assertions)]
        {
            if TW::weight_type() != WeightType::WeightU8 {
                let error_msg = "Quantized weight type can only be u8.";
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
        match self {
            GenericElement::SimpleElement(simple_element) => GenericElement::SimpleElement(SimpleElement::<TW> {
                row_id: simple_element.row_id(),
                weight: TW::from_u8(W::quantize_with_param(simple_element.weight, quantized_param)),
            }),
            GenericElement::ExtendedElement(_) => {
                let error_msg = "Not supported! `ExtendedElement` can't be quantized.";
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
    }

    pub fn unquantize_with_param<OW: QuantizedWeight>(&self, quantized_param: QuantizedParam) -> GenericElement<OW> {
        // Boundary
        #[cfg(debug_assertions)]
        {
            if W::weight_type() != WeightType::WeightU8 {
                let error_msg = format!("Can only unquantize the u8 weight type.");
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
        match self {
            GenericElement::SimpleElement(simple_element) => GenericElement::SimpleElement(SimpleElement::<OW> {
                row_id: simple_element.row_id(),
                weight: OW::unquantize_with_param(W::to_u8(simple_element.weight()), quantized_param),
            }),
            GenericElement::ExtendedElement(_) => {
                let error_msg = "Not supported! `ExtendedElement` can't be unquantized.";
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }
    }

    pub fn type_convert<T: QuantizedWeight>(&self) -> GenericElement<T> {
        if T::weight_type() == W::weight_type() {
            let converted: &GenericElement<T> = unsafe { std::mem::transmute(self) };
            return converted.clone();
        }
        match self {
            GenericElement::SimpleElement(simple_element) => {
                GenericElement::SimpleElement(SimpleElement::<T> { row_id: simple_element.row_id(), weight: T::from_f32(W::to_f32(simple_element.weight())) })
            }
            GenericElement::ExtendedElement(extended_element) => GenericElement::ExtendedElement(ExtendedElement::<T> {
                row_id: extended_element.row_id(),
                weight: T::from_f32(W::to_f32(extended_element.weight())),
                max_next_weight: T::from_f32(W::to_f32(extended_element.max_next_weight())),
            }),
        }
    }

    pub fn convert_or_unquantize<T: QuantizedWeight>(&self, quantized_param: Option<QuantizedParam>) -> GenericElement<T> {
        match quantized_param {
            Some(param) => self.unquantize_with_param(param),
            None => self.type_convert(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::QuantizedParam;

    // Test for element_type method
    #[test]
    fn test_generic_element_type() {
        let simple_element = SimpleElement::<f32>::new(1, 100.0);
        let extended_element = ExtendedElement::<f32>::new(2, 200.0);

        let simple_generic = GenericElement::SimpleElement(simple_element);
        let extended_generic = GenericElement::ExtendedElement(extended_element);

        // Test element_type for SimpleElement
        assert_eq!(simple_generic.element_type(), ElementType::SIMPLE);

        // Test element_type for ExtendedElement
        assert_eq!(extended_generic.element_type(), ElementType::EXTENDED);
    }

    // Test for as_simple method
    #[test]
    fn test_generic_element_as_simple() {
        let simple_element = SimpleElement::<f32>::new(1, 100.0);
        let generic_element = GenericElement::SimpleElement(simple_element);

        // Test successful conversion to SimpleElement
        let simple_ref = generic_element.as_simple().unwrap();
        assert_eq!(simple_ref.row_id, 1);
        assert_eq!(simple_ref.row_id(), 1);
        assert_eq!(simple_ref.weight, 100.0);
        assert_eq!(simple_ref.weight(), 100.0);

        // Test calling as_simple on ExtendedElement (should panic)
        let extended_element = ExtendedElement::<f32>::new(2, 200.0);
        let generic_extended = GenericElement::ExtendedElement(extended_element);
        assert!(generic_extended.as_simple().is_none());
    }

    // Test for as_extended method
    #[test]
    fn test_generic_element_as_extended() {
        let extended_element = ExtendedElement::<f32>::new(2, 200.0);
        let generic_element = GenericElement::ExtendedElement(extended_element);

        // Test successful conversion to ExtendedElement
        let extended_ref = generic_element.as_extended().unwrap();
        assert_eq!(extended_ref.row_id, 2);
        assert_eq!(extended_ref.row_id(), 2);
        assert_eq!(extended_ref.weight, 200.0);
        assert_eq!(extended_ref.weight(), 200.0);

        // Test calling as_extended on SimpleElement (should panic)
        let simple_element = SimpleElement::<f32>::new(1, 100.0);
        let generic_simple = GenericElement::SimpleElement(simple_element);
        assert!(generic_simple.as_extended().is_none());
    }

    // Test for quantize_with_param method
    #[test]
    fn test_quantize_with_param() {
        let simple_element = SimpleElement::<f32>::new(1, 9.3754);
        let generic_element = GenericElement::SimpleElement(simple_element);

        // Quantize with a sample parameter
        let quantized_param = QuantizedParam::from_minmax(0.0, 10.0);
        let quantized_generic = generic_element.quantize_with_param::<u8>(quantized_param);

        // Check if quantized element has correct row_id and weight type
        if let GenericElement::SimpleElement(simple) = quantized_generic {
            assert_eq!(simple.row_id(), 1);
            assert_eq!(simple.weight(), 239); // 10/255*239 = 9.3725490196
        } else {
            panic!("Expected SimpleElement after quantization");
        }
    }

    // Test for unquantize_with_param method
    #[test]
    fn test_unquantize_with_param() {
        let simple_element = SimpleElement::<u8>::new(1, 239.0);
        let generic_element = GenericElement::SimpleElement(simple_element);

        // Unquantize with a sample parameter
        let quantized_param = QuantizedParam::from_minmax(0.0, 10.0);
        let unquantized_generic = generic_element.unquantize_with_param::<f32>(quantized_param);

        // Check if unquantized element has correct row_id and weight type
        if let GenericElement::SimpleElement(simple) = unquantized_generic {
            assert_eq!(simple.row_id(), 1);
            assert_eq!(format!("{:.2}", simple.weight()), format!("{:.2}", 10.0 / 255.0 * 239.0));
        } else {
            panic!("Expected SimpleElement after unquantization");
        }
    }

    // Test for type_convert method
    #[test]
    fn test_type_convert() {
        let simple_element = SimpleElement::<f32>::new(1, 9.375431);
        let generic_element = GenericElement::SimpleElement(simple_element);

        // Check convert from f32 to f32
        if let GenericElement::SimpleElement(simple) = generic_element.type_convert::<f32>() {
            assert_eq!(simple.row_id(), 1);
            assert_eq!(simple.weight(), 9.375431);
        } else {
            panic!("Expected SimpleElement after type conversion");
        }

        // Check convert from f32 to u8
        if let GenericElement::SimpleElement(simple) = generic_element.type_convert::<u8>() {
            assert_eq!(simple.row_id(), 1);
            assert_eq!(simple.weight(), 9);
        } else {
            panic!("Expected SimpleElement after type conversion");
        }

        // Check convert from f32 to f16
        if let GenericElement::SimpleElement(simple) = generic_element.type_convert::<half::f16>() {
            assert_eq!(simple.row_id(), 1);
            assert_eq!(simple.weight(), half::f16::from_f32(9.375431));
        } else {
            panic!("Expected SimpleElement after type conversion");
        }
    }

    // Test for convert_or_unquantize method
    #[test]
    fn test_convert_or_unquantize() {
        let simple_element = SimpleElement::<u8>::new(1, 239.0);
        let generic_element = GenericElement::SimpleElement(simple_element);

        // Test convert with no quantized_param
        let converted_generic = generic_element.convert_or_unquantize::<f32>(None);

        // Check if element was converted (not unquantized)
        if let GenericElement::SimpleElement(simple) = converted_generic {
            assert_eq!(simple.row_id(), 1);
            assert_eq!(simple.weight(), 239.0);
        } else {
            panic!("Expected SimpleElement after conversion");
        }

        // Test unquantize with a quantized_param
        let quantized_param = QuantizedParam::from_minmax(0.0, 10.0);
        let unquantized_generic = generic_element.convert_or_unquantize::<f32>(Some(quantized_param));

        // Check if element was unquantized (not converted)
        if let GenericElement::SimpleElement(simple) = unquantized_generic {
            assert_eq!(simple.row_id(), 1);
            assert_eq!(format!("{:.2}", simple.weight()), format!("{:.2}", 10.0 / 255.0 * 239.0));
        } else {
            panic!("Expected SimpleElement after unquantization");
        }
    }
}
