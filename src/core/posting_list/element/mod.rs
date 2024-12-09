use std::{any::TypeId, fmt::Debug};

use crate::{core::{QuantizedParam, QuantizedWeight, WeightType}, RowId};

mod simple_element;
mod extended_element;

pub const DEFAULT_MAX_NEXT_WEIGHT: f32 = f32::NEG_INFINITY;
pub const SIMPLE_ELEMENT_TYPE: u8 = 0;
pub const EXTENDED_ELEMENT_TYPE: u8 = 1;

use enum_dispatch::enum_dispatch;
use log::error;
pub use simple_element::SimpleElement;
pub use extended_element::ExtendedElement;


/// macro `enum_dispatch` doesn't support associated type in trait.
/// we need use generic type instead of associated type.
#[enum_dispatch]
pub trait Element<W: QuantizedWeight>: Default + Debug {
    fn row_id(&self) -> RowId;

    fn weight(&self) -> W;

    fn update_weight(&mut self, value: W);

    fn max_next_weight(&self) -> W;

    fn update_max_next_weight(&mut self, value: W);

    // fn convert_to<T: QuantizedWeight>(&self) -> Element<T>;
}


#[derive(Debug, Clone, PartialEq, PartialOrd)]
#[enum_dispatch(Element<W>)]
pub enum GenericElement<W: QuantizedWeight> {
    SimpleElement(SimpleElement<W>),
    ExtendedElement(ExtendedElement<W>)
}

impl<W: QuantizedWeight> Default for GenericElement<W> {
    fn default() -> Self {
        GenericElement::SimpleElement(SimpleElement { row_id: 0, weight: W::MINIMUM() })
    }
}

/// TODO: Figure out erase of genric type.
impl<W: QuantizedWeight> GenericElement<W> {
    pub fn type_id(&self) -> TypeId {
        match self {
            GenericElement::SimpleElement(_) => TypeId::of::<SimpleElement<W>>(),
            GenericElement::ExtendedElement(_) => TypeId::of::<ExtendedElement<W>>(),
        }
    }

    pub fn element_type(&self) -> u8 {
        match self {
            GenericElement::SimpleElement(_) => SIMPLE_ELEMENT_TYPE,
            GenericElement::ExtendedElement(_) => EXTENDED_ELEMENT_TYPE,
        }
    }

    pub fn check_valid(element_type: u8) {
        let valid: bool = element_type==SIMPLE_ELEMENT_TYPE || element_type==EXTENDED_ELEMENT_TYPE;
        if !valid {
            panic!("element_type should be SIMPLE or EXTENDED");
        }
    }

    pub fn quantized_with_param<TW: QuantizedWeight>(&self, quantized_param: QuantizedParam) -> GenericElement<TW> {
        match self {
            GenericElement::SimpleElement(simple_element) => {
                GenericElement::SimpleElement(
                    SimpleElement::<TW>{
                        row_id: simple_element.row_id(),
                        weight: TW::from_u8( W::quantize_with_param(simple_element.weight, quantized_param)),
                    }
                )
            },
            GenericElement::ExtendedElement(_) => {
                panic!("extended element not supported be quantized!")
            },
        }
    }

    /// 这是执行反量化的操作，并非是把 f32 存储为量化的操作
    pub fn type_convert<T: QuantizedWeight>(&self, quantized_param: Option<QuantizedParam>) -> GenericElement<T> {
        match self {
            GenericElement::SimpleElement(simple_element) => {
                // [`SimpleElement`] can be quantized.
                if quantized_param.is_none() {
                    GenericElement::SimpleElement(
                        SimpleElement::<T>{
                            row_id: simple_element.row_id(),
                            weight: T::from_f32(W::to_f32(simple_element.weight())),
                        }
                    )
                } else {
                    if W::weight_type()!=WeightType::WeightU8 {
                        let error_msg = format!("Can't unquantize a non-u8 type weight.");
                        error!("{}", error_msg);
                        panic!("{}", error_msg);
                    }
                    GenericElement::SimpleElement(
                        SimpleElement::<T>{
                            row_id: simple_element.row_id(),
                            weight: T::unquantize_with_param(W::to_u8(simple_element.weight()), quantized_param.unwrap()),
                        }
                    )
                }

            },
            GenericElement::ExtendedElement(extended_element) => {
                // [`ExtendedElement`] doesn't support quantized. 
                assert!(quantized_param.is_none());
                GenericElement::ExtendedElement(
                    ExtendedElement::<T> {
                        row_id: extended_element.row_id(),
                        weight: T::from_f32(W::to_f32(extended_element.weight())),
                        max_next_weight: T::from_f32(W::to_f32(extended_element.max_next_weight())),
                    }
                )
            },
        }
    }
}


