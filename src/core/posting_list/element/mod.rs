use std::fmt::Debug;

use crate::{
    core::{
        transmute_from_u8_to_slice, transmute_to_u8_slice, QuantizedParam, QuantizedWeight,
        WeightType,
    },
    RowId,
};

mod extended_element;
mod simple_element;

pub const DEFAULT_MAX_NEXT_WEIGHT: f32 = f32::NEG_INFINITY;
// pub const SIMPLE_ELEMENT_TYPE: u8 = 0;
// pub const EXTENDED_ELEMENT_TYPE: u8 = 1;

use enum_dispatch::enum_dispatch;
pub use extended_element::ExtendedElement;
use log::error;
use serde::{Deserialize, Serialize};
pub use simple_element::SimpleElement;

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub enum ElementType {
    #[default]
    #[serde(rename = "simple")]
    SIMPLE,
    #[serde(rename = "extended")]
    EXTENDED,
}

#[enum_dispatch]
pub trait ElementWrite<W: QuantizedWeight> {
    fn update_weight(&mut self, value: W);
    fn update_max_next_weight(&mut self, value: W);
}

#[enum_dispatch]
pub trait ElementRead<W: QuantizedWeight> {
    fn row_id(&self) -> RowId;
    fn weight(&self) -> W;
    fn max_next_weight(&self) -> W;
}

#[enum_dispatch]
pub trait ElementSlice<'a, W: QuantizedWeight> {
    fn length(&self) -> usize;

    fn get_opt(&self, index: usize) -> Option<GenericElementRef<'a, W>>;

    fn last_opt(&self) -> Option<GenericElementRef<'a, W>> {
        let len = self.length();
        if len == 0 {
            None
        } else {
            self.get_opt(len - 1)
        }
    }

    fn generic_iter(&self) -> Box<dyn Iterator<Item = GenericElementRef<'a, W>> + '_>;
    // fn generic_iter(&self) -> impl Iterator<Item = GenericElementRef<'a, W>> + '_;

    fn slice(&self, range: std::ops::RangeFrom<usize>) -> Self;

    fn binary_search_by_row_id(&self, row_id: RowId) -> Result<usize, usize>;
}

impl<W: QuantizedWeight, T: ElementRead<W> + ?Sized> ElementRead<W> for &T {
    fn row_id(&self) -> RowId {
        (**self).row_id()
    }

    fn weight(&self) -> W {
        (**self).weight()
    }

    fn max_next_weight(&self) -> W {
        (**self).max_next_weight()
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
#[enum_dispatch(ElementWrite<W>, ElementRead<W>)]
pub enum GenericElement<W: QuantizedWeight> {
    SimpleElement(SimpleElement<W>),
    ExtendedElement(ExtendedElement<W>),
}

// impl<W: QuantizedWeight> Default for GenericElement<W> {
//     fn default() -> Self {
//         GenericElement::SimpleElement(SimpleElement { row_id: 0, weight: W::MINIMUM() })
//     }
// }

/// TODO: Figure out erase of genric type.
impl<W: QuantizedWeight> GenericElement<W> {
    // pub fn type_id(&self) -> TypeId {
    //     match self {
    //         GenericElement::SimpleElement(_) => TypeId::of::<SimpleElement<W>>(),
    //         GenericElement::ExtendedElement(_) => TypeId::of::<ExtendedElement<W>>(),
    //     }
    // }

    pub fn element_type(&self) -> ElementType {
        match self {
            GenericElement::SimpleElement(_) => ElementType::SIMPLE,
            GenericElement::ExtendedElement(_) => ElementType::EXTENDED,
        }
    }

    pub fn as_simple(&self) -> &SimpleElement<W> {
        if let GenericElement::SimpleElement(simple) = self {
            simple
        } else {
            panic!("Failed call Generic `as_simple`");
        }
    }

    pub fn as_extended(&self) -> &ExtendedElement<W> {
        if let GenericElement::ExtendedElement(extended) = self {
            extended
        } else {
            panic!("Failed call Generic `as_extended`");
        }
    }

    pub fn quantized_with_param<TW: QuantizedWeight>(
        &self,
        quantized_param: QuantizedParam,
    ) -> GenericElement<TW> {
        match self {
            GenericElement::SimpleElement(simple_element) => {
                GenericElement::SimpleElement(SimpleElement::<TW> {
                    row_id: simple_element.row_id(),
                    weight: TW::from_u8(W::quantize_with_param(
                        simple_element.weight,
                        quantized_param,
                    )),
                })
            }
            GenericElement::ExtendedElement(_) => {
                panic!("extended element not supported be quantized!")
            }
        }
    }

    /// 这是执行反量化的操作，并非是把 f32 存储为量化的操作
    pub fn type_convert<T: QuantizedWeight>(
        &self,
        quantized_param: Option<QuantizedParam>,
    ) -> GenericElement<T> {
        match self {
            GenericElement::SimpleElement(simple_element) => {
                // [`SimpleElement`] can be quantized.
                if quantized_param.is_none() {
                    GenericElement::SimpleElement(SimpleElement::<T> {
                        row_id: simple_element.row_id(),
                        weight: T::from_f32(W::to_f32(simple_element.weight())),
                    })
                } else {
                    if W::weight_type() != WeightType::WeightU8 {
                        let error_msg = format!("Can't unquantize a non-u8 type weight.");
                        error!("{}", error_msg);
                        panic!("{}", error_msg);
                    }
                    GenericElement::SimpleElement(SimpleElement::<T> {
                        row_id: simple_element.row_id(),
                        weight: T::unquantize_with_param(
                            W::to_u8(simple_element.weight()),
                            quantized_param.unwrap(),
                        ),
                    })
                }
            }
            GenericElement::ExtendedElement(extended_element) => {
                // [`ExtendedElement`] doesn't support quantized.
                if quantized_param.is_some() {
                    let error_msg =
                        "ExtendedElement can't be unquantized, cause it can't be quantized stored.";
                    error!("{}", error_msg);
                }
                assert!(quantized_param.is_none());

                GenericElement::ExtendedElement(ExtendedElement::<T> {
                    row_id: extended_element.row_id(),
                    weight: T::from_f32(W::to_f32(extended_element.weight())),
                    max_next_weight: T::from_f32(W::to_f32(extended_element.max_next_weight())),
                })
            }
        }
    }
}

// impl <'a, W:QuantizedWeight> &'a Vec<GenericElement<W>> {

// }

// fn transmute_generic_elements_to_u8_slice<'a, W: QuantizedWeight>(elements: &'a Vec<GenericElement<W>>, element_type: ElementType) -> Cow<[u8]> {
//     match element_type {
//         ElementType::SIMPLE => {
//             let simple_els: Vec<SimpleElement<W>> = elements.iter().map(|e| e.as_simple().clone()).collect::<Vec<_>>();
//             let posting_elements_bytes = transmute_to_u8_slice(&simple_els);
//             posting_elements_bytes
//         },
//         ElementType::EXTENDED => {
//             let elements: Vec<ExtendedElement<W>> = elements.iter().map(|e|e.as_extended().clone()).collect::<Vec<_>>();
//             let posting_elements_bytes= transmute_to_u8_slice(&elements);
//             posting_elements_bytes
//         },
//     }
// }

#[derive(Debug, Clone, PartialEq, PartialOrd)]
#[enum_dispatch(ElementRead<W>)]
pub enum GenericElementRef<'a, W: QuantizedWeight> {
    SimpleElementRef(&'a SimpleElement<W>),
    ExtendedElementRef(&'a ExtendedElement<W>),
}

impl<'a, W: QuantizedWeight> GenericElementRef<'a, W> {
    /// Converts `GenericElementRef` to an owned `GenericElement`.
    pub fn to_owned(&self) -> GenericElement<W> {
        match self {
            GenericElementRef::SimpleElementRef(simple_ref) => {
                GenericElement::SimpleElement((*simple_ref).clone())
            }
            GenericElementRef::ExtendedElementRef(extended_ref) => {
                GenericElement::ExtendedElement((*extended_ref).clone())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
#[enum_dispatch(ElementSlice<W>)]
pub enum GenericElementSlice<'a, W: QuantizedWeight> {
    SimpleElementSlice(&'a [SimpleElement<W>]),
    ExtendedElementSlice(&'a [ExtendedElement<W>]),
}

impl<'a, W: QuantizedWeight> GenericElementSlice<'a, W> {
    pub fn from_simple_slice(slice: &'a [SimpleElement<W>]) -> Self {
        GenericElementSlice::SimpleElementSlice(slice)
    }
    pub fn from_extended_slice(slice: &'a [ExtendedElement<W>]) -> Self {
        GenericElementSlice::ExtendedElementSlice(slice)
    }

    pub fn empty_slice(element_type: ElementType) -> GenericElementSlice<'static, W> {
        match element_type {
            ElementType::SIMPLE => GenericElementSlice::SimpleElementSlice(&[]),
            ElementType::EXTENDED => GenericElementSlice::ExtendedElementSlice(&[]),
        }
    }

    pub fn from_bytes_and_type(element_type: ElementType, element_bytes: &'a [u8]) -> Self {
        match element_type {
            ElementType::SIMPLE => {
                let posting_slice: &'a [SimpleElement<W>] =
                    transmute_from_u8_to_slice(element_bytes);
                GenericElementSlice::from_simple_slice(posting_slice)
            }
            ElementType::EXTENDED => {
                let posting_slice: &'a [ExtendedElement<W>] =
                    transmute_from_u8_to_slice(element_bytes);
                GenericElementSlice::from_extended_slice(posting_slice)
            }
        }
    }
    pub fn to_u8_slice(&self) -> &'a [u8] {
        match self {
            GenericElementSlice::SimpleElementSlice(simple_slice) => {
                transmute_to_u8_slice(simple_slice)
            }
            GenericElementSlice::ExtendedElementSlice(extended_slice) => {
                transmute_to_u8_slice(extended_slice)
            }
        }
    }
}
