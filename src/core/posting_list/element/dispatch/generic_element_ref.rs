use enum_dispatch::enum_dispatch;

use crate::core::QuantizedWeight;

#[allow(unused_imports)]
use super::super::{ElementRead, ExtendedElement, GenericElement, SimpleElement};

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
            GenericElementRef::SimpleElementRef(&ref simple_ref) => GenericElement::SimpleElement(simple_ref.clone()),
            GenericElementRef::ExtendedElementRef(&ref extended_ref) => GenericElement::ExtendedElement(extended_ref.clone()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::core::{ElementWrite, GenericElement, SimpleElement};

    use super::GenericElementRef;

    #[test]
    fn test_generic_element_ref_to_owned() {
        // Create a SimpleElement
        let mut simple = SimpleElement::<f32>::new(1, 10.0);

        // Create a reference to SimpleElement
        let simple_ref = GenericElementRef::SimpleElementRef(&simple);

        // Convert reference to owned element
        let owned = simple_ref.to_owned();

        simple.update_weight(12.0);

        if let GenericElement::SimpleElement(simple_element) = owned {
            assert_eq!(simple_element.row_id, 1);
            assert_eq!(simple_element.weight, 10.0);
        } else {
            panic!("Expected SimpleElement");
        }
    }
}
