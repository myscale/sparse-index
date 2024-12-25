use enum_dispatch::enum_dispatch;

use crate::core::{transmute_from_u8_to_slice, transmute_to_u8_slice, QuantizedWeight};

#[allow(unused_imports)]
use super::super::{ElementSlice, ElementType, ExtendedElement, SimpleElement};

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
                let posting_slice: &'a [SimpleElement<W>] = transmute_from_u8_to_slice(element_bytes);
                GenericElementSlice::from_simple_slice(posting_slice)
            }
            ElementType::EXTENDED => {
                let posting_slice: &'a [ExtendedElement<W>] = transmute_from_u8_to_slice(element_bytes);
                GenericElementSlice::from_extended_slice(posting_slice)
            }
        }
    }

    #[allow(unused)]
    pub fn to_u8_slice(&self) -> &'a [u8] {
        match self {
            GenericElementSlice::SimpleElementSlice(&ref simple_slice) => transmute_to_u8_slice(simple_slice),
            GenericElementSlice::ExtendedElementSlice(&ref extended_slice) => transmute_to_u8_slice(extended_slice),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        core::{ElementRead, ElementSlice, ElementType, ExtendedElement, QuantizedWeight, SimpleElement},
        RowId,
    };

    use super::GenericElementSlice;

    fn mock_extended_elements<W: QuantizedWeight>() -> Vec<ExtendedElement<W>> {
        let total_rows = 100;
        let mut extended_elements: Vec<ExtendedElement<W>> = (0..total_rows)
            .map(|i| ExtendedElement::<W> {
                row_id: total_rows - i as RowId,
                weight: W::from_f32(i as f32 + i as f32 * 0.011),
                max_next_weight: W::from_f32(i as f32 + i as f32 * 0.011),
            })
            .collect();

        extended_elements.reverse();
        return extended_elements;
    }

    // Generic test function
    #[rustfmt::skip]
    fn test_serialize_and_deserialize<W: QuantizedWeight>(element_type: ElementType) {
        let extended_elements = mock_extended_elements::<W>();
        let simple_elements = extended_elements.iter().map(|e| SimpleElement::from(e.clone())).collect::<Vec<_>>();

        assert!(extended_elements.len()!=0);
        assert_eq!(extended_elements.len(), simple_elements.len());

        let generic_slice = match element_type {
            ElementType::SIMPLE => {
                let simple_slice: &[SimpleElement<W>] = &simple_elements;
                GenericElementSlice::from_simple_slice(simple_slice)
            }
            ElementType::EXTENDED => {
                let extended_slice: &[ExtendedElement<W>] = &extended_elements;
                GenericElementSlice::from_extended_slice(extended_slice)
            }
        };

        // Convert to byte slice using to_u8_slice method
        let simple_elements_bytes = generic_slice.to_u8_slice();
        // Create GenericElementSlice from byte slice
        let restore_generic_slice = GenericElementSlice::<W>::from_bytes_and_type(element_type, simple_elements_bytes);

        match restore_generic_slice {
            GenericElementSlice::SimpleElementSlice(slice) => {
                for (restored_simple_element_ref, origin_generic_element_ref) in
                    slice.iter().zip(generic_slice.generic_iter())
                {
                    assert_eq!(restored_simple_element_ref.row_id(), origin_generic_element_ref.row_id());
                    assert_eq!(restored_simple_element_ref.weight(), origin_generic_element_ref.weight());
                    assert_eq!(restored_simple_element_ref.max_next_weight(), origin_generic_element_ref.max_next_weight());
                }
            }
            GenericElementSlice::ExtendedElementSlice(slice) => {
                for (restored_extended_element_ref, origin_generic_element_ref) in
                    slice.iter().zip(generic_slice.generic_iter())
                {
                    assert_eq!(restored_extended_element_ref.row_id(), origin_generic_element_ref.row_id());
                    assert_eq!(restored_extended_element_ref.weight(), origin_generic_element_ref.weight());
                    assert_eq!(restored_extended_element_ref.max_next_weight(), origin_generic_element_ref.max_next_weight());
                }
            }
        }
    }

    #[test]
    fn test_generic_element_slice() {
        test_serialize_and_deserialize::<f32>(ElementType::SIMPLE);
        test_serialize_and_deserialize::<f32>(ElementType::EXTENDED);
        test_serialize_and_deserialize::<half::f16>(ElementType::SIMPLE);
        test_serialize_and_deserialize::<half::f16>(ElementType::EXTENDED);
        test_serialize_and_deserialize::<u8>(ElementType::SIMPLE);
        test_serialize_and_deserialize::<u8>(ElementType::EXTENDED);
    }
}
