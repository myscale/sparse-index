use crate::{
    core::{DimWeight, QuantizedWeight},
    RowId,
};

use super::{ElementRead, ElementSlice, ElementWrite, GenericElementRef};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct SimpleElement<W: QuantizedWeight> {
    pub row_id: RowId,
    pub weight: W,
}

impl<W: QuantizedWeight> std::fmt::Display for SimpleElement<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(row_id: {}, weight: {:?})", self.row_id, self.weight)
    }
}

impl<W: QuantizedWeight> ElementWrite<W> for SimpleElement<W> {
    fn update_weight(&mut self, value: W) {
        self.weight = value;
    }

    fn update_max_next_weight(&mut self, _value: W) {}
}

impl<W: QuantizedWeight> ElementRead<W> for SimpleElement<W> {
    fn row_id(&self) -> RowId {
        self.row_id
    }

    fn weight(&self) -> W {
        self.weight
    }

    fn max_next_weight(&self) -> W {
        W::MINIMUM()
    }
}

impl<'a, W: QuantizedWeight> ElementSlice<'a, W> for &'a [SimpleElement<W>] {
    fn length(&self) -> usize {
        self.len()
    }

    fn get_opt(&self, index: usize) -> Option<GenericElementRef<'a, W>> {
        self.get(index).map(GenericElementRef::SimpleElementRef)
    }

    // fn generic_iter(&self) -> impl Iterator<Item = GenericElementRef<'a, W>> + '_ {
    fn generic_iter(&self) -> Box<dyn Iterator<Item = GenericElementRef<'a, W>> + '_> {
        Box::new(self.iter().map(GenericElementRef::SimpleElementRef))
    }
    fn slice_from(&self, range: std::ops::RangeFrom<usize>) -> Self {
        if range.start >= self.len() {
            return &self[0..0];
        }
        &self[range]
    }
    fn binary_search_by_row_id(&self, row_id: RowId) -> Result<usize, usize> {
        self.binary_search_by(|el| el.row_id().cmp(&row_id))
    }
}

impl<W: QuantizedWeight> SimpleElement<W> {
    pub fn new(row_id: RowId, weight: DimWeight) -> Self {
        Self { row_id, weight: W::from_f32(weight) }
    }
}

impl<W: QuantizedWeight> Default for SimpleElement<W> {
    fn default() -> Self {
        Self { row_id: 0, weight: W::MINIMUM() }
    }
}

impl<W: QuantizedWeight> From<super::ExtendedElement<W>> for SimpleElement<W> {
    fn from(value: super::ExtendedElement<W>) -> Self {
        Self { row_id: value.row_id, weight: value.weight }
    }
}

#[cfg(test)]
mod tests {
    use core::f32;

    use super::*;
    use crate::core::{DimWeight, ExtendedElement};
    use half::f16;

    // Test for SimpleElement with f32 type
    #[test]
    fn test_simple_element_f32() {
        // Define row_id and weight for the test.
        let row_id: RowId = 1;
        let weight: DimWeight = 42.0;

        // Create a SimpleElement with f32 type.
        let mut element = SimpleElement::<f32>::new(row_id, weight);

        // Verify that the element's row_id and weight match the input values.
        assert_eq!(element.row_id, row_id);
        assert_eq!(element.weight, 42.0);

        // Test the `ElementRead` trait methods.
        // Check if `row_id()` returns the correct row_id.
        assert_eq!(element.row_id(), row_id);
        // Check if `weight()` returns the correct weight.
        assert_eq!(element.weight(), 42.0);
        // `max_next_weight()` should return the minimum value for f32.
        assert_eq!(element.max_next_weight(), f32::NEG_INFINITY);

        // Test the `ElementWrite` trait methods.
        // Update the element's weight.
        element.update_weight(20.0);
        // Verify the weight has been updated.
        assert_eq!(element.weight, 20.0);
        assert_eq!(element.weight(), 20.0);

        // Test updating `max_next_weight` (no change expected for SimpleElement).
        element.update_max_next_weight(100.0);
        assert_eq!(element.max_next_weight(), f32::NEG_INFINITY); // No update for max_next_weight.
    }

    // Test the Default implementation for SimpleElement
    #[test]
    fn test_simple_element_default() {
        // Test default values for SimpleElement with f32, f16, and u8 types.

        // For f32 type, default value should be row_id = 0 and weight = NEG_INFINITY.
        let element_f32: SimpleElement<f32> = Default::default();
        assert_eq!(element_f32.row_id, 0);
        assert_eq!(element_f32.weight, f32::NEG_INFINITY);

        // For f16 type, default value should be row_id = 0 and weight = NEG_INFINITY.
        let element_f16: SimpleElement<f16> = Default::default();
        assert_eq!(element_f16.row_id, 0);
        assert_eq!(element_f16.weight, f16::NEG_INFINITY);

        // For u8 type, default value should be row_id = 0 and weight = 0.
        let element_u8: SimpleElement<u8> = Default::default();
        assert_eq!(element_u8.row_id, 0);
        assert_eq!(element_u8.weight, 0);
    }

    // Test conversion from ExtendedElement to SimpleElement
    #[test]
    fn test_from_extended_element() {
        // Create an ExtendedElement with some initial values.
        let extended = ExtendedElement { row_id: 1, weight: f32::MIN, max_next_weight: f32::MIN };

        // Convert ExtendedElement into SimpleElement.
        let simple_element: SimpleElement<f32> = extended.into();

        // Verify that the row_id and weight are correctly transferred.
        assert_eq!(simple_element.row_id, 1);
        assert_eq!(simple_element.weight, f32::MIN);
    }

    #[test]
    fn test_simple_element_slice() {
        // Create a vector of SimpleElement instances with different row_ids and weights.
        let elements = vec![
            SimpleElement::<f32>::new(1, 100.0),
            SimpleElement::<f32>::new(5, 50.0),
            SimpleElement::<f32>::new(7, 70.0),
            SimpleElement::<f32>::new(10, 10.0),
            SimpleElement::<f32>::new(15, 15.0),
            SimpleElement::<f32>::new(20, 20.0),
            SimpleElement::<f32>::new(25, 25.0),
            SimpleElement::<f32>::new(30, 30.0),
        ];

        // Borrow a slice of the elements vector for testing.
        let slice: &[SimpleElement<f32>] = &elements;

        // Assert that the slice has the expected length (8 elements).
        assert_eq!(slice.length(), 8);

        // Test `get_opt` method: Try retrieving an element that exists in the slice.
        let element_ref = slice.get_opt(0);
        assert!(element_ref.is_some()); // Element should be found.
        assert_eq!(element_ref.unwrap().row_id(), 1); // Check if the row_id matches.

        // Test `get_opt` method: Try retrieving an element that does not exist in the slice.
        let element_ref = slice.get_opt(10);
        assert!(element_ref.is_none()); // No element should be found for index 10.

        // Test `binary_search_by_row_id`: Search for an element with `row_id = 20`.
        let result = slice.binary_search_by_row_id(20);
        assert_eq!(result, Ok(5)); // `row_id = 20` should be at index 5 (zero-indexed).

        // Test `binary_search_by_row_id`: Search for an element with `row_id = 18` (nonexistent).
        let result = slice.binary_search_by_row_id(18);
        assert_eq!(result, Err(5)); // No element with `row_id = 18`, it should return `Err(5)`.

        // Test valid `RangeFrom`: Slice the elements from index 2 to the end of the vector.
        let sub_slice = slice.slice_from(2..);
        assert_eq!(sub_slice.length(), 6); // The sub-slice should contain 6 elements.
        assert_eq!(sub_slice.get_opt(0).unwrap().row_id(), 7); // The first element in the sub-slice should have `row_id = 7`.

        // Test invalid `RangeFrom`: Slice starting from index 10 (beyond the bounds of the vector).
        let sub_slice = slice.slice_from(10..);
        assert_eq!(sub_slice.length(), 0); // The sub-slice should be empty since the start index is out of bounds.
    }
}
