use crate::{
    core::{DimWeight, QuantizedWeight},
    RowId,
};

use super::{ElementRead, ElementSlice, ElementWrite, GenericElementRef};

/// SimpleElement
///
/// just contain a row_id and weight.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct SimpleElement<W: QuantizedWeight> {
    pub row_id: RowId,
    pub weight: W,
}

impl<W: QuantizedWeight> ElementWrite<W> for SimpleElement<W> {
    fn update_weight(&mut self, value: W) {
        self.weight = value;
    }

    fn update_max_next_weight(&mut self, _value: W) {
        // Do nothing.
    }
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
    fn slice(&self, range: std::ops::RangeFrom<usize>) -> Self {
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
        Self { row_id: value.row_id, weight: W::from_f32(value.weight.to_f32()) }
    }
}
