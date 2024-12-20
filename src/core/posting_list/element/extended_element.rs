use crate::{
    core::{DimWeight, QuantizedWeight},
    RowId,
};

use super::{ElementRead, ElementSlice, ElementWrite, GenericElementRef};

/// PostingListElementEx
///
/// It's `max_next_weight` is useful for accelerating search process.
/// When quantized is enabled, you should use [`super::PostingListElement`]
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct ExtendedElement<W: QuantizedWeight> {
    pub row_id: RowId,
    pub weight: W,
    /// max next weight in a posting with specific dimension.
    pub max_next_weight: W,
}

impl<W: QuantizedWeight> ElementWrite<W> for ExtendedElement<W> {
    fn update_weight(&mut self, value: W) {
        self.weight = value;
    }

    fn update_max_next_weight(&mut self, value: W) {
        self.max_next_weight = value;
    }
}

impl<W: QuantizedWeight> ElementRead<W> for ExtendedElement<W> {
    fn row_id(&self) -> RowId {
        self.row_id
    }

    fn weight(&self) -> W {
        self.weight
    }

    fn max_next_weight(&self) -> W {
        self.max_next_weight
    }
}

impl<'a, W: QuantizedWeight> ElementSlice<'a, W> for &'a [ExtendedElement<W>] {
    fn length(&self) -> usize {
        self.len()
    }

    fn get_opt(&self, index: usize) -> Option<GenericElementRef<'a, W>> {
        self.get(index).map(GenericElementRef::ExtendedElementRef)
    }

    // fn generic_iter(&self) -> impl Iterator<Item = GenericElementRef<'a, W>> + '_ {
    fn generic_iter(&self) -> Box<dyn Iterator<Item = GenericElementRef<'a, W>> + '_> {
        Box::new(self.iter().map(GenericElementRef::ExtendedElementRef))
    }

    fn slice(&self, range: std::ops::RangeFrom<usize>) -> Self {
        &self[range]
    }

    fn binary_search_by_row_id(&self, row_id: RowId) -> Result<usize, usize> {
        self.binary_search_by(|el| el.row_id().cmp(&row_id))
    }
}

impl<W: QuantizedWeight> ExtendedElement<W> {
    pub fn new(row_id: RowId, weight: DimWeight) -> Self {
        Self {
            row_id,
            weight: W::from_f32(weight),
            max_next_weight: W::from_f32(super::DEFAULT_MAX_NEXT_WEIGHT),
        }
    }
}

impl<W: QuantizedWeight> Default for ExtendedElement<W> {
    fn default() -> Self {
        Self { row_id: 0, weight: W::MINIMUM(), max_next_weight: W::MINIMUM() }
    }
}

impl<W: QuantizedWeight> From<super::SimpleElement<W>> for ExtendedElement<W> {
    fn from(value: super::SimpleElement<W>) -> Self {
        Self {
            row_id: value.row_id,
            weight: value.weight,
            max_next_weight: W::from_f32(super::DEFAULT_MAX_NEXT_WEIGHT),
        }
    }
}
