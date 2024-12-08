use crate::{core::{DimWeight, Element, QuantizedWeight}, RowId};

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


impl<W: QuantizedWeight> Element<W> for ExtendedElement<W>  {

    fn row_id(&self) -> RowId {
        return self.row_id;
    }

    fn weight(&self) -> W {
        return self.weight;
    }

    fn update_weight(&mut self,value:W) {
        self.weight = value
    }
    
    fn max_next_weight(&self) -> W {
        return self.max_next_weight;
    }
    
    fn update_max_next_weight(&mut self, value:W) {
        self.max_next_weight = value
    }
    
    // fn convert_to<T:QuantizedWeight>(&self) -> Self {
    //     ExtendedElement::<T> {
    //         row_id: self.row_id(),
    //         weight: T::from_f32(W::to_f32(self.weight())),
    //         max_next_weight: T::from_f32(W::to_f32(self.max_next_weight())),
    //     }
    // }
}


impl<W: QuantizedWeight> ExtendedElement<W>  {
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
        Self {
            row_id: 0,
            weight: W::MINIMUM(),
            max_next_weight: W::MINIMUM()
        }
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
