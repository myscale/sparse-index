use crate::{core::{DimWeight, Element, QuantizedWeight}, RowId};

/// SimpleElement
/// 
/// just contain a row_id and weight.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct SimpleElement<W: QuantizedWeight> {
    pub row_id: RowId,
    pub weight: W,
}


impl<W: QuantizedWeight> Element<W> for SimpleElement<W>  {
    fn row_id(&self) -> RowId {
        return self.row_id;
    }

    fn weight(&self) -> W {
        return self.weight;
    }
    
    fn update_weight(&mut self, value:W) {
        self.weight = value
    }

    fn max_next_weight(&self) -> W {
        // panic!("SimpleElement doesn't contain `max_next_weight` attribute")
        W::MINIMUM()
    }
    
    fn update_max_next_weight(&mut self, _value: W) {
        // panic!("SimpleElement doesn't contain `max_next_weight` attribute")
    }
    
    // fn convert_to<T:QuantizedWeight>(&self) -> Self {
    //     SimpleElement::<T>{
    //         row_id: self.row_id(),
    //         weight: T::from_f32(W::to_f32(self.weight())),
    //     }
    // }
}

impl<W: QuantizedWeight> SimpleElement<W>  {
    pub fn new(row_id: RowId, weight: DimWeight) -> Self {
        Self {
            row_id,
            weight: W::from_f32(weight)
        }
    }
}

impl<W: QuantizedWeight> Default for SimpleElement<W> {
    fn default() -> Self {
        Self {
            row_id: 0,
            weight: W::MINIMUM()
        }
    }
}

impl<W: QuantizedWeight> From<super::ExtendedElement<W>> for SimpleElement<W> {
    fn from(value: super::ExtendedElement<W>) -> Self {
        Self { row_id: value.row_id, weight: W::from_f32(value.weight.to_f32()) }
    }
}
