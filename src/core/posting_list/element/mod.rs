mod dispatch;
mod extended_element;
mod simple_element;
pub use dispatch::*;

pub const DEFAULT_MAX_NEXT_WEIGHT: f32 = f32::NEG_INFINITY;

use crate::{core::QuantizedWeight, RowId};
use enum_dispatch::enum_dispatch;
pub use extended_element::ExtendedElement;
use serde::{Deserialize, Serialize};
pub use simple_element::SimpleElement;
use std::fmt::Debug;

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub enum ElementType {
    #[default]
    #[serde(rename = "simple")]
    SIMPLE,
    #[serde(rename = "extended")]
    EXTENDED,
}

impl std::fmt::Display for ElementType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElementType::SIMPLE => write!(f, "simple"),
            ElementType::EXTENDED => write!(f, "extended"),
        }
    }
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

    fn slice_from(&self, range: std::ops::RangeFrom<usize>) -> Self;

    fn binary_search_by_row_id(&self, row_id: RowId) -> Result<usize, usize>;
}
