use crate::core::{inverted_index::common::InvertedIndexMetrics, ElementType};
use std::fmt::Debug;



pub trait InvertedIndexRamAccess: Sized + Debug {
    fn metrics(&self) -> InvertedIndexMetrics;
    fn element_type(&self) -> ElementType;
    fn size(&self) -> usize;
    fn empty(&self) -> bool {
        self.size() == 0
    }
}
