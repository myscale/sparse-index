use std::mem::size_of;

use crate::core::{
    ElementRead, ElementType, ElementWrite, ExtendedElement, GenericElement, QuantizedWeight,
    SimpleElement, DEFAULT_MAX_NEXT_WEIGHT,
};
use crate::RowId;
use log::{debug, error};

#[derive(Debug, Clone, PartialEq)]
pub struct PostingList<OW: QuantizedWeight> {
    pub elements: Vec<GenericElement<OW>>,
    pub element_type: ElementType,
}

impl<OW: QuantizedWeight> Default for PostingList<OW> {
    fn default() -> Self {
        Self::new(ElementType::SIMPLE)
    }
}

impl<OW: QuantizedWeight> PostingList<OW> {
    pub fn new(element_type: ElementType) -> Self {
        Self { elements: vec![], element_type }
    }
}

impl<OW: QuantizedWeight> PostingList<OW> {
    pub fn get_ref(&self, idx: usize) -> &GenericElement<OW> {
        self.elements.get(idx).unwrap_or_else(|| {
            let error_msg = format!(
                "idx:{} overflow when `get_ref` of GenericElement, posting length is {}",
                idx,
                self.len()
            );
            error!("{}", error_msg);
            panic!("{}", error_msg);
        })
    }

    pub fn storage_size(&self) -> usize {
        match self.element_type {
            ElementType::SIMPLE => self.len() * size_of::<SimpleElement<OW>>(),
            ElementType::EXTENDED => self.len() * size_of::<ExtendedElement<OW>>(),
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn delete(&mut self, row_id: RowId) -> (usize, bool) {
        let search_result = self.elements.binary_search_by_key(&row_id, |e| e.row_id());

        match search_result {
            Ok(found_idx) => {
                self.elements.remove(found_idx);

                // Reset the max_next_weight for the last element to the default.
                if self.element_type == ElementType::EXTENDED {
                    if let Some(last) = self.elements.last_mut() {
                        last.update_max_next_weight(OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT));
                    }
                }
                return (found_idx, true);
            }
            Err(_) => {
                // Return 0 to indicate that no element was deleted.
                // This value should be handled carefully as it also represents the index of the first element.
                return (0, false);
            }
        }
    }

    pub fn delete_with_propagate(&mut self, row_id: RowId) -> bool {
        let (deleted_idx, success_deleted) = self.delete(row_id);
        if !success_deleted {
            return false;
        }
        if self.element_type == ElementType::SIMPLE {
            return true;
        }

        if deleted_idx < self.elements.len() {
            // The element on `deleted_idx` has already been replaced by the right elements,
            // we only need to propagate it's left side.
            self.propagate_max_next_weight(deleted_idx);
        } else if !self.elements.is_empty() {
            // The element on `deleted_idx` is the Posting's last element,
            // we should propagate it's left side.
            self.propagate_max_next_weight(self.elements.len() - 1);
        }

        return true;
    }

    pub fn upsert(&mut self, element: GenericElement<OW>) -> (usize, bool) {
        // boundary
        if self.elements.is_empty() {
            self.elements.push(element);

            // record the postion of inserted index, and the operation is insert.
            return (0, true);
        }

        // sequential insert
        if let Some(last_element) = self.elements.last() {
            if last_element.row_id() < element.row_id() {
                self.elements.push(element);

                // record the postion of inserted index, and the operation is insert.
                return (self.elements.len() - 1, true);
            } else if last_element.row_id() == element.row_id() {
                let last_element: &mut GenericElement<OW> = self.elements.last_mut().unwrap();
                last_element.update_weight(element.weight());
                if self.element_type == ElementType::EXTENDED {
                    last_element.update_max_next_weight(element.max_next_weight());
                }
                // record the postion of updated index, and the operation is update.
                return (self.elements.len() - 1, false);
            }
        }

        // binary search to insert or update. (performance is worser than sequential upsert)
        debug!("Inserting an element with a smaller row_id than the last element. This may impact performance.");
        let search_result = self.elements.binary_search_by_key(&element.row_id(), |e| e.row_id());
        match search_result {
            Ok(found_idx) => {
                let found_element: &mut GenericElement<OW> = &mut self.elements[found_idx];
                found_element.update_weight(element.weight());
                if self.element_type == ElementType::EXTENDED {
                    found_element.update_max_next_weight(element.max_next_weight());
                }
                // rectord the postion of updated element.
                return (found_idx, false);
            }
            Err(insert_idx) => {
                self.elements.insert(insert_idx, element);

                // record the position of inserted element.
                return (insert_idx, true);
            }
        }
    }

    pub fn upsert_with_propagate(&mut self, element: GenericElement<OW>) -> bool {
        let (upserted_idx, is_insert_operation) = self.upsert(element);
        if self.element_type == ElementType::SIMPLE {
            return is_insert_operation;
        }
        if upserted_idx == self.elements.len() - 1 {
            self.propagate_max_next_weight(upserted_idx);
        } else {
            self.propagate_max_next_weight(upserted_idx + 1);
        }
        return is_insert_operation;
    }

    /// Maintain all elements before element in postion `index`
    fn propagate_max_next_weight(&mut self, index: usize) {
        // boundary
        if self.element_type == ElementType::SIMPLE {
            return;
        }

        // used element at `index` as the starting point
        let cur_element = self.elements.get(index).unwrap_or_else(||{
            let error_msg = format!(
                "index:{} overflow when executing `propagate_max_next_weight` for [`PostingList`], posting length is {}", 
                index,
                self.len()
            );
            error!("{}", error_msg);
            panic!("{}", error_msg);
        });
        let mut max_next_weight: OW = cur_element.weight().max(cur_element.max_next_weight());

        for element in self.elements.iter_mut().take(index).rev() {
            element.update_max_next_weight(max_next_weight);
            max_next_weight = max_next_weight.max(element.weight());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{
        ElementRead, ElementType, ElementWrite, ExtendedElement, PostingListBuilder,
    };

    #[test]
    fn test_delete() {
        let mut posting = PostingListBuilder::<f32, f32>::build_from(
            vec![(1, 10.0), (2, 20.0), (3, 30.0)],
            ElementType::EXTENDED,
        );

        assert_eq!(posting.delete(2), (1, true)); // Delete middle element
        assert_eq!(posting.len(), 2);

        assert!(posting.get_ref(0).row_id() == 1 && posting.get_ref(1).row_id() == 3);

        assert_eq!(posting.delete(3), (1, true)); // Delete last element
        assert_eq!(posting.len(), 1);
        assert_eq!(posting.get_ref(0).row_id(), 1);

        assert_eq!(posting.delete(1), (0, true)); // Delete first element
        assert_eq!(posting.len(), 0);

        assert_eq!(posting.delete(4), (0, false)); // Try deleting non-existing element
        assert_eq!(posting.len(), 0);
    }

    #[test]
    fn test_delete_with_propagate() {
        let mut posting = PostingListBuilder::<f32, f32>::build_from(
            vec![(1, 10.0), (2, 20.0), (3, 30.0)],
            ElementType::EXTENDED,
        );
        posting.delete_with_propagate(2);
        assert_eq!(posting.len(), 2);
        assert!(posting.get_ref(0).max_next_weight() == 30.0);
        assert!(posting.get_ref(1).max_next_weight() == f32::NEG_INFINITY);
    }

    #[test]
    fn test_upsert() {
        let mut posting = PostingListBuilder::<f32, f32>::build_from(vec![], ElementType::EXTENDED);
        assert_eq!(posting.upsert(ExtendedElement::new(1, 10.0).into()), (0, true)); // Insert first element
        assert_eq!(posting.upsert(ExtendedElement::new(2, 20.0).into()), (1, true)); // Insert second element

        // Update existing element
        assert_eq!(posting.upsert(ExtendedElement::new(2, 25.0).into()), (1, false));
        assert_eq!(posting.get_ref(1).weight(), 25.0);
    }

    #[test]
    fn test_upsert_with_propagate() {
        let mut list = PostingListBuilder::<f32, f32>::build_from(vec![], ElementType::EXTENDED);
        assert_eq!(list.upsert_with_propagate(ExtendedElement::new(0, 10.0).into()), true);
        assert_eq!(list.upsert_with_propagate(ExtendedElement::new(1, 20.0).into()), true);
        assert_eq!(list.upsert_with_propagate(ExtendedElement::new(2, 50.0).into()), true);
        assert_eq!(list.upsert_with_propagate(ExtendedElement::new(3, 30.0).into()), true);
        assert_eq!(list.upsert_with_propagate(ExtendedElement::new(4, 40.0).into()), true);
        assert_eq!(list.upsert_with_propagate(ExtendedElement::new(4, 42.0).into()), false);
        assert_eq!(list.upsert_with_propagate(ExtendedElement::new(1, 22.0).into()), false);

        // Check max_next_weight propagation
        assert_eq!(list.get_ref(0).max_next_weight(), 50.0);
        assert_eq!(list.get_ref(2).max_next_weight(), 42.0);
    }
}
