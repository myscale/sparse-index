use std::mem::size_of;

use crate::core::{ElementRead, ElementType, ElementWrite, ExtendedElement, GenericElement, QuantizedWeight, SimpleElement, DEFAULT_MAX_NEXT_WEIGHT};
use crate::RowId;
use log::{debug, error};

#[derive(Debug, Clone, PartialEq)]
pub struct PostingList<OW: QuantizedWeight> {
    pub elements: Vec<GenericElement<OW>>,
    pub element_type: ElementType,
}

impl<OW: QuantizedWeight> std::fmt::Display for PostingList<OW> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Posting[{}]{:?}", self.element_type, self.elements)
    }
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
    #[allow(unused)]
    pub fn get_ref(&self, idx: usize) -> Option<&GenericElement<OW>> {
        self.elements.get(idx)
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

    #[allow(unused)]
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

    #[allow(unused)]
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
        let cur_element = self.elements.get(index).unwrap_or_else(|| {
            let error_msg = format!("index:{} overflow when executing `propagate_max_next_weight` for [`PostingList`], posting length is {}", index, self.len());
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
    use core::f32;

    use crate::{
        core::{ElementType, ExtendedElement, GenericElement, PostingListBuilder, QuantizedWeight, DEFAULT_MAX_NEXT_WEIGHT},
        RowId,
    };

    use super::PostingList;

    fn create_extended_posting<W: QuantizedWeight>(elements: Vec<(RowId, W, W)>) -> PostingList<W> {
        let elements: Vec<GenericElement<W>> =
            elements.into_iter().map(|(row_id, weight, max_next_weight)| ExtendedElement { row_id, weight, max_next_weight }.into()).collect::<Vec<_>>();
        PostingList { elements, element_type: ElementType::EXTENDED }
    }

    #[test]
    fn test_posting_delete() {
        let mut posting = PostingListBuilder::<f32, f32>::build_from(
            vec![(1, 100.0), (2, 90.0), (3, 80.0), (4, 70.0), (5, 60.0), (6, 50.0), (7, 40.0), (8, 30.0), (9, 20.0), (10, 10.0)],
            ElementType::EXTENDED,
        )
        .expect("");
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        assert_eq!(
            posting,
            create_extended_posting::<f32>(vec![
                (1, 100.0, 90.0),
                (2, 90.0, 80.0),
                (3, 80.0, 70.0),
                (4, 70.0, 60.0),
                (5, 60.0, 50.0),
                (6, 50.0, 40.0),
                (7, 40.0, 30.0),
                (8, 30.0, 20.0),
                (9, 20.0, 10.0),
                (10, 10.0, m),
            ])
        );

        // Delete middle element in posting.
        assert_eq!(posting.delete(4), (3, true));
        assert_eq!(posting.delete(4), (0, false));

        // Delete first and last element in posting.
        assert_eq!(posting.delete(10), (8, true));
        assert_eq!(posting.delete(1), (0, true));
        assert!(posting.get_ref(8).is_none());

        assert_eq!(
            posting,
            create_extended_posting::<f32>(vec![(2, 90.0, 80.0), (3, 80.0, 70.0), (5, 60.0, 50.0), (6, 50.0, 40.0), (7, 40.0, 30.0), (8, 30.0, 20.0), (9, 20.0, m),])
        );
    }

    #[test]
    fn test_posting_delete_with_propagate() {
        let mut posting = PostingListBuilder::<f32, f32>::build_from(
            vec![(1, 100.0), (2, 90.0), (3, 80.0), (4, 70.0), (5, 60.0), (6, 50.0), (7, 40.0), (8, 30.0), (9, 20.0), (10, 10.0)],
            ElementType::EXTENDED,
        )
        .expect("");
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        assert_eq!(
            posting,
            create_extended_posting::<f32>(vec![
                (1, 100.0, 90.0),
                (2, 90.0, 80.0),
                (3, 80.0, 70.0),
                (4, 70.0, 60.0),
                (5, 60.0, 50.0),
                (6, 50.0, 40.0),
                (7, 40.0, 30.0),
                (8, 30.0, 20.0),
                (9, 20.0, 10.0),
                (10, 10.0, m),
            ])
        );

        // Delete middle element in posting.
        assert_eq!(posting.delete_with_propagate(4), true);
        assert_eq!(posting.delete_with_propagate(4), false);

        // Delete last and first element in posting.
        assert_eq!(posting.delete_with_propagate(10), true);
        assert_eq!(posting.delete_with_propagate(1), true);
        assert!(posting.get_ref(8).is_none());

        assert_eq!(
            posting,
            create_extended_posting::<f32>(vec![(2, 90.0, 80.0), (3, 80.0, 60.0), (5, 60.0, 50.0), (6, 50.0, 40.0), (7, 40.0, 30.0), (8, 30.0, 20.0), (9, 20.0, m),])
        );
    }

    #[test]
    fn test_posting_upsert() {
        let mut posting = PostingListBuilder::<f32, f32>::build_from(vec![], ElementType::EXTENDED).expect("");
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        // Sequence insert
        assert_eq!(posting.upsert(ExtendedElement::new(2, 40.0).into()), (0, true));
        assert_eq!(posting.upsert(ExtendedElement::new(3, 30.0).into()), (1, true));
        assert_eq!(posting.upsert(ExtendedElement::new(4, 50.0).into()), (2, true));
        assert_eq!(posting.upsert(ExtendedElement::new(5, 20.0).into()), (3, true));
        assert_eq!(posting.upsert(ExtendedElement::new(7, 50.0).into()), (4, true));
        assert_eq!(posting.upsert(ExtendedElement::new(9, 10.0).into()), (5, true));
        assert_eq!(posting, create_extended_posting::<f32>(vec![(2, 40.0, m), (3, 30.0, m), (4, 50.0, m), (5, 20.0, m), (7, 50.0, m), (9, 10.0, m)]));

        // Update existing element
        assert_eq!(posting.upsert(ExtendedElement::new(2, 45.0).into()), (0, false));
        assert_eq!(posting.upsert(ExtendedElement::new(5, 25.0).into()), (3, false));
        assert_eq!(posting.upsert(ExtendedElement::new(7, 55.0).into()), (4, false));
        assert_eq!(posting.upsert(ExtendedElement::new(9, 15.0).into()), (5, false));
        assert_eq!(posting, create_extended_posting::<f32>(vec![(2, 45.0, m), (3, 30.0, m), (4, 50.0, m), (5, 25.0, m), (7, 55.0, m), (9, 15.0, m)]));

        // Unordered insert
        assert_eq!(posting.upsert(ExtendedElement::new(1, 20.0).into()), (0, true));
        assert_eq!(posting.upsert(ExtendedElement::new(6, 35.0).into()), (5, true));
        assert_eq!(posting, create_extended_posting::<f32>(vec![(1, 20.0, m), (2, 45.0, m), (3, 30.0, m), (4, 50.0, m), (5, 25.0, m), (6, 35.0, m), (7, 55.0, m), (9, 15.0, m)]));
    }

    #[test]
    fn test_posting_upsert_with_propagate() {
        let mut posting = PostingListBuilder::<f32, f32>::build_from(vec![], ElementType::EXTENDED).expect("");
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        // Sequence insert
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(2, 40.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(3, 30.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(4, 50.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(5, 20.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(7, 50.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(9, 10.0).into()), true);
        assert_eq!(posting, create_extended_posting::<f32>(vec![(2, 40.0, 50.0), (3, 30.0, 50.0), (4, 50.0, 50.0), (5, 20.0, 50.0), (7, 50.0, 10.0), (9, 10.0, m)]));

        // Update existing element
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(2, 45.0).into()), false);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(5, 25.0).into()), false);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(7, 55.0).into()), false);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(9, 15.0).into()), false);
        assert_eq!(posting, create_extended_posting::<f32>(vec![(2, 45.0, 55.0), (3, 30.0, 55.0), (4, 50.0, 55.0), (5, 25.0, 55.0), (7, 55.0, 15.0), (9, 15.0, m)]));

        // Unordered insert
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(1, 20.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(6, 80.0).into()), true);
        assert_eq!(
            posting,
            create_extended_posting::<f32>(vec![
                (1, 20.0, 80.0),
                (2, 45.0, 80.0),
                (3, 30.0, 80.0),
                (4, 50.0, 80.0),
                (5, 25.0, 80.0),
                (6, 80.0, 55.0),
                (7, 55.0, 15.0),
                (9, 15.0, m)
            ])
        );
    }
}
