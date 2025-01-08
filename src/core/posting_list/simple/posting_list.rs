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
    use std::collections::HashSet;

    use crate::{
        core::{ElementType, ExtendedElement, GenericElement, PostingListBuilder, QuantizedWeight, SimpleElement, WeightType, DEFAULT_MAX_NEXT_WEIGHT},
        RowId,
    };
    use super::super::test::{build_simple_posting, expect_posting_with_simple_elements, generate_random_int, expect_posting_with_extended_elements};

    use super::PostingList;

    fn scrape_posting_from_simple<TW: QuantizedWeight>(elements: Vec<(RowId, TW)>) -> PostingList<TW> {
        let elements: Vec<GenericElement<TW>> =
            elements.into_iter().map(|(row_id, weight)| SimpleElement { row_id, weight }.into()).collect::<Vec<_>>();
        PostingList { elements, element_type: ElementType::SIMPLE }
    }

    fn scrape_posting_from_extended<TW: QuantizedWeight>(elements: Vec<(RowId, TW, TW)>) -> PostingList<TW> {
        let elements: Vec<GenericElement<TW>> =
            elements.into_iter().map(|(row_id, weight, max_next_weight)| ExtendedElement { row_id, weight, max_next_weight }.into()).collect::<Vec<_>>();
        PostingList { elements, element_type: ElementType::EXTENDED }
    }

    // TODO Should be unit test, not integration test.
    fn inner_test_posting_delete<OW: QuantizedWeight, TW: QuantizedWeight>(element_type: ElementType, elements_count: usize, delete_with_propagate: bool) {
        let use_quantized = OW::weight_type()!=TW::weight_type() && TW::weight_type()==WeightType::WeightU8;

        // Use [`PostingBuilder`] to build a random Posting.
        let (mut output_posting, output_param, mut raw_elements, _) = build_simple_posting::<OW, TW>(element_type, elements_count, 1, true);

        // Check the posting is expected.
        let (expected_posting, expected_param) = match element_type {
            ElementType::SIMPLE => expect_posting_with_simple_elements::<OW, TW>(raw_elements.clone()),
            ElementType::EXTENDED => {
                // We need propagate `max_next_weight`, so we have to use PostingListBuilder.
                let mut builder = PostingListBuilder::<OW, TW>::new(element_type, false).expect("msg");
                for (row_id, weight) in raw_elements.iter() {
                    builder.add(*row_id, *weight);
                }
                builder.build().expect("msg")
            },
        };
        assert_eq!(expected_posting, output_posting);
        assert_eq!(expected_param, output_param);
        assert_eq!(output_posting.len(), elements_count);

        // Generate a group of row_ids which need to be deleted.
        let row_ids_del_set = (0..elements_count/2).into_iter().map(|_| generate_random_int(1, elements_count as u32)).collect::<HashSet<u32>>();
        let row_ids_del = row_ids_del_set.clone().into_iter().collect::<Vec<u32>>();

        // Remove these row_ids from posting.
        for row_id_del in row_ids_del {
            match delete_with_propagate {
                true => {
                    let del_status = output_posting.delete_with_propagate(row_id_del);
                    assert!(del_status);
                },
                false => {
                    let (_, del_status) = output_posting.delete(row_id_del);
                    assert!(del_status);
                },
            }
        }
        assert_eq!(output_posting.len(), elements_count - row_ids_del_set.len());
        // Remove elements in `raw_elements`.
        raw_elements.retain(|(row_id, _)| !row_ids_del_set.contains(row_id));

        // Remove these row_ids from elements.
        match element_type {
            ElementType::SIMPLE => {
                let (expected_posting_del, expected_param_del) = expect_posting_with_simple_elements::<OW, TW>(raw_elements.clone());
                match use_quantized {
                    true => {
                        if output_param == expected_param_del {
                            assert_eq!(output_posting, expected_posting_del);
                        } else {
                            assert_ne!(output_posting, expected_posting_del);
                        }
                    },
                    false => {
                        assert_eq!(output_posting, expected_posting_del);
                    },
                }
            },
            ElementType::EXTENDED => {
                // We need propagate `max_next_weight`, so we have to use PostingListBuilder.
                let mut builder = PostingListBuilder::<OW, TW>::new(element_type, false).expect("msg");
                for (row_id, weight) in raw_elements {
                    builder.add(row_id, weight);
                }
                let (expected_posting_del, expected_param_del) = builder.build().expect("msg");
                match use_quantized {
                    true => {
                        if output_param == expected_param_del {
                            assert_eq!(output_posting, expected_posting_del);
                        } else {
                            assert_ne!(output_posting, expected_posting_del);
                        }
                    },
                    false => {
                        assert_eq!(output_posting, expected_posting_del);
                    },
                }    
            },
        }
    }


    #[test]
    fn test_posting_delete() {
        // Boundary Test.
        inner_test_posting_delete::<f32, f32>(ElementType::SIMPLE, 0, false);
        inner_test_posting_delete::<f32, u8>(ElementType::SIMPLE, 0, false);
        inner_test_posting_delete::<f32, f32>(ElementType::SIMPLE, 1, false);
        inner_test_posting_delete::<f32, u8>(ElementType::SIMPLE, 1, false);
        inner_test_posting_delete::<f32, f32>(ElementType::EXTENDED, 0, false);
        inner_test_posting_delete::<f32, f32>(ElementType::EXTENDED, 0, true);
        inner_test_posting_delete::<f32, f32>(ElementType::EXTENDED, 1, false);
        inner_test_posting_delete::<f32, f32>(ElementType::EXTENDED, 1, true);

        // Normal Test.
        inner_test_posting_delete::<f32, f32>(ElementType::SIMPLE, 20096, false);
        inner_test_posting_delete::<f32, u8>(ElementType::SIMPLE, 20096, false);
        inner_test_posting_delete::<half::f16, half::f16>(ElementType::SIMPLE, 20096, false);
        inner_test_posting_delete::<half::f16, u8>(ElementType::SIMPLE, 20096, false);
        inner_test_posting_delete::<u8, u8>(ElementType::SIMPLE, 20096, false);

        inner_test_posting_delete::<f32, f32>(ElementType::EXTENDED, 10, true);
        inner_test_posting_delete::<half::f16, half::f16>(ElementType::EXTENDED, 20096, true);
        inner_test_posting_delete::<u8, u8>(ElementType::EXTENDED, 20096, true);
    }


    fn inner_test_posting_upsert<OW: QuantizedWeight>() {
        let mut posting = PostingList::<OW>::new(ElementType::EXTENDED);
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        // Sequence insert
        assert_eq!(posting.upsert(ExtendedElement::new(2, 40.0).into()), (0, true));
        assert_eq!(posting.upsert(ExtendedElement::new(3, 30.0123456).into()), (1, true));
        assert_eq!(posting.upsert(ExtendedElement::new(4, 50.0).into()), (2, true));
        assert_eq!(posting.upsert(ExtendedElement::new(5, 20.0).into()), (3, true));
        assert_eq!(posting.upsert(ExtendedElement::new(7, 50.0).into()), (4, true));
        assert_eq!(posting.upsert(ExtendedElement::new(9, 10.0).into()), (5, true));
        assert_eq!(posting, expect_posting_with_extended_elements::<OW, OW>(vec![(2, 40.0, m), (3, 30.0123456, m), (4, 50.0, m), (5, 20.0, m), (7, 50.0, m), (9, 10.0, m)]).0);

        // Update existing element
        assert_eq!(posting.upsert(ExtendedElement::new(2, 45.0).into()), (0, false));
        assert_eq!(posting.upsert(ExtendedElement::new(5, 25.0).into()), (3, false));
        assert_eq!(posting.upsert(ExtendedElement::new(7, 55.0).into()), (4, false));
        assert_eq!(posting.upsert(ExtendedElement::new(9, 15.0).into()), (5, false));
        assert_eq!(posting, expect_posting_with_extended_elements::<OW,OW>(vec![(2, 45.0, m), (3, 30.0123456, m), (4, 50.0, m), (5, 25.0, m), (7, 55.0, m), (9, 15.0, m)]).0);

        // Unordered insert
        assert_eq!(posting.upsert(ExtendedElement::new(1, 20.0).into()), (0, true));
        assert_eq!(posting.upsert(ExtendedElement::new(6, 35.0).into()), (5, true));
        assert_eq!(posting, expect_posting_with_extended_elements::<OW,OW>(vec![(1, 20.0, m), (2, 45.0, m), (3, 30.0123456, m), (4, 50.0, m), (5, 25.0, m), (6, 35.0, m), (7, 55.0, m), (9, 15.0, m)]).0);
    }


    #[test]
    fn test_posting_upsert() {
        inner_test_posting_upsert::<f32>();
        inner_test_posting_upsert::<half::f16>();
        inner_test_posting_upsert::<u8>();

    }

    fn inner_test_posting_upsert_with_propagate<OW: QuantizedWeight>() {
        let mut posting = PostingList::<OW>::new(ElementType::EXTENDED);
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        // Sequence insert
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(2, 40.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(3, 30.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(4, 50.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(5, 20.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(7, 50.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(9, 10.0).into()), true);
        assert_eq!(posting, expect_posting_with_extended_elements::<OW,OW>(vec![(2, 40.0, 50.0), (3, 30.0, 50.0), (4, 50.0, 50.0), (5, 20.0, 50.0), (7, 50.0, 10.0), (9, 10.0, m)]).0);

        // Update existing element
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(2, 45.0).into()), false);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(5, 25.0).into()), false);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(7, 55.0).into()), false);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(9, 15.0).into()), false);
        assert_eq!(posting, expect_posting_with_extended_elements::<OW,OW>(vec![(2, 45.0, 55.0), (3, 30.0, 55.0), (4, 50.0, 55.0), (5, 25.0, 55.0), (7, 55.0, 15.0), (9, 15.0, m)]).0);

        // Unordered insert
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(1, 20.0).into()), true);
        assert_eq!(posting.upsert_with_propagate(ExtendedElement::new(6, 80.0).into()), true);
        assert_eq!(
            posting,
            expect_posting_with_extended_elements::<OW, OW>(vec![
                (1, 20.0, 80.0),
                (2, 45.0, 80.0),
                (3, 30.0, 80.0),
                (4, 50.0, 80.0),
                (5, 25.0, 80.0),
                (6, 80.0, 55.0),
                (7, 55.0, 15.0),
                (9, 15.0, m)
            ]).0
        );
    }

    #[test]
    fn test_posting_upsert_with_propagate() {
        inner_test_posting_upsert_with_propagate::<f32>();
        inner_test_posting_upsert_with_propagate::<half::f16>();
        inner_test_posting_upsert_with_propagate::<u8>();
    }

}
