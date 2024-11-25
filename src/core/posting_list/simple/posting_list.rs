use super::super::traits::PostingElementEx;
use super::PostingListBuilder;
use crate::core::common::types::DimWeight;
use crate::core::{QuantizedWeight, DEFAULT_MAX_NEXT_WEIGHT};
use crate::RowId;
use log::{debug, warn};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct PostingList<OW: QuantizedWeight> {
    pub elements: Vec<PostingElementEx<OW>>,
}

/// PostingList Creater
impl<OW: QuantizedWeight> PostingList<OW> {
    pub fn new() -> Self {
        Self { elements: vec![] }
    }

    /// ## brief
    /// create a PostingListBuilder with `sort` or `propagate`
    pub fn from(records: Vec<(RowId, DimWeight)>) -> PostingList<OW> {
        // TODO  builder 写好之后重构一下
        let mut posting_list_builder: PostingListBuilder<OW, OW> =
            PostingListBuilder::<OW, OW>::new()
                .with_finally_sort(true)
                .with_finally_propagate(true)
                .with_propagate_while_upserting(false);
        for (row_id, weight) in records {
            posting_list_builder.add(row_id, weight);
        }

        posting_list_builder.build().0
    }

    pub fn new_one(row_id: RowId, dim_weight: DimWeight) -> PostingList<OW> {
        PostingList { elements: vec![PostingElementEx::new(row_id, dim_weight)] }
    }
}

impl<OW: QuantizedWeight> PostingList<OW> {
    pub fn get_ref(&self, mut idx: usize) -> &PostingElementEx<OW> {
        if idx >= self.len() {
            warn!("idx:{} overflow when `get_ref` of PostingElementEx. will reset it to end.", idx);
            idx = self.len() - 1;
        }
        return &self.elements[idx];
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// ## brief
    /// Deletes an element corresponding to the specified `row_id` from the Posting.
    ///
    /// ## return
    /// func `delete` returns tuple(usize, bool):
    /// - the first element means the index of deleted element
    /// - when failed to delete(row_id doesn't exist in posting list), the second element is false.
    pub fn delete(&mut self, row_id: RowId) -> (usize, bool) {
        let search_result = self.elements.binary_search_by_key(&row_id, |e| e.row_id);

        match search_result {
            Ok(found_idx) => {
                self.elements.remove(found_idx);

                // Reset the max_next_weight for the last element to the default.
                if let Some(last) = self.elements.last_mut() {
                    last.max_next_weight = OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT);
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

    /// ## brief
    /// Deletes an element by `row_id` and propagates to maintain `max_next_weight` correctly.
    /// ## return
    /// if `row_id` doesn't exist in postingList, it will return `false`.
    pub fn delete_with_propagate(&mut self, row_id: RowId) -> bool {
        let (deleted_idx, success_deleted) = self.delete(row_id);
        if !success_deleted {
            return false;
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

    /// ## brief
    /// Insert or update an element to the Posting, return the inserted or updated position.
    /// ## return
    /// tuple(usize, bool):
    /// - first element record the position for `insert` or `update`
    /// - second element means the operation type, for `insert`, it's `true`, otherwise is `false`.
    pub fn upsert(&mut self, element: PostingElementEx<OW>) -> (usize, bool) {
        // boundary
        if self.elements.is_empty() {
            self.elements.push(element);

            // record the postion of inserted index, and the operation is insert.
            return (0, true);
        }

        // sequential insert
        if let Some(last_element) = self.elements.last() {
            if last_element.row_id < element.row_id {
                self.elements.push(element);

                // record the postion of inserted index, and the operation is insert.
                return (self.elements.len() - 1, true);
            } else if last_element.row_id == element.row_id {
                let last_element: &mut PostingElementEx<OW> = self.elements.last_mut().unwrap();
                last_element.weight = element.weight;
                last_element.max_next_weight = element.max_next_weight;

                // record the postion of updated index, and the operation is update.
                return (self.elements.len() - 1, false);
            }
        }

        // binary search to insert or update. (performance is worser than sequential upsert)
        debug!("Inserting an element with a smaller row_id than the last element. This may impact performance.");
        let search_result = self.elements.binary_search_by_key(&element.row_id, |e| e.row_id);
        match search_result {
            Ok(found_idx) => {
                let found_element: &mut PostingElementEx<OW> = &mut self.elements[found_idx];
                found_element.weight = element.weight;
                found_element.max_next_weight = element.max_next_weight;

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

    /// ## brief
    /// Insert or update an element and propagates to maintain `max_next_weight` correctly.
    /// ## return
    /// bool: means wheather this upsert is `insert`, if it's false, means this `upsert` is `update`.
    pub fn upsert_with_propagate(&mut self, element: PostingElementEx<OW>) -> bool {
        let (upserted_idx, is_insert_operation) = self.upsert(element);
        if upserted_idx == self.elements.len() - 1 {
            self.propagate_max_next_weight(upserted_idx);
        } else {
            self.propagate_max_next_weight(upserted_idx + 1);
        }
        return is_insert_operation;
    }

    /// Maintain all elements before element in postion `index`
    fn propagate_max_next_weight(&mut self, mut index: usize) {
        // boundary
        if index >= self.elements.len() {
            warn!("wrong index, will propagate from bottom element.");
            // reset `index`
            index = self.elements.len() - 1;
        }

        // used element at `index` as the starting point
        let cur_element: &PostingElementEx<OW> = &self.elements[index];
        let mut max_next_weight: OW = cur_element.weight.max(cur_element.max_next_weight);

        for element in self.elements.iter_mut().take(index).rev() {
            element.max_next_weight = max_next_weight;
            max_next_weight = max_next_weight.max(element.weight);
        }
    }
}

// impl<'a, W: QuantizedWeight> PostingListTrait<'a, W> for PostingList<OW> {

// }

#[cfg(test)]
mod tests {
    use crate::core::PostingElementEx;

    use super::PostingList;

    #[test]
    fn test_delete() {
        let mut posting = PostingList::<f32>::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]);

        assert_eq!(posting.delete(2), (1, true)); // Delete middle element
        assert_eq!(posting.len(), 2);
        assert!(posting.get_ref(0).row_id == 1 && posting.get_ref(1).row_id == 3);

        assert_eq!(posting.delete(3), (1, true)); // Delete last element
        assert_eq!(posting.len(), 1);
        assert_eq!(posting.get_ref(0).row_id, 1);

        assert_eq!(posting.delete(1), (0, true)); // Delete first element
        assert_eq!(posting.len(), 0);

        assert_eq!(posting.delete(4), (0, false)); // Try deleting non-existing element
        assert_eq!(posting.len(), 0);
    }

    #[test]
    fn test_delete_with_propagate() {
        let mut posting = PostingList::<f32>::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]);
        posting.delete_with_propagate(2);
        assert_eq!(posting.len(), 2);
        assert!(posting.get_ref(0).max_next_weight == 30.0);
        assert!(posting.get_ref(1).max_next_weight == f32::NEG_INFINITY);
    }

    #[test]
    fn test_upsert() {
        let mut posting = PostingList::<f32>::from(vec![]);
        assert_eq!(posting.upsert(PostingElementEx::new(1, 10.0)), (0, true)); // Insert first element
        assert_eq!(posting.upsert(PostingElementEx::new(2, 20.0)), (1, true)); // Insert second element

        // Update existing element
        assert_eq!(posting.upsert(PostingElementEx::new(2, 25.0)), (1, false));
        assert_eq!(posting.get_ref(1).weight, 25.0);
    }

    #[test]
    fn test_upsert_with_propagate() {
        let mut list = PostingList::<f32>::from(vec![]);
        assert_eq!(list.upsert_with_propagate(PostingElementEx::new(0, 10.0)), true);
        assert_eq!(list.upsert_with_propagate(PostingElementEx::new(1, 20.0)), true);
        assert_eq!(list.upsert_with_propagate(PostingElementEx::new(2, 50.0)), true);
        assert_eq!(list.upsert_with_propagate(PostingElementEx::new(3, 30.0)), true);
        assert_eq!(list.upsert_with_propagate(PostingElementEx::new(4, 40.0)), true);
        assert_eq!(list.upsert_with_propagate(PostingElementEx::new(4, 42.0)), false);
        assert_eq!(list.upsert_with_propagate(PostingElementEx::new(1, 22.0)), false);

        // Check max_next_weight propagation
        assert_eq!(list.get_ref(0).max_next_weight, 50.0);
        assert_eq!(list.get_ref(2).max_next_weight, 42.0);
    }
}
