use itertools::Itertools;
use log::error;
use std::{marker::PhantomData, mem::size_of};
use typed_builder::TypedBuilder;

use super::PostingList;
use crate::{
    core::{
        DimWeight, ElementRead, ElementType, ElementWrite, ExtendedElement, GenericElement,
        QuantizedParam, QuantizedWeight, SimpleElement, WeightType, DEFAULT_MAX_NEXT_WEIGHT,
    },
    RowId,
};

#[derive(TypedBuilder)]
pub struct PostingListBuilder<OW: QuantizedWeight, TW: QuantizedWeight> {
    /// [`PostingListBuilder`] will operate inner [`PostingList`]
    #[builder(default=PostingList::<OW>::new(ElementType::SIMPLE))]
    posting: PostingList<OW>,

    /// Element type in [`PostingList`]
    #[builder(default = ElementType::SIMPLE)]
    element_type: ElementType,

    /// Whether need quantize weight in [`PostingList`]
    #[builder(default = false)]
    need_quantized: bool,

    /// This switch is supported when the element type is [`EXTENDED_ELEMENT_TYPE`].
    #[builder(default = false)]
    propagate_while_upserting: bool,

    /// Whether need sort the whole [`PostingList`] when finally build.
    #[builder(default = false)]
    finally_sort: bool,

    /// This switch is supported when the element type is [`EXTENDED_ELEMENT_TYPE`].
    /// It is conflict with switcher [`propagate_while_upserting`]
    #[builder(default = false)]
    finally_propagate: bool,

    pub(super) _phantom_tw: PhantomData<TW>,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> Default for PostingListBuilder<OW, TW> {
    fn default() -> Self {
        Self::new(ElementType::SIMPLE, false, false)
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> PostingListBuilder<OW, TW> {
    pub fn new(
        element_type: ElementType,
        finally_sort: bool,
        propagate_while_upserting: bool,
    ) -> Self {
        // If we need quantize weight.
        let need_quantized =
            TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if !need_quantized {
            assert_eq!(TW::weight_type(), OW::weight_type());
        }

        // only simple element support quantized.
        // quantize extended element will lead max_next_weight nonsense.
        if need_quantized && element_type == ElementType::EXTENDED {
            let error_msg = format!("extended element not supported be quantized.");
            error!("{}", error_msg);
            panic!("{}", error_msg);
        }

        Self::builder()
            .posting(PostingList::<OW>::new(element_type))
            .element_type(element_type)
            .need_quantized(need_quantized)
            .propagate_while_upserting(
                element_type == ElementType::EXTENDED && propagate_while_upserting,
            )
            .finally_sort(finally_sort)
            .finally_propagate(element_type == ElementType::EXTENDED && !propagate_while_upserting)
            ._phantom_tw(PhantomData)
            .build()
    }

    pub fn build_from(
        records: Vec<(RowId, DimWeight)>,
        element_type: ElementType,
    ) -> PostingList<OW> {
        let mut posting_list_builder: PostingListBuilder<OW, OW> =
            PostingListBuilder::<OW, OW>::new(element_type, true, false);
        for (row_id, weight) in records {
            posting_list_builder.add(row_id, weight);
        }

        posting_list_builder.build().0
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> PostingListBuilder<OW, TW> {
    /// ## brief
    /// add a new Element to the posting list.
    /// ## return
    /// bool: `ture` means the `insert` operation, `false` means `update`.
    pub fn add(&mut self, row_id: RowId, weight: DimWeight) -> bool {
        let generic_element: GenericElement<OW> = match self.element_type {
            ElementType::SIMPLE => SimpleElement::<OW>::new(row_id, weight).into(),
            ElementType::EXTENDED => ExtendedElement::<OW>::new(row_id, weight).into(),
            _ => panic!("Not supported element type, this panic should not happen."),
        };

        if self.propagate_while_upserting {
            self.posting.upsert_with_propagate(generic_element)
        } else {
            self.posting.upsert(generic_element).1
        }
    }

    /// return actual and inner memory usage
    pub fn memory_usage(&self) -> (usize, usize) {
        let actual_memory_usage = self.posting.len() * size_of::<GenericElement<OW>>();
        let inner_memory_usage = match self.element_type {
            ElementType::SIMPLE => self.posting.len() * size_of::<SimpleElement<OW>>(),
            ElementType::EXTENDED => self.posting.len() * size_of::<ExtendedElement<OW>>(),
            _ => panic!("Not supported element type, this panic should not happen."),
        };
        (actual_memory_usage, inner_memory_usage)
    }

    fn execute_finally_propagate(&mut self) -> Option<QuantizedParam> {
        // boundary
        assert!(self.element_type == ElementType::EXTENDED);

        if self.posting.elements.len() == 0 && self.need_quantized {
            return Some(QuantizedParam::default());
        }

        let mut max_next_weight: OW = OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT);
        let mut min_weight = self.posting.elements.last().unwrap().weight();
        let mut max_weight = min_weight.clone();

        // reverse iter, update max_next_weight for element_ex.
        for element in self.posting.elements.iter_mut().rev() {
            element.update_max_next_weight(max_next_weight);
            max_next_weight = max_next_weight.max(element.weight());

            if self.need_quantized {
                min_weight = OW::min(min_weight, element.weight());
                max_weight = OW::max(max_weight, element.weight());
            }
        }
        if self.need_quantized {
            Some(OW::gen_quantized_param(min_weight, max_weight))
        } else {
            None
        }
    }

    fn quantize_posting(self, quantized_param: Option<QuantizedParam>) -> PostingList<TW> {
        // boundary
        if self.need_quantized {
            assert!(quantized_param.is_some())
        } else {
            assert!(quantized_param.is_none())
        }

        if self.need_quantized && quantized_param.is_some() {
            let mut quantized_posting_list: PostingList<TW> =
                PostingList::<TW>::new(self.element_type);
            for element in self.posting.elements {
                let quantized_element =
                    element.quantized_with_param::<TW>(quantized_param.unwrap());
                quantized_posting_list.elements.push(quantized_element);
            }
            return quantized_posting_list;
        } else {
            assert_eq!(TW::weight_type(), OW::weight_type());
            let quantized_posting_list: PostingList<TW> =
                unsafe { std::mem::transmute(self.posting) };
            return quantized_posting_list;
        }
    }

    pub fn build(mut self) -> (PostingList<TW>, Option<QuantizedParam>) {
        if self.finally_sort {
            self.posting.elements.sort_unstable_by_key(|e| e.row_id());
        }

        #[cfg(debug_assertions)]
        {
            if let Some(res) =
                self.posting.elements.windows(2).find(|e| e[0].row_id() >= e[1].row_id())
            {
                let error_msg = format!("Duplicated row_id, or posting is not sorted by row_id correctly, left_row_id: {:?}, right_row_id: {:?}.", res[0], res[1]);
                error!("{}", error_msg);
                panic!("{}", error_msg);
            }
        }

        let mut quantized_param: Option<QuantizedParam> = None;

        if self.finally_propagate {
            // We should ensure that only extended type can execute weight propagate.
            assert_eq!(self.element_type, ElementType::EXTENDED);

            quantized_param = self.execute_finally_propagate();
        } else {
            if self.need_quantized {
                let elements_iter = self.posting.elements.iter().map(|e| e.weight());
                let (min, max) = match elements_iter.minmax() {
                    itertools::MinMaxResult::NoElements => (OW::MINIMUM(), OW::MINIMUM()),
                    itertools::MinMaxResult::OneElement(e) => (e, e),
                    itertools::MinMaxResult::MinMax(min, max) => (min, max),
                };
                quantized_param = Some(OW::gen_quantized_param(min, max));
            }
        }

        // quantized or convert posting.
        (self.quantize_posting(quantized_param), quantized_param)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{ElementRead, ElementWrite, ExtendedElement, GenericElement};

    #[test]
    fn test_sort_unstable_by() {
        let mut elements: Vec<GenericElement<f32>> = vec![
            ExtendedElement::<f32>::new(2, 0.9).into(),
            ExtendedElement::<f32>::new(1, 1.2).into(),
            ExtendedElement::<f32>::new(3, 0.2).into(),
        ];
        elements.sort_unstable_by_key(|e| e.row_id());
        println!("{:?}", elements);
    }

    #[test]
    fn test_deref() {
        let x = 8;
        let x_r = &x;
        let x_rr = &x_r;
        println!("{}", x_r);
        println!("{}", x_rr);
    }
}
