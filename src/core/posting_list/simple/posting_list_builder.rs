use itertools::Itertools;
use log::error;
use std::{marker::PhantomData, mem::size_of};
use typed_builder::TypedBuilder;

use super::PostingList;
use crate::{
    core::{
        DimWeight, ElementRead, ElementType, ElementWrite, ExtendedElement, GenericElement, PostingListError, QuantizedParam, QuantizedWeight, SimpleElement, WeightType,
        DEFAULT_MAX_NEXT_WEIGHT,
    },
    RowId,
};

#[derive(TypedBuilder, Clone)]
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

    /// It is conflict with switcher [`propagate_while_upserting`]
    #[builder(default = false)]
    finally_propagate: bool,

    pub(super) _phantom_tw: PhantomData<TW>,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> Default for PostingListBuilder<OW, TW> {
    fn default() -> Self {
        Self::new(ElementType::SIMPLE, false).unwrap()
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> PostingListBuilder<OW, TW> {
    // pub fn new(element_type: ElementType, finally_sort: bool, propagate_while_upserting: bool) -> Result<Self, PostingListError> {
    pub fn new(element_type: ElementType, propagate_while_upserting: bool) -> Result<Self, PostingListError> {
        // If we need quantize weight.
        let need_quantized = TW::weight_type() != OW::weight_type() && TW::weight_type() == WeightType::WeightU8;
        if !need_quantized && TW::weight_type() != OW::weight_type() {
            let error_msg = "[PostingListBuilder] WeightType should keep same, while quantized is disabled.";
            error!("{}", error_msg);
            return Err(PostingListError::InvalidParameter(error_msg.to_string()));
        }

        // Quantize ExtendedElement will lead `max_next_weight` nonsense.
        if need_quantized && element_type == ElementType::EXTENDED {
            let error_msg = "[PostingListBuilder] ExtendedElement doesn't support to be quantized.";
            error!("{}", error_msg);
            return Err(PostingListError::InvalidParameter(error_msg.to_string()));
        }

        Ok(Self::builder()
            .posting(PostingList::<OW>::new(element_type))
            .element_type(element_type)
            .need_quantized(need_quantized)
            .propagate_while_upserting(element_type == ElementType::EXTENDED && propagate_while_upserting)
            // .finally_sort(finally_sort)
            .finally_propagate(element_type == ElementType::EXTENDED && !propagate_while_upserting)
            ._phantom_tw(PhantomData)
            .build())
    }

    // #[cfg(test)]
    // pub fn build_from(records: Vec<(RowId, DimWeight)>, element_type: ElementType) -> Result<PostingList<OW>, PostingListError> {
    //     let mut posting_list_builder: PostingListBuilder<OW, OW> = PostingListBuilder::<OW, OW>::new(element_type, false)?;
    //     for (row_id, weight) in records {
    //         posting_list_builder.add(row_id, weight);
    //     }

    //     Ok(posting_list_builder.build()?.0)
    // }
    #[cfg(test)]
    pub fn update_inner_posting(&mut self, posting: PostingList<OW>) {
        self.posting = posting
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
        };
        (actual_memory_usage, inner_memory_usage)
    }

    fn execute_finally_propagate(&mut self) -> Result<(), PostingListError> {
        // Boundary
        if self.element_type != ElementType::EXTENDED {
            return Err(PostingListError::InvalidParameter("Only ExtendedElement needs execute `max_next_weight` propagate".to_string()));
        }

        if self.need_quantized {
            return Err(PostingListError::InvalidParameter("ExtendedElement can't be quantized.".to_string()));
        }

        if self.posting.elements.len() == 0 {
            return Ok(());
        }

        if self.posting.elements.len() == 1 {
            self.posting.elements[0].update_max_next_weight(OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT));
            return Ok(());
        }

        let mut max_next_weight: OW = OW::from_f32(DEFAULT_MAX_NEXT_WEIGHT);

        // reverse iter, update max_next_weight for element_ex.
        for element in self.posting.elements.iter_mut().rev() {
            element.update_max_next_weight(max_next_weight);
            max_next_weight = max_next_weight.max(element.weight());
        }
        Ok(())
    }

    fn quantize_posting(self, quantized_param: Option<QuantizedParam>) -> Result<PostingList<TW>, PostingListError> {
        // Boundary
        if self.need_quantized && quantized_param.is_none() {
            return Err(PostingListError::InvalidParameter("QuantizedParam is none, but PostingListBuilder needs to be quantized!".to_string()));
        }
        if !self.need_quantized && quantized_param.is_some() {
            return Err(PostingListError::InvalidParameter("PostingListBuilder doesn't need to be quantized, but the QuantizedParam is not none".to_string()));
        }

        if self.need_quantized && quantized_param.is_some() {
            // Execute quantize.
            let mut quantized_posting_list: PostingList<TW> = PostingList::<TW>::new(self.element_type);
            for element in self.posting.elements {
                let quantized_element = element.quantize_with_param::<TW>(quantized_param.unwrap());
                quantized_posting_list.elements.push(quantized_element);
            }
            return Ok(quantized_posting_list);
        } else {
            // Don't need quantize the posting, just convert it's type.
            if TW::weight_type() != OW::weight_type() {
                return Err(PostingListError::TypeConvertError(format!("Can't convert from {:?} to {:?}, it's not safe!", OW::weight_type(), TW::weight_type())));
            }
            let quantized_posting_list: PostingList<TW> = unsafe { std::mem::transmute(self.posting) };
            return Ok(quantized_posting_list);
        }
    }

    pub fn build(mut self) -> Result<(PostingList<TW>, Option<QuantizedParam>), PostingListError> {
        #[cfg(debug_assertions)]
        {
            if let Some(res) = self.posting.elements.windows(2).find(|e| e[0].row_id() >= e[1].row_id()) {
                let error_msg = format!("Duplicated row_id, or posting is not sorted by row_id correctly, left_row_id: {:?}, right_row_id: {:?}.", res[0], res[1]);
                error!("{}", error_msg);
                return Err(PostingListError::DuplicatedRowId(error_msg));
            }
        }

        let mut quantized_param: Option<QuantizedParam> = None;

        if self.finally_propagate {
            // We should ensure that only extended type can execute weight propagate.
            let _ = self.execute_finally_propagate()?;
        } else {
            if self.need_quantized {
                let elements_iter = self.posting.elements.iter().map(|e| e.weight());
                let (min, max) = match elements_iter.minmax() {
                    itertools::MinMaxResult::NoElements => (OW::MINIMUM(), OW::MINIMUM()),
                    itertools::MinMaxResult::OneElement(e) => (e, e),
                    itertools::MinMaxResult::MinMax(min, max) => (min, max),
                };
                if min == OW::MINIMUM() && max == OW::MINIMUM() {
                    quantized_param = Some(QuantizedParam::default());
                } else {
                    quantized_param = Some(OW::gen_quantized_param(min, max));
                }
            }
        }

        // quantized or convert posting.
        let quantized_posting = self.quantize_posting(quantized_param)?;
        Ok((quantized_posting, quantized_param))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test::{build_simple_posting_from_elements, expect_posting_with_extended_elements, expect_posting_with_simple_elements};
    use super::*;
    use crate::core::QuantizedWeight;

    // TODO 检查 quantized 之后的参数是否和预期一致
    fn mock_build_elements<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        elements: Vec<(RowId, f32)>,
        propagate_while_upserting: bool,
    ) -> Result<(PostingList<TW>, Option<QuantizedParam>), PostingListError> {
        let mut builder = PostingListBuilder::<OW, TW>::new(element_type, propagate_while_upserting)?;
        for el in elements {
            builder.add(el.0, el.1);
        }
        builder.build()
    }

    fn create_extended_posting<W: QuantizedWeight>(elements: Vec<(RowId, W, W)>) -> PostingList<W> {
        let elements: Vec<GenericElement<W>> =
            elements.into_iter().map(|(row_id, weight, max_next_weight)| ExtendedElement { row_id, weight, max_next_weight }.into()).collect::<Vec<_>>();
        PostingList { elements, element_type: ElementType::EXTENDED }
    }

    // fn create_simple_posting<W: QuantizedWeight>(elements: Vec<(RowId, W)>) -> PostingList<W> {
    //     let elements: Vec<GenericElement<W>> = elements.into_iter().map(|(row_id, weight)| SimpleElement { row_id, weight }.into()).collect::<Vec<_>>();
    //     PostingList { elements, element_type: ElementType::SIMPLE }
    // }

    fn inner_test_new_posting_builder<OW: QuantizedWeight, TW: QuantizedWeight>(element_type: ElementType, propagate_while_upserting: bool) {
        let builder: PostingListBuilder<OW, TW> = PostingListBuilder::<OW, TW>::new(element_type, propagate_while_upserting).unwrap();
        assert_eq!(builder.element_type, element_type);
        assert_eq!(builder.need_quantized, OW::weight_type() != TW::weight_type() && TW::weight_type() == WeightType::WeightU8);
        assert_eq!(builder.propagate_while_upserting, propagate_while_upserting && element_type == ElementType::EXTENDED);
        assert_eq!(builder.finally_propagate, !propagate_while_upserting && element_type == ElementType::EXTENDED);
    }
    #[test]
    fn test_new_posting_builder() {
        // Not quantized.
        inner_test_new_posting_builder::<f32, f32>(ElementType::SIMPLE, true);
        inner_test_new_posting_builder::<f32, f32>(ElementType::SIMPLE, false);
        inner_test_new_posting_builder::<half::f16, half::f16>(ElementType::EXTENDED, true);
        inner_test_new_posting_builder::<half::f16, half::f16>(ElementType::EXTENDED, false);
        inner_test_new_posting_builder::<u8, u8>(ElementType::EXTENDED, false);

        // Quantized.
        inner_test_new_posting_builder::<f32, u8>(ElementType::SIMPLE, true);
        inner_test_new_posting_builder::<half::f16, u8>(ElementType::SIMPLE, false);

        // Invalid.
        assert!(PostingListBuilder::<f32, u8>::new(ElementType::EXTENDED, false).is_err());
        assert!(PostingListBuilder::<half::f16, u8>::new(ElementType::EXTENDED, false).is_err());
    }

    fn inner_test_build_from_simple_elements<OW: QuantizedWeight, TW: QuantizedWeight>(elements: Vec<(u32, f32)>) {
        let (output_posting, output_param) = build_simple_posting_from_elements::<OW, TW>(ElementType::SIMPLE, elements.clone());
        let (expected_posting, expected_param) = expect_posting_with_simple_elements::<OW, TW>(elements.clone());

        assert_eq!(output_posting, expected_posting);
        assert_eq!(output_param, expected_param);
    }

    fn inner_test_build_from_extended_elements<OW: QuantizedWeight, TW: QuantizedWeight>(elements: Vec<(u32, f32, f32)>) {
        let simple_elements = elements.iter().map(|(row_id, weight, _)| (*row_id, *weight)).collect::<Vec<_>>();

        let (output_posting, output_param) = build_simple_posting_from_elements::<OW, TW>(ElementType::EXTENDED, simple_elements.clone());
        let (expected_posting, expected_param) = expect_posting_with_extended_elements::<OW, TW>(elements.clone());

        assert_eq!(output_posting, expected_posting);
        assert_eq!(output_param, expected_param);
    }

    #[test]
    fn test_build_elements() {
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        // For simple element.
        let simple_elements = vec![(6, 70.0), (18, 30.0), (21, 20.0), (17, 10.0), (14, 45.0)];
        // not quantized
        inner_test_build_from_simple_elements::<f32, f32>(simple_elements.clone());
        inner_test_build_from_simple_elements::<half::f16, half::f16>(simple_elements.clone());
        inner_test_build_from_simple_elements::<u8, u8>(simple_elements.clone());
        // quantized
        inner_test_build_from_simple_elements::<f32, u8>(simple_elements.clone());
        inner_test_build_from_simple_elements::<half::f16, u8>(simple_elements.clone());

        // For extended element.
        let extended_elements = vec![(6, 70.0, 45.0), (14, 45.0, 30.0), (17, 10.0, 30.0), (18, 30.0, 20.0), (21, 20.0, m)];
        inner_test_build_from_extended_elements::<f32, f32>(extended_elements.clone());
        inner_test_build_from_extended_elements::<half::f16, half::f16>(extended_elements.clone());
        inner_test_build_from_extended_elements::<u8, u8>(extended_elements.clone());
    }

    #[test]
    fn test_propagate_while_building() {
        let m = DEFAULT_MAX_NEXT_WEIGHT;
        let simple_elements = vec![(6, 70.0), (18, 30.0), (21, 20.0), (17, 10.0), (14, 45.0)];
        let propagated_elements = vec![(6, 70.0, 45.0), (14, 45.0, 30.0), (17, 10.0, 30.0), (18, 30.0, 20.0), (21, 20.0, m)];
        let not_propagated_elements = vec![(6, 70.0, m), (14, 45.0, m), (17, 10.0, m), (18, 30.0, m), (21, 20.0, m)];

        // When upserting, we execute propagating.
        {
            let mut builder = PostingListBuilder::<f32, f32>::new(ElementType::EXTENDED, true).expect("");
            for (row_id, weight) in simple_elements.iter() {
                assert!(builder.add(*row_id, *weight));
            }
            // verify before building.
            assert_eq!(builder.posting, create_extended_posting::<f32>(propagated_elements.clone()));
            // verify after building.
            let posting = builder.build().unwrap().0;
            assert_eq!(posting, create_extended_posting::<f32>(propagated_elements.clone()));
        }
        // When upserting, we not execute propagating.
        {
            let mut builder = PostingListBuilder::<f32, f32>::new(ElementType::EXTENDED, false).expect("");
            for (row_id, weight) in simple_elements.iter() {
                assert!(builder.add(*row_id, *weight));
            }
            // verify before building.
            assert_eq!(builder.posting, create_extended_posting::<f32>(not_propagated_elements.clone()));
            // verify after building.
            let posting = builder.build().unwrap().0;
            assert_eq!(posting, create_extended_posting::<f32>(propagated_elements.clone()));
        }
    }
}
