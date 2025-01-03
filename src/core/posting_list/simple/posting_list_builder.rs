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

    #[cfg(test)]
    pub fn build_from(records: Vec<(RowId, DimWeight)>, element_type: ElementType) -> Result<PostingList<OW>, PostingListError> {
        let mut posting_list_builder: PostingListBuilder<OW, OW> = PostingListBuilder::<OW, OW>::new(element_type, false)?;
        for (row_id, weight) in records {
            posting_list_builder.add(row_id, weight);
        }

        Ok(posting_list_builder.build()?.0)
    }
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
        // if self.finally_sort {
        //     self.posting.elements.sort_unstable_by_key(|e| e.row_id());
        // }

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
                quantized_param = Some(OW::gen_quantized_param(min, max));
            }
        }

        // quantized or convert posting.
        let quantized_posting = self.quantize_posting(quantized_param)?;
        Ok((quantized_posting, quantized_param))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::QuantizedWeight;

    fn mock_build_elements<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        propagate_while_upserting: bool,
    ) -> Result<(PostingList<TW>, Option<QuantizedParam>), PostingListError> {
        let mut builder = PostingListBuilder::<OW, TW>::new(element_type, propagate_while_upserting)?;
        assert_eq!(builder.add(6, 70.0), true);
        assert_eq!(builder.add(14, 50.0), true);
        assert_eq!(builder.add(18, 30.0), true);
        assert_eq!(builder.add(21, 20.0), true);
        assert_eq!(builder.add(17, 10.0), true);
        assert_eq!(builder.add(14, 45.0), false);
        builder.build()
    }

    fn create_extended_posting<W: QuantizedWeight>(elements: Vec<(RowId, W, W)>) -> PostingList<W> {
        let elements: Vec<GenericElement<W>> =
            elements.into_iter().map(|(row_id, weight, max_next_weight)| ExtendedElement { row_id, weight, max_next_weight }.into()).collect::<Vec<_>>();
        PostingList { elements, element_type: ElementType::EXTENDED }
    }

    fn create_simple_posting<W: QuantizedWeight>(elements: Vec<(RowId, W)>) -> PostingList<W> {
        let elements: Vec<GenericElement<W>> = elements.into_iter().map(|(row_id, weight)| SimpleElement { row_id, weight }.into()).collect::<Vec<_>>();
        PostingList { elements, element_type: ElementType::SIMPLE }
    }

    #[test]
    fn test_new_posting_builder() {
        let builder_f32_f32: PostingListBuilder<f32, f32> = PostingListBuilder::<f32, f32>::new(ElementType::SIMPLE, true).unwrap();
        assert_eq!(builder_f32_f32.element_type, ElementType::SIMPLE);
        assert_eq!(builder_f32_f32.need_quantized, false);
        assert_eq!(builder_f32_f32.propagate_while_upserting, false);
        assert_eq!(builder_f32_f32.finally_propagate, false);

        let builder_f16_f16: PostingListBuilder<half::f16, half::f16> = PostingListBuilder::<half::f16, half::f16>::new(ElementType::EXTENDED, true).unwrap();
        assert_eq!(builder_f16_f16.element_type, ElementType::EXTENDED);
        assert_eq!(builder_f16_f16.need_quantized, false);
        assert_eq!(builder_f16_f16.propagate_while_upserting, true);
        assert_eq!(builder_f16_f16.finally_propagate, false);

        let builder_u8_u8: PostingListBuilder<u8, u8> = PostingListBuilder::<u8, u8>::new(ElementType::EXTENDED, false).unwrap();
        assert_eq!(builder_u8_u8.element_type, ElementType::EXTENDED);
        assert_eq!(builder_u8_u8.need_quantized, false);
        assert_eq!(builder_u8_u8.propagate_while_upserting, false);
        assert_eq!(builder_u8_u8.finally_propagate, true);

        assert!(PostingListBuilder::<f32, u8>::new(ElementType::EXTENDED, false).is_err());
        assert!(PostingListBuilder::<half::f16, u8>::new(ElementType::EXTENDED, false).is_err());

        let builder_f32_u8: PostingListBuilder<f32, u8> = PostingListBuilder::<f32, u8>::new(ElementType::SIMPLE, false).unwrap();
        assert_eq!(builder_f32_u8.element_type, ElementType::SIMPLE);
        assert_eq!(builder_f32_u8.need_quantized, true);
        assert_eq!(builder_f32_u8.propagate_while_upserting, false);
        assert_eq!(builder_f32_u8.finally_propagate, false);
    }

    // 测试 PostingListBuilder::add 函数
    #[test]
    fn test_build_elements() {
        let m = DEFAULT_MAX_NEXT_WEIGHT;

        // Weight type is f32, stored without quantized, `ExtendedType`
        {
            let (posting, param) = mock_build_elements::<f32, f32>(ElementType::EXTENDED, false).expect("");
            let expected = create_extended_posting::<f32>(vec![(6, 70.0, 45.0), (14, 45.0, 30.0), (17, 10.0, 30.0), (18, 30.0, 20.0), (21, 20.0, m)]);
            assert_eq!(posting, expected);
            assert!(param.is_none())
        }
        // Weight type is f32, stored without quantized, `SimpleType`
        {
            let (posting, param) = mock_build_elements::<f32, f32>(ElementType::SIMPLE, false).expect("");
            let expected = create_simple_posting::<f32>(vec![(6, 70.0), (14, 45.0), (17, 10.0), (18, 30.0), (21, 20.0)]);
            assert_eq!(posting, expected);
            assert!(param.is_none())
        }
        // Weight type is f32, stored with quantized-u8, `SimpleType`
        {
            let (posting, param) = mock_build_elements::<f32, u8>(ElementType::SIMPLE, false).expect("");
            let expected_param = QuantizedParam::from_minmax(10.0, 70.0);
            let expected = create_simple_posting::<u8>(vec![
                (6, f32::quantize_with_param(70.0, expected_param)),
                (14, f32::quantize_with_param(45.0, expected_param)),
                (17, f32::quantize_with_param(10.0, expected_param)),
                (18, f32::quantize_with_param(30.0, expected_param)),
                (21, f32::quantize_with_param(20.0, expected_param)),
            ]);
            assert_eq!(posting, expected);
            assert!(param.is_some());
            assert_eq!(param.unwrap(), expected_param);
            assert_eq!(format!("{:.2}", 70.0), format!("{:.2}", f32::unquantize_with_param(expected.get_ref(0).unwrap().weight(), param.unwrap())));
        }
        // Invalid parameter.
        {
            assert!(mock_build_elements::<f32, u8>(ElementType::EXTENDED, false).is_err());
            assert!(mock_build_elements::<half::f16, u8>(ElementType::EXTENDED, false).is_err());
            assert!(mock_build_elements::<u8, u8>(ElementType::EXTENDED, false).is_ok());
        }
    }

    #[test]
    fn test_propagate_while_build() {
        let m = DEFAULT_MAX_NEXT_WEIGHT;
        // propagate while upserting.
        {
            let mut builder = PostingListBuilder::<f32, f32>::new(ElementType::EXTENDED, true).expect("");
            assert_eq!(builder.add(6, 70.0), true);
            assert_eq!(builder.add(14, 50.0), true);
            assert_eq!(builder.add(18, 30.0), true);
            assert_eq!(builder.add(21, 20.0), true);
            assert_eq!(builder.add(17, 10.0), true);
            assert_eq!(builder.add(14, 45.0), false);
            assert_eq!(builder.posting, create_extended_posting::<f32>(vec![(6, 70.0, 45.0), (14, 45.0, 30.0), (17, 10.0, 30.0), (18, 30.0, 20.0), (21, 20.0, m)]));
        }
        // propagate will be trigger while build.
        {
            let mut builder = PostingListBuilder::<f32, f32>::new(ElementType::EXTENDED, false).expect("");
            assert_eq!(builder.add(6, 70.0), true);
            assert_eq!(builder.add(14, 50.0), true);
            assert_eq!(builder.add(18, 30.0), true);
            assert_eq!(builder.add(21, 20.0), true);
            assert_eq!(builder.add(17, 10.0), true);
            assert_eq!(builder.add(14, 45.0), false);
            assert_eq!(builder.posting, create_extended_posting::<f32>(vec![(6, 70.0, m), (14, 45.0, m), (17, 10.0, m), (18, 30.0, m), (21, 20.0, m)]));

            let posting = builder.build().unwrap().0;
            assert_eq!(posting, create_extended_posting::<f32>(vec![(6, 70.0, 45.0), (14, 45.0, 30.0), (17, 10.0, 30.0), (18, 30.0, 20.0), (21, 20.0, m)]));
        }
    }
}
