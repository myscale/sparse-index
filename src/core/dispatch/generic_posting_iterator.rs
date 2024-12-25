use enum_dispatch::enum_dispatch;
use log::error;

use crate::core::{CompressedPostingListIterator, ElementRead, GenericElement, PostingListIter, PostingListIterator, QuantizedWeight, SparseBitmap, TopK};
use crate::ffi::ScoredPointOffset;
use crate::RowId;
use std::any::TypeId;
use std::mem;

#[enum_dispatch(PostingListIter<OW, TW>)]
pub enum PostingListIteratorWrapper<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    SimplePostingListIterator(PostingListIterator<'a, OW, TW>),
    CompressedPostingListIterator(CompressedPostingListIterator<'a, OW, TW>),
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> PostingListIteratorWrapper<'a, OW, TW> {
    #[rustfmt::skip]
    fn batch_compute(
        &mut self,
        batch_scores: &mut Vec<f32>,
        query_dim_weight: f32,
        batch_start_row_id: RowId,
        batch_end_row_id: RowId
    ) {
        assert_eq!(batch_scores.len(), (batch_end_row_id - batch_start_row_id + 1) as usize);

        match self {
            PostingListIteratorWrapper::SimplePostingListIterator(e) => {
                e.for_each_till_row_id(batch_end_row_id, |generic_element| {
                    if generic_element.row_id() < batch_start_row_id || generic_element.row_id() > batch_end_row_id {
                        let error_msg: &str = "Error happended when compute SimplePostingListIterator! row_id is not in valid batch range.";
                        error!("{}", error_msg);
                        return;
                    }
                    let score: f32 = OW::to_f32(generic_element.weight()) * query_dim_weight;
                    let offset: usize = (generic_element.row_id() - batch_start_row_id) as usize;
                    batch_scores[offset] = score;
                });
            },
            PostingListIteratorWrapper::CompressedPostingListIterator(e) => {
                e.for_each_till_row_id(batch_end_row_id, |generic_element| {
                    if generic_element.row_id() < batch_start_row_id || generic_element.row_id() > batch_end_row_id {
                        let error_msg: &str = "Error happended when compute SimplePostingListIterator! row_id is not in valid batch range.";
                        error!("{}", error_msg);
                        return;
                    }
                    let score: f32 = OW::to_f32(generic_element.weight()) * query_dim_weight;
                    let offset: usize = (generic_element.row_id() - batch_start_row_id) as usize;
                    batch_scores[offset] = score;
                });
            },
        }
    }

    #[rustfmt::skip]
    fn full_compute(&mut self, end_row_id: RowId, query_dim_weight: f32, alive_bitmap: &Option<SparseBitmap>, top_k: &mut TopK) {
        match self {
            PostingListIteratorWrapper::SimplePostingListIterator(e) => {
                e.for_each_till_row_id(end_row_id, |generic_element|{
                    let mut is_alive = true;
                    if let Some(bitmap) = alive_bitmap {
                        is_alive = bitmap.is_alive(generic_element.row_id());
                    }
                    if is_alive {
                        let score: f32 = OW::to_f32(generic_element.weight()) * query_dim_weight;
                        top_k.push(ScoredPointOffset{ row_id: generic_element.row_id(), score });
                    }
                });
            },
            PostingListIteratorWrapper::CompressedPostingListIterator(e) => {
                e.for_each_till_row_id(end_row_id, |generic_element|{
                    let mut is_alive = true;
                    if let Some(bitmap) = alive_bitmap {
                        is_alive = bitmap.is_alive(generic_element.row_id());
                    }
                    if is_alive {
                        let score: f32 = OW::to_f32(generic_element.weight()) * query_dim_weight;
                        top_k.push(ScoredPointOffset{ row_id: generic_element.row_id(), score });
                    }
                });
            },
        }
    }
}

#[enum_dispatch(GenericPostingListIter)]
pub enum GenericPostingListIterator<'a> {
    F32NoQuantized(PostingListIteratorWrapper<'a, f32, f32>),
    F32Quantized(PostingListIteratorWrapper<'a, f32, u8>),
    F16NoQuantized(PostingListIteratorWrapper<'a, half::f16, half::f16>),
    F16Quantized(PostingListIteratorWrapper<'a, half::f16, u8>),
    U8NoQuantized(PostingListIteratorWrapper<'a, u8, u8>),
}

#[rustfmt::skip]
impl<'a> GenericPostingListIterator<'a> {
    pub fn get_element_opt(&mut self, row_id: RowId) -> Option<GenericElement<f32>> {
        match self {
            GenericPostingListIterator::F32NoQuantized(e) => {
                match e.skip_to(row_id) {
                    Some(element) => Some(element),
                    None => None,
                }
            },
            GenericPostingListIterator::F32Quantized(e) => {
                match e.skip_to(row_id) {
                    Some(element) => Some(element),
                    None => None,
                }
            },
            GenericPostingListIterator::F16NoQuantized(e) => {
                match e.skip_to(row_id) {
                    Some(element) => Some(element.type_convert::<f32>()),
                    None => None,
                }
            },
            GenericPostingListIterator::F16Quantized(e) => {
                match e.skip_to(row_id) {
                    Some(element) => Some(element.type_convert::<f32>()),
                    None => None,
                }
            },
            GenericPostingListIterator::U8NoQuantized(e) => {
                match e.skip_to(row_id) {
                    Some(element) => Some(element.type_convert::<f32>()),
                    None => None,
                }
            },
        }
    }

    #[rustfmt::skip]
    pub fn batch_compute(
        &mut self,
        batch_scores: &mut Vec<f32>,
        query_weight: f32,
        batch_start_row_id: RowId,
        batch_end_row_id: RowId
    ) {
        match self {
            GenericPostingListIterator::F32NoQuantized(e) => e.batch_compute(batch_scores, query_weight, batch_start_row_id, batch_end_row_id),
            GenericPostingListIterator::F32Quantized(e) => e.batch_compute(batch_scores, query_weight, batch_start_row_id, batch_end_row_id),
            GenericPostingListIterator::F16NoQuantized(e) => e.batch_compute(batch_scores, query_weight, batch_start_row_id, batch_end_row_id),
            GenericPostingListIterator::F16Quantized(e) => e.batch_compute(batch_scores, query_weight, batch_start_row_id, batch_end_row_id),
            GenericPostingListIterator::U8NoQuantized(e) => e.batch_compute(batch_scores, query_weight, batch_start_row_id, batch_end_row_id),
        }
    }

    #[rustfmt::skip]
    pub fn full_compute(&mut self, end_row_id: RowId, query_dim_weight: f32, alive_bitmap: &Option<SparseBitmap>, top_k: &mut TopK) {
        match self {
            GenericPostingListIterator::F32NoQuantized(e) => e.full_compute(end_row_id, query_dim_weight, alive_bitmap, top_k),
            GenericPostingListIterator::F32Quantized(e) => e.full_compute(end_row_id, query_dim_weight, alive_bitmap, top_k),
            GenericPostingListIterator::F16NoQuantized(e) => e.full_compute(end_row_id, query_dim_weight, alive_bitmap, top_k),
            GenericPostingListIterator::F16Quantized(e) => e.full_compute(end_row_id, query_dim_weight, alive_bitmap, top_k),
            GenericPostingListIterator::U8NoQuantized(e) => e.full_compute(end_row_id, query_dim_weight, alive_bitmap, top_k),
        }
    }

    #[rustfmt::skip]
    pub fn remains(&self) -> usize {
        match self {
            GenericPostingListIterator::F32NoQuantized(e) => e.remains(),
            GenericPostingListIterator::F32Quantized(e) => e.remains(),
            GenericPostingListIterator::F16NoQuantized(e) => e.remains(),
            GenericPostingListIterator::F16Quantized(e) => e.remains(),
            GenericPostingListIterator::U8NoQuantized(e) => e.remains(),
        }
    }

    #[rustfmt::skip]
    pub fn cursor(&self) -> usize {
        match self {
            GenericPostingListIterator::F32NoQuantized(e) => e.cursor(),
            GenericPostingListIterator::F32Quantized(e) => e.cursor(),
            GenericPostingListIterator::F16NoQuantized(e) => e.cursor(),
            GenericPostingListIterator::F16Quantized(e) => e.cursor(),
            GenericPostingListIterator::U8NoQuantized(e) => e.cursor(),
        }
    }

    #[rustfmt::skip]
    pub fn skip_to_end(&mut self) {
        match self {
            GenericPostingListIterator::F32NoQuantized(e) => e.skip_to_end(),
            GenericPostingListIterator::F32Quantized(e) => e.skip_to_end(),
            GenericPostingListIterator::F16NoQuantized(e) => e.skip_to_end(),
            GenericPostingListIterator::F16Quantized(e) => e.skip_to_end(),
            GenericPostingListIterator::U8NoQuantized(e) => e.skip_to_end(),
        }
    }

    #[rustfmt::skip]
    pub fn skip_to(&mut self, row_id: RowId) {
        match self {
            GenericPostingListIterator::F32NoQuantized(e) => { e.skip_to(row_id); },
            GenericPostingListIterator::F32Quantized(e) => { e.skip_to(row_id); },
            GenericPostingListIterator::F16NoQuantized(e) => { e.skip_to(row_id); },
            GenericPostingListIterator::F16Quantized(e) => { e.skip_to(row_id); },
            GenericPostingListIterator::U8NoQuantized(e) => { e.skip_to(row_id); },
        }
    }
    
    #[rustfmt::skip]
    pub fn peek(&mut self) -> Option<GenericElement<f32>> {
        match self {
            GenericPostingListIterator::F32NoQuantized(i) => i.peek(),
            GenericPostingListIterator::F32Quantized(i) => i.peek(),
            GenericPostingListIterator::F16NoQuantized(i) => i.peek().map(|e: GenericElement<half::f16>| e.type_convert::<f32>()),
            GenericPostingListIterator::F16Quantized(i) => i.peek().map(|e: GenericElement<half::f16>| e.type_convert::<f32>()),
            GenericPostingListIterator::U8NoQuantized(i) => i.peek().map(|e: GenericElement<u8>| e.type_convert::<f32>()),
        }
    }
}

#[rustfmt::skip]
impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> From<PostingListIteratorWrapper<'a, OW, TW>> for GenericPostingListIterator<'a> {
    #[rustfmt::skip]
    fn from(wrapper: PostingListIteratorWrapper<'a, OW, TW>) -> Self {
        match (TypeId::of::<OW>(), TypeId::of::<TW>()) {
            // OW f32，TW f32
            (t1, t2) if t1 == TypeId::of::<f32>() && t2 == TypeId::of::<f32>() => {
                // 使用 unsafe 强制转换类型
                let wrapper: PostingListIteratorWrapper<'a, f32, f32> = unsafe { mem::transmute(wrapper) };
                GenericPostingListIterator::F32NoQuantized(wrapper)
            }
            // OW f32，TW u8
            (t1, t2) if t1 == TypeId::of::<f32>() && t2 == TypeId::of::<u8>() => {
                let wrapper: PostingListIteratorWrapper<'a, f32, u8> = unsafe { mem::transmute(wrapper) };
                GenericPostingListIterator::F32Quantized(wrapper)
            }
            // OW half::f16，TW half::f16
            (t1, t2) if t1 == TypeId::of::<half::f16>() && t2 == TypeId::of::<half::f16>() => {
                let wrapper: PostingListIteratorWrapper<'a, half::f16, half::f16> = unsafe { mem::transmute(wrapper) };
                GenericPostingListIterator::F16NoQuantized(wrapper)
            }
            // OW half::f16，TW u8
            (t1, t2) if t1 == TypeId::of::<half::f16>() && t2 == TypeId::of::<u8>() => {
                let wrapper: PostingListIteratorWrapper<'a, half::f16, u8> = unsafe { mem::transmute(wrapper) };
                GenericPostingListIterator::F16Quantized(wrapper)
            }
            // OW u8，TW u8
            (t1, t2) if t1 == TypeId::of::<u8>() && t2 == TypeId::of::<u8>() => {
                let wrapper: PostingListIteratorWrapper<'a, u8, u8> = unsafe { mem::transmute(wrapper) };
                GenericPostingListIterator::U8NoQuantized(wrapper)
            }
            _ => panic!("Unsupported combination of types for PostingListIteratorWrapper"),
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_generic() {}
}
