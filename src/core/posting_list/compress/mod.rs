mod compressed_posting_block;
mod compressed_posting_builder;
mod compressed_posting_iterator;
mod compressed_posting_list;
mod compressed_posting_list_merger;
mod compressed_posting_list_view;

pub use compressed_posting_block::*;
pub use compressed_posting_builder::CompressedPostingBuilder;
pub use compressed_posting_iterator::CompressedPostingListIterator;
pub use compressed_posting_list::CompressedPostingList;
pub use compressed_posting_list_merger::CompressedPostingListMerger;
pub use compressed_posting_list_view::*;

#[cfg(test)]
mod test {
    use rand::Rng;

    use crate::{
        core::{ElementType, QuantizedParam, QuantizedWeight},
        RowId,
    };

    use super::{CompressedPostingBuilder, CompressedPostingList, CompressedPostingListIterator};

    pub(super) fn mock_build_compressed_posting<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        elements: Vec<(RowId, f32)>,
    ) -> (CompressedPostingList<TW>, Option<QuantizedParam>) {
        let mut builder = CompressedPostingBuilder::<OW, TW>::new(element_type, true, false).expect("");
        for (row_id, weight) in elements {
            builder.add(row_id, weight);
        }
        let compressed_posting = builder.build();
        let param = compressed_posting.quantization_params.clone();
        return (compressed_posting, param);
    }

    pub(super) fn generate_random_float() -> f32 {
        let mut rng = rand::thread_rng();
        rng.gen_range(0.010101..10.1111)
    }

    pub(super) fn enlarge_elements(vector: Vec<(u32, f32)>, base: u32) -> Vec<(u32, f32)> {
        let mut enlarged = vector.clone();
        for (row_id, _) in vector {
            enlarged.push((row_id + base, generate_random_float()));
        }
        enlarged
    }

    pub(super) fn get_compressed_posting_iterator<OW: QuantizedWeight, TW: QuantizedWeight>(
        compressed_posting: &CompressedPostingList<TW>,
    ) -> CompressedPostingListIterator<'_, OW, TW> {
        let view = compressed_posting.view();
        CompressedPostingListIterator::<OW, TW>::new(&view)
    }

    pub(super) fn get_compressed_posting_iterators<OW: QuantizedWeight, TW: QuantizedWeight>(
        compressed_postings: &[CompressedPostingList<TW>],
    ) -> Vec<CompressedPostingListIterator<'_, OW, TW>> {
        let iterators = compressed_postings
            .iter()
            .map(|e| {
                let view = e.view();
                let iter = CompressedPostingListIterator::<OW, TW>::new(&view);
                return iter;
            })
            .collect::<Vec<_>>();
        iterators
    }
}
