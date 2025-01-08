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
        core::{posting_list::encoder::VIntDecoder, BlockDecoder, ElementType, QuantizedParam, QuantizedWeight, COMPRESSION_BLOCK_SIZE},
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
        let compressed_posting = builder.build().expect("msg");
        let param = compressed_posting.quantization_params.clone();
        return (compressed_posting, param);
    }

    pub(super) fn generate_random_float() -> f32 {
        let mut rng = rand::thread_rng();
        rng.gen_range(0.010101..10.1111)
    }

    pub(super) fn generate_random_int(min: u32, max: u32) -> u32 {
        let mut rng = rand::thread_rng();
        rng.gen_range(min..=max)
    }

    /// Utility function to enlarge the vector (one posting), mainly used for merge testing.
    pub(super) fn enlarge_elements(vector: Vec<(u32, f32)>, base: u32) -> Vec<(u32, f32)> {
        let mut enlarged = vector.clone();
        for (row_id, _) in vector {
            enlarged.push((row_id + base, generate_random_float()));
        }
        enlarged
    }

    pub(super) fn mock_compressed_posting_from_sequence_elements<OW: QuantizedWeight, TW: QuantizedWeight>(
        element_type: ElementType,
        count: usize,
    ) -> (CompressedPostingList<TW>, Vec<(u32, f32)>) {
        #[rustfmt::skip]
        let mut elements: Vec<(u32, f32)> = Vec::new();
        for row_id in 1..=count {
            elements.push((row_id as u32, format!("{:.3}", generate_random_float()).parse::<f32>().unwrap()));
        }

        let (cmp_posting, _) = mock_build_compressed_posting::<OW, TW>(element_type, elements.clone());
        (cmp_posting, elements)
    }

    /// When `enable_random_gap` is true, the row_id will be generated sortly but not sequence.
    pub(super) fn generate_elements(count: usize, enable_random_gap: bool) -> Vec<(u32, f32)> {
        let mut elements: Vec<(u32, f32)> = Vec::new();
        // Boundary
        if count == 0 {
            return elements;
        }
        // Init first element
        elements.push((1, format!("{:.3}", generate_random_float()).parse::<f32>().unwrap()));

        for _ in 2..=count {
            let row_id = elements.last().unwrap().0
                + match enable_random_gap {
                    true => generate_random_int(1, 128),
                    false => 1,
                };
            elements.push((row_id as u32, format!("{:.3}", generate_random_float()).parse::<f32>().unwrap()));
        }
        elements
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

    pub(super) fn uncompress_row_ids_from_compressed_posting<TW: QuantizedWeight>(compressed_posting: &CompressedPostingList<TW>) -> Vec<u32> {
        let row_ids_count = compressed_posting.row_ids_count;
        let mut row_ids_restore: Vec<u32> = Vec::with_capacity(row_ids_count as usize);
        // 手动 uncompress row_ids
        let mut decoder = BlockDecoder::default();
        let row_ids_compressed = &compressed_posting.row_ids_compressed;

        match compressed_posting.compressed_block_type {
            super::CompressedBlockType::Simple => {
                for simple_block in &compressed_posting.simple_blocks {
                    let row_ids_compressed_in_block =
                        &row_ids_compressed[simple_block.block_offset as usize..(simple_block.block_offset as usize + simple_block.row_ids_compressed_size as usize)];
                    let mut row_ids_uncompressed = inner_uncompress_row_ids(
                        &mut decoder,
                        simple_block.row_id_start,
                        simple_block.num_bits,
                        simple_block.row_ids_compressed_size,
                        simple_block.row_ids_count,
                        row_ids_compressed_in_block,
                    );
                    row_ids_restore.append(&mut row_ids_uncompressed);
                }
            }
            super::CompressedBlockType::Extended => {
                for extend_block in &compressed_posting.extended_blocks {
                    let row_ids_compressed_in_block =
                        &row_ids_compressed[extend_block.block_offset as usize..(extend_block.block_offset as usize + extend_block.row_ids_compressed_size as usize)];
                    let mut row_ids_uncompressed = inner_uncompress_row_ids(
                        &mut decoder,
                        extend_block.row_id_start,
                        extend_block.num_bits,
                        extend_block.row_ids_compressed_size,
                        extend_block.row_ids_count,
                        row_ids_compressed_in_block,
                    );
                    row_ids_restore.append(&mut row_ids_uncompressed);
                }
            }
        }

        row_ids_restore
    }

    fn inner_uncompress_row_ids(
        decoder: &mut BlockDecoder,
        row_id_start: RowId,
        num_bits: u8,
        row_ids_compressed_size: u16,
        row_ids_count: u8,
        row_ids_compressed_in_block: &[u8],
    ) -> Vec<u32> {
        let consumed_bytes: usize = match row_ids_count as usize == COMPRESSION_BLOCK_SIZE {
            true => decoder.uncompress_block_sorted(row_ids_compressed_in_block, row_id_start.checked_sub(1).unwrap_or(0), num_bits, true),
            false => decoder.uncompress_vint_sorted(row_ids_compressed_in_block, row_id_start.checked_sub(1).unwrap_or(0), row_ids_count as usize, RowId::MAX),
        };

        assert_eq!(consumed_bytes, row_ids_compressed_size as usize);
        let row_ids_uncompressed: Vec<u32> = match row_ids_count as usize == COMPRESSION_BLOCK_SIZE {
            true => {
                let res: &[u32; COMPRESSION_BLOCK_SIZE] = decoder.full_output();
                res.to_vec()
            }
            false => {
                let res: &[u32] = &decoder.output_array()[0..decoder.output_len];
                res.to_vec()
            }
        };
        row_ids_uncompressed
    }
}
