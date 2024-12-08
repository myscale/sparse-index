use std::{
    cmp::{max, min},
    marker::PhantomData,
    mem::size_of,
    path::PathBuf,
    sync::Arc,
};

use crate::{
    core::{
        atomic_save_json, madvise, transmute_to_u8, transmute_to_u8_slice, DimId,
        InvertedIndexMeta, InvertedIndexMmapAccess, ExtendedElement, PostingListHeader,
        PostingListMerger, QuantizedParam, QuantizedWeight, Revision, Version, WeightType,
        POSTING_HEADER_SIZE,
    },
    RowId,
};

use super::InvertedIndexMmap;
use super::{MmapInvertedIndexMeta, MmapManager};

pub struct InvertedIndexMmapMerger<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    inverted_index_mmaps: &'a Vec<&'a InvertedIndexMmap<OW, TW>>,
}

fn unquantized_posting<OW: QuantizedWeight, TW: QuantizedWeight>(
    quantized_posting: &[ExtendedElement<TW>],
    param: Option<QuantizedParam>,
) -> Vec<ExtendedElement<OW>> {
    if param.is_none() {
        assert!(OW::weight_type() == TW::weight_type() || quantized_posting.len() == 0);

        let mut converted_posting = vec![];
        for element in quantized_posting {
            let converted_element: ExtendedElement<OW> = ExtendedElement {
                row_id: element.row_id,
                weight: OW::from_f32(TW::to_f32(element.weight)),
                max_next_weight: OW::from_f32(TW::to_f32(element.max_next_weight)),
            };
            converted_posting.push(converted_element);
        }
        return converted_posting;
    } else {
        assert_eq!(TW::weight_type(), WeightType::WeightU8);
        let param: QuantizedParam = param.unwrap();

        let mut unquantized_posting = vec![];
        for quantized_element in quantized_posting {
            let unquantized_element: ExtendedElement<OW> = ExtendedElement::<OW> {
                row_id: quantized_element.row_id,
                weight: OW::unquantize_with_param(TW::to_u8(quantized_element.weight), param),
                max_next_weight: OW::unquantize_with_param(
                    TW::to_u8(quantized_element.max_next_weight),
                    param,
                ),
            };
            unquantized_posting.push(unquantized_element);
        }
        return unquantized_posting;
    }
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexMmapMerger<'a, OW, TW> {
    pub fn new(inverted_index_mmaps: &'a Vec<&'a InvertedIndexMmap<OW, TW>>) -> Self {
        Self { inverted_index_mmaps }
    }

    fn get_unquantized_postings_with_dim(&self, dim_id: DimId) -> Vec<Vec<ExtendedElement<OW>>> {
        let mut unquantized_postings: Vec<Vec<ExtendedElement<OW>>> = vec![];
        let empty_posting: &[ExtendedElement<TW>] = &[];

        for mmap_index in self.inverted_index_mmaps {
            let (posting, quantized_param) =
                mmap_index.posting_with_param(&dim_id).unwrap_or((empty_posting, None));

            // TW means actually storage type, it needs reduction to OW.
            let unquantized_posting = unquantized_posting::<OW, TW>(posting, quantized_param);
            unquantized_postings.push(unquantized_posting);
        }

        unquantized_postings
    }

    pub fn merge(
        &self,
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<InvertedIndexMmap<OW, TW>> {
        // Record all the metrics of the inverted index that are pending to be merged.
        let mut min_dim_id = 0;
        let mut max_dim_id = 0;
        let mut min_row_id = RowId::MAX;
        let mut max_row_id = RowId::MIN;
        let mut total_vector_counts = 0;
        let mut total_postings_storage_size: u64 = 0;

        for inverted_index in self.inverted_index_mmaps.iter() {
            let metrics = inverted_index.metrics();
            min_dim_id = min(min_dim_id, metrics.min_dim_id);
            max_dim_id = max(max_dim_id, metrics.max_dim_id);
            min_row_id = min(min_row_id, metrics.min_row_id);
            max_row_id = max(max_row_id, metrics.max_row_id);

            total_postings_storage_size += inverted_index.meta.postings_storage_size;
            total_vector_counts += metrics.vector_count;
        }
        let total_headers_storage_size =
            (max_dim_id - min_dim_id + 1) as u64 * POSTING_HEADER_SIZE as u64;

        // Init mmap files.
        let (headers_mmap_file_path, postings_mmap_file_path) =
            MmapManager::get_all_mmap_files_path(&directory.clone().to_path_buf(), segment_id);
        let mut headers_mmap = MmapManager::create_mmap_file(
            headers_mmap_file_path.as_ref(),
            total_headers_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        let mut postings_mmap = MmapManager::create_mmap_file(
            postings_mmap_file_path.as_ref(),
            total_postings_storage_size as u64,
            madvise::Advice::Normal,
        )?;

        // TODO: Make sure we should use `max_dim_id + 1`
        let mut current_element_offset = 0;
        for dim_id in min_dim_id..(max_dim_id + 1) {
            // Merging all postings in current dim-id
            let postings: Vec<Vec<ExtendedElement<OW>>> =
                self.get_unquantized_postings_with_dim(dim_id);

            let (merged_posting, quantized_param) =
                PostingListMerger::merge_posting_lists::<OW, TW>(&postings);

            // Step 1: Generate header
            let header_obj = PostingListHeader {
                start: current_element_offset,
                end: current_element_offset
                    + (merged_posting.len() * size_of::<ExtendedElement<TW>>()),
                quantized_params: quantized_param,
                row_ids_count: merged_posting.len() as RowId,
                max_row_id,
            };
            let header_bytes = transmute_to_u8(&header_obj);
            let header_obj_start = dim_id as usize * POSTING_HEADER_SIZE;
            let header_obj_end = (dim_id + 1) as usize * POSTING_HEADER_SIZE;
            headers_mmap[header_obj_start..header_obj_end].copy_from_slice(header_bytes);

            // Step 2: Generate posting
            let merged_posting_elements_bytes = transmute_to_u8_slice(&merged_posting.elements);
            postings_mmap[current_element_offset
                ..(current_element_offset + merged_posting_elements_bytes.len())]
                .copy_from_slice(merged_posting_elements_bytes);

            // increase offsets.
            current_element_offset += merged_posting_elements_bytes.len();
        }

        // flushing mmap
        if total_headers_storage_size > 0 {
            headers_mmap.flush()?;
        }
        if total_postings_storage_size > 0 {
            postings_mmap.flush()?;
        }

        let meta = MmapInvertedIndexMeta {
            inverted_index_meta: InvertedIndexMeta {
                posting_count: (max_dim_id - min_dim_id + 1) as usize,
                vector_count: total_vector_counts,
                min_row_id,
                max_row_id,
                min_dim_id,
                max_dim_id,
                quantized: (TW::weight_type() == WeightType::WeightU8)
                    && (OW::weight_type() != TW::weight_type()),
                version: Version::mmap(Revision::V1),
            },
            headers_storage_size: total_headers_storage_size,
            postings_storage_size: total_postings_storage_size,
        };
        let meta_file_path = MmapManager::get_index_meta_file_path(&directory.clone(), segment_id);
        atomic_save_json(&meta_file_path, &meta)?;

        Ok(InvertedIndexMmap {
            path: directory.clone(),
            headers_mmap: Arc::new(headers_mmap.make_read_only()?),
            postings_mmap: Arc::new(postings_mmap.make_read_only()?),
            meta,
            _phantom_w: PhantomData,
            _phantom_t: PhantomData,
        })
    }
}
