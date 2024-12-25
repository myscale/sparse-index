use std::{
    cmp::{max, min},
    marker::PhantomData,
    mem::size_of,
    path::PathBuf,
    sync::Arc,
};

use log::debug;

use crate::{
    core::{
        atomic_save_json,
        inverted_index::common::{InvertedIndexMeta, Revision, Version},
        madvise, transmute_to_u8, transmute_to_u8_slice, DimId, ElementSlice, ElementType, ExtendedElement, GenericElement, GenericElementSlice, InvertedIndexMmapAccess,
        PostingListHeader, PostingListMerger, QuantizedParam, QuantizedWeight, WeightType, POSTING_HEADER_SIZE,
    },
    RowId,
};

use super::InvertedIndexMmap;
use super::{MmapInvertedIndexMeta, MmapManager};

pub struct InvertedIndexMmapMerger<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    inverted_index_mmaps: &'a Vec<&'a InvertedIndexMmap<OW, TW>>,
    element_type: ElementType,
}

fn unquantize_posting<'a, OW: QuantizedWeight, TW: QuantizedWeight>(quantized_posting: GenericElementSlice<'a, TW>, param: Option<QuantizedParam>) -> Vec<GenericElement<OW>> {
    // Boundary
    if param.is_none() {
        assert!(OW::weight_type() == TW::weight_type());
    } else {
        assert_eq!(TW::weight_type(), WeightType::WeightU8);
        assert_ne!(OW::weight_type(), TW::weight_type());
    }
    debug!(">>>>>>>>>>>|| after boundary");

    let mut unquantized_posting = vec![];
    for quantized_element in quantized_posting.generic_iter() {
        let element = quantized_element.to_owned().convert_or_unquantize(param);
        unquantized_posting.push(element);
    }
    unquantized_posting
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexMmapMerger<'a, OW, TW> {
    pub fn new(inverted_index_mmaps: &'a Vec<&'a InvertedIndexMmap<OW, TW>>, element_type: ElementType) -> Self {
        Self { inverted_index_mmaps, element_type }
    }

    fn get_unquantized_postings_with_dim(&self, dim_id: DimId) -> Vec<Vec<GenericElement<OW>>> {
        let mut unquantized_postings: Vec<Vec<GenericElement<OW>>> = vec![];

        for mmap_index in self.inverted_index_mmaps {
            debug!(">>>>>>>>>>>|| try call `posting_with_param` with dim:{}", dim_id);
            let (posting, quantized_param) = mmap_index.posting_with_param(&dim_id).unwrap_or(
                (GenericElementSlice::empty_slice(self.element_type), None), // 这里的 None 只起到一个填充的作用，不需要考虑 Default
            );
            debug!(">>>>>>>>>>>|| execute unquantize for dim:{} with param:{:?}", dim_id, quantized_param.clone());

            // TW means actually storage type, it needs reduction to OW.
            let unquantized_posting = unquantize_posting::<OW, TW>(posting, quantized_param);
            debug!(">>>>>>>>>>>|| finish execute unquantize for dim:{} with param:{:?}", dim_id, quantized_param.clone());

            unquantized_postings.push(unquantized_posting);
        }

        unquantized_postings
    }

    pub fn merge(&self, directory: &PathBuf, segment_id: Option<&str>) -> crate::Result<InvertedIndexMmap<OW, TW>> {
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

        debug!(">>>>>>>>>>> prepare merge");

        let total_headers_storage_size = (max_dim_id - min_dim_id + 1) as u64 * POSTING_HEADER_SIZE as u64;

        // Init mmap files.
        let (headers_mmap_file_path, postings_mmap_file_path) = MmapManager::get_all_mmap_files_path(&directory.clone().to_path_buf(), segment_id);
        let mut headers_mmap = MmapManager::create_mmap_file(headers_mmap_file_path.as_ref(), total_headers_storage_size as u64, madvise::Advice::Normal)?;
        let mut postings_mmap = MmapManager::create_mmap_file(postings_mmap_file_path.as_ref(), total_postings_storage_size as u64, madvise::Advice::Normal)?;

        // TODO: Make sure we should use `max_dim_id + 1`
        let mut current_element_offset = 0;
        for dim_id in min_dim_id..(max_dim_id + 1) {
            // Merging all postings in current dim-id
            debug!(">>>>>>>>>>> try get unquantized postings with dim:{}", dim_id);
            let postings = self.get_unquantized_postings_with_dim(dim_id);

            debug!(">>>>>>>>>>> before merged for dim:{}", dim_id);
            let (merged_posting, quantized_param) = PostingListMerger::merge_posting_lists::<OW, TW>(&postings, self.element_type)?;
            debug!(">>>>>>>>>>> after merged for dim:{}, param:{:?}", dim_id, quantized_param.clone());

            // Step 1: Generate header
            let header_obj = PostingListHeader {
                start: current_element_offset,
                end: current_element_offset + (merged_posting.len() * size_of::<ExtendedElement<TW>>()),
                quantized_params: quantized_param,
                row_ids_count: merged_posting.len() as RowId,
                max_row_id,
                element_type: self.element_type,
            };
            let header_bytes = transmute_to_u8(&header_obj);
            let header_obj_start = dim_id as usize * POSTING_HEADER_SIZE;
            let header_obj_end = (dim_id + 1) as usize * POSTING_HEADER_SIZE;
            headers_mmap[header_obj_start..header_obj_end].copy_from_slice(header_bytes);

            // Step 2: Generate posting
            // TODO 优化这个序列化存储的逻辑
            // let merged_posting_elements_bytes = transmute_to_u8_slice(&merged_posting.elements);
            match self.element_type {
                ElementType::SIMPLE => {
                    let simple_els = merged_posting.elements.iter().map(|e| e.as_simple().unwrap().clone()).collect::<Vec<_>>();
                    let posting_elements_bytes = transmute_to_u8_slice(&simple_els);
                    postings_mmap[current_element_offset..(current_element_offset + posting_elements_bytes.len())].copy_from_slice(posting_elements_bytes);
                    // increase offsets.
                    current_element_offset += posting_elements_bytes.len();
                }
                ElementType::EXTENDED => {
                    let elements = merged_posting.elements.iter().map(|e| e.as_extended().unwrap().clone()).collect::<Vec<_>>();
                    let posting_elements_bytes = transmute_to_u8_slice(&elements);
                    postings_mmap[current_element_offset..(current_element_offset + posting_elements_bytes.len())].copy_from_slice(posting_elements_bytes);
                    // increase offsets.
                    current_element_offset += posting_elements_bytes.len();
                }
            }
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
                quantized: (TW::weight_type() == WeightType::WeightU8) && (OW::weight_type() != TW::weight_type()),
                version: Version::mmap(Revision::V1),
                element_type: self.element_type,
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
