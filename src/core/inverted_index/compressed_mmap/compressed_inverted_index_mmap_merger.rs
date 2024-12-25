use std::{
    cmp::{max, min},
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
};

use log::{debug, trace};

use crate::{
    core::{
        atomic_save_json,
        inverted_index::common::{InvertedIndexMeta, Revision, Version},
        madvise, transmute_from_u8, transmute_to_u8, transmute_to_u8_slice, CompressedBlockType, CompressedPostingListIterator, CompressedPostingListMerger,
        CompressedPostingListView, DimId, ElementType, ExtendedCompressedPostingBlock, InvertedIndexMmapAccess, PostingListIterAccess, QuantizedWeight,
        SimpleCompressedPostingBlock, WeightType,
    },
    thread_name, RowId,
};

use super::{
    compressed_posting_list_header::COMPRESSED_POSTING_HEADER_SIZE, CompressedInvertedIndexMmap, CompressedMmapInvertedIndexMeta, CompressedMmapManager,
    CompressedPostingListHeader,
};

pub struct CompressedInvertedIndexMmapMerger<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    compressed_inverted_index_mmaps: &'a Vec<&'a CompressedInvertedIndexMmap<OW, TW>>,
    element_type: ElementType,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> CompressedInvertedIndexMmapMerger<'a, OW, TW> {
    pub fn new(compressed_inverted_index_mmaps: &'a Vec<&'a CompressedInvertedIndexMmap<OW, TW>>, element_type: ElementType) -> Self {
        Self { compressed_inverted_index_mmaps, element_type }
    }

    fn get_compressed_posting_iterators_with_dim(&self, dim_id: DimId) -> Vec<CompressedPostingListIterator<'_, OW, TW>> {
        let mut compressed_postings_iterators = vec![];
        for &mmap_index in self.compressed_inverted_index_mmaps {
            let iter_opt = mmap_index.iter(&dim_id);
            if iter_opt.is_some() {
                compressed_postings_iterators.push(iter_opt.unwrap());
            }
        }
        compressed_postings_iterators
    }

    pub fn merge(&self, directory: &PathBuf, segment_id: Option<&str>) -> crate::Result<CompressedInvertedIndexMmap<OW, TW>> {
        // Record all the metrics of the inverted index that are pending to be merged.
        let mut min_dim_id = 0;
        let mut max_dim_id = 0;
        let mut min_row_id = RowId::MAX;
        let mut max_row_id = RowId::MIN;
        let mut total_vector_counts = 0;

        debug!("[{}]-[cmp-mmap-merger] merging {} compressed mmap indexes.", thread_name!(), self.compressed_inverted_index_mmaps.len());
        let mut approximate_row_ids_storage_size = 0;
        let mut approximate_blocks_storage_size = 0;
        for inverted_index in self.compressed_inverted_index_mmaps.iter() {
            let metrics = inverted_index.metrics();
            min_dim_id = min(min_dim_id, metrics.min_dim_id);
            max_dim_id = max(max_dim_id, metrics.max_dim_id);
            min_row_id = min(min_row_id, metrics.min_row_id);
            max_row_id = max(max_row_id, metrics.max_row_id);

            // TODO: refine approximate storage size, currently it's performance is poor.
            total_vector_counts += metrics.vector_count;
            approximate_row_ids_storage_size += inverted_index.meta.row_ids_storage_size;
            approximate_blocks_storage_size += inverted_index.meta.blocks_storage_size;
        }
        let total_headers_storage_size = (max_dim_id - min_dim_id + 1) as u64 * COMPRESSED_POSTING_HEADER_SIZE as u64;

        // Init headers file path.
        let (headers_mmap_file_path, _, _) = CompressedMmapManager::get_all_files(&directory.clone().to_path_buf(), segment_id);

        // Init two temporary mmap file path with given approximate storage size.
        let (row_ids_mmap_temp_path, blocks_mmap_temp_path) = CompressedMmapManager::get_temp_row_ids_and_blocks_mmap_files(&directory.clone(), segment_id);

        // Create mmap files.
        let mut headers_mmap = CompressedMmapManager::create_mmap_file(headers_mmap_file_path.as_ref(), total_headers_storage_size as u64, madvise::Advice::Normal)?;
        let mut row_ids_temp_mmap = CompressedMmapManager::create_mmap_file(row_ids_mmap_temp_path.as_ref(), approximate_row_ids_storage_size as u64, madvise::Advice::Normal)?;
        let mut blocks_temp_mmap = CompressedMmapManager::create_mmap_file(blocks_mmap_temp_path.as_ref(), approximate_blocks_storage_size as u64, madvise::Advice::Normal)?;

        let mut true_row_ids_storage_size = 0;
        let mut true_blocks_storage_size = 0;
        let mut total_blocks_count = 0;

        for dim_id in min_dim_id..(max_dim_id + 1) {
            // Merging all postings in current dim-id
            trace!("[{}]-[cmp-mmap-merger]-[dim-id:{}] loading a group of cmp-posting-iters.", thread_name!(), dim_id);
            let mut compressed_posting_iterators: Vec<CompressedPostingListIterator<'_, OW, TW>> = self.get_compressed_posting_iterators_with_dim(dim_id);

            trace!("[{}]-[cmp-mmap-merger]-[dim-id:{}] merging a group of cmp-posting-iters.", thread_name!(), dim_id);
            // TODO Figure out life comment in here
            let (merged_compressed_posting, quantized_param) = CompressedPostingListMerger::merge_posting_lists::<OW, TW>(&mut compressed_posting_iterators, self.element_type);
            // `TW` actually means storage type in disk.
            let compressed_posting_view: CompressedPostingListView<'_, TW> = merged_compressed_posting.view();

            // Step 1.1: Generate header
            let header_obj = CompressedPostingListHeader {
                compressed_row_ids_start: true_row_ids_storage_size,
                compressed_row_ids_end: true_row_ids_storage_size + compressed_posting_view.row_ids_storage_size(),

                compressed_blocks_start: true_blocks_storage_size,
                compressed_blocks_end: true_blocks_storage_size + compressed_posting_view.blocks_storage_size(),

                quantized_params: quantized_param,
                row_ids_count: compressed_posting_view.row_ids_count,
                max_row_id: compressed_posting_view.max_row_id,
                compressed_block_type: compressed_posting_view.compressed_block_type,
            };

            trace!("[{}]-[cmp-mmap-merger]-[dim-id:{}] header-obj generated:{:?}", thread_name!(), dim_id, header_obj.clone());

            // Step 1.2: Save the offset object to mmap.
            let header_bytes = transmute_to_u8(&header_obj);
            let header_offset_left = (dim_id as usize) * COMPRESSED_POSTING_HEADER_SIZE;
            let header_offset_right = (dim_id + 1) as usize * COMPRESSED_POSTING_HEADER_SIZE;

            trace!(
                "[{}]-[cmp-mmap-merger]-[dim-id:{}] store header-obj, left:{}, right:{}, copy:{}, approximate_storage:{}",
                thread_name!(),
                dim_id,
                header_offset_left,
                header_offset_right,
                header_bytes.len(),
                total_headers_storage_size
            );
            headers_mmap[header_offset_left..header_offset_right].copy_from_slice(header_bytes);

            // Step 2: Store row_ids
            trace!(
                "[{}]-[cmp-mmap-merger]-[dim-id:{}] store row-ids, left:{}, right:{}, copy:{}, approximate_storage:{}",
                thread_name!(),
                dim_id,
                header_obj.compressed_row_ids_start,
                header_obj.compressed_row_ids_end,
                compressed_posting_view.row_ids_compressed.len(),
                approximate_row_ids_storage_size
            );
            row_ids_temp_mmap[header_obj.compressed_row_ids_start..header_obj.compressed_row_ids_end].copy_from_slice(&compressed_posting_view.row_ids_compressed);

            // Step 3: Store posting blocks
            trace!(
                "[{}]-[cmp-mmap-merger]-[dim-id:{}] store blocks, left:{}, right:{}, simple_blocks:{}, extended_blocks:{}, approximate_storage:{}",
                thread_name!(),
                dim_id,
                header_obj.compressed_blocks_start,
                header_obj.compressed_blocks_end,
                compressed_posting_view.simple_blocks.len(),
                compressed_posting_view.extended_blocks.len(),
                approximate_blocks_storage_size
            );
            match compressed_posting_view.compressed_block_type {
                CompressedBlockType::Simple => {
                    let blocks: &[SimpleCompressedPostingBlock<TW>] = compressed_posting_view.simple_blocks;
                    let block_bytes = transmute_to_u8_slice(blocks);
                    blocks_temp_mmap[header_obj.compressed_blocks_start..header_obj.compressed_blocks_end].copy_from_slice(block_bytes);
                    total_blocks_count += blocks.len();
                }
                CompressedBlockType::Extended => {
                    let blocks: &[ExtendedCompressedPostingBlock<TW>] = compressed_posting_view.extended_blocks;
                    let block_bytes = transmute_to_u8_slice(blocks);
                    blocks_temp_mmap[header_obj.compressed_blocks_start..header_obj.compressed_blocks_end].copy_from_slice(block_bytes);
                    total_blocks_count += blocks.len();
                }
            }

            trace!("[{}]-[cmp-mmap-merger]-[dim-id:{}] merge has been finished.", thread_name!(), dim_id);
            // increase offsets.
            true_row_ids_storage_size += compressed_posting_view.row_ids_storage_size();
            true_blocks_storage_size += compressed_posting_view.blocks_storage_size();
        }

        // TODO Do some research about the `flush` option, it may has a influence on memory usage.
        if total_headers_storage_size > 0 {
            headers_mmap.flush()?;
        }
        if true_row_ids_storage_size > 0 {
            row_ids_temp_mmap.flush()?;
        }
        if true_blocks_storage_size > 0 {
            blocks_temp_mmap.flush()?;
        }

        // Create final version mmap files with specific storage size.
        let (row_ids_mmap_path, blocks_mmap_path) = CompressedMmapManager::get_row_ids_and_blocks_mmap_files(&directory.clone(), segment_id);
        let mut row_ids_mmap = CompressedMmapManager::create_mmap_file(row_ids_mmap_path.as_ref(), true_row_ids_storage_size as u64, madvise::Advice::Normal)?;
        let mut blocks_mmap = CompressedMmapManager::create_mmap_file(blocks_mmap_path.as_ref(), true_blocks_storage_size as u64, madvise::Advice::Normal)?;
        debug!("[{}]-[cmp-mmap-merger] rewriting for final version cmp-mmap-index file.", thread_name!());
        for dim_id in min_dim_id..(max_dim_id + 1) {
            let header_start = dim_id as usize * COMPRESSED_POSTING_HEADER_SIZE;
            let header_obj: CompressedPostingListHeader =
                transmute_from_u8::<CompressedPostingListHeader>(&headers_mmap[header_start..(header_start + COMPRESSED_POSTING_HEADER_SIZE)]).clone();
            let row_ids_compressed = &row_ids_temp_mmap[header_obj.compressed_row_ids_start..header_obj.compressed_row_ids_end];
            row_ids_mmap[header_obj.compressed_row_ids_start..header_obj.compressed_row_ids_end].copy_from_slice(row_ids_compressed);
            let blocks = &blocks_temp_mmap[header_obj.compressed_blocks_start..header_obj.compressed_blocks_end];
            blocks_mmap[header_obj.compressed_blocks_start..header_obj.compressed_blocks_end].copy_from_slice(blocks);
        }

        if true_row_ids_storage_size > 0 {
            row_ids_mmap.flush()?;
        }
        if true_blocks_storage_size > 0 {
            blocks_mmap.flush()?;
        }
        CompressedMmapManager::remove_temp_mmap_file(&directory.clone(), segment_id);

        debug!("[{}]-[cmp-mmap-merger] saving cmp-mmap-index meta file.", thread_name!());
        let meta: CompressedMmapInvertedIndexMeta = CompressedMmapInvertedIndexMeta {
            inverted_index_meta: InvertedIndexMeta {
                posting_count: (max_dim_id - min_dim_id + 1) as usize,
                vector_count: total_vector_counts,
                min_row_id,
                max_row_id,
                min_dim_id,
                max_dim_id,
                quantized: (TW::weight_type() == WeightType::WeightU8) && (OW::weight_type() != TW::weight_type()),
                version: Version::compressed_mmap(Revision::V1),
                element_type: self.element_type,
            },
            row_ids_storage_size: true_row_ids_storage_size as u64,
            total_blocks_count: total_blocks_count as u64,
            blocks_storage_size: true_blocks_storage_size as u64,
            headers_storage_size: total_headers_storage_size,
        };
        let meta_file_path = CompressedMmapManager::get_index_meta_file_path(&directory.clone(), segment_id);
        atomic_save_json(&meta_file_path, &meta)?;

        Ok(CompressedInvertedIndexMmap::<OW, TW> {
            path: directory.clone(),
            headers_mmap: Arc::new(headers_mmap.make_read_only()?),
            row_ids_mmap: Arc::new(row_ids_mmap.make_read_only()?),
            blocks_mmap: Arc::new(blocks_mmap.make_read_only()?),
            meta,
            _ow: PhantomData,
            _tw: PhantomData,
        })
    }
}
