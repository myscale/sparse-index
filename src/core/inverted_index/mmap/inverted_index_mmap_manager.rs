use std::{
    io,
    mem::size_of,
    path::{Path, PathBuf},
    sync::Arc,
};

use memmap2::{Mmap, MmapMut};

use crate::{
    core::{
        create_and_ensure_length, madvise::{self, Advice}, open_write_mmap, transmute_to_u8, transmute_to_u8_slice, Element, ElementType, ExtendedElement, GenericElement, InvertedIndexRam, InvertedIndexRamAccess, QuantizedWeight, SimpleElement
    },
    RowId,
};

use super::{InvertedIndexMmapFileConfig, PostingListHeader, POSTING_HEADER_SIZE};

pub struct MmapManager;

impl MmapManager {
    pub(super) fn get_file_path<F>(directory: &PathBuf, segment_id: Option<&str>, f: F) -> PathBuf
    where
        F: Fn(Option<&str>) -> String,
    {
        directory.join(f(segment_id))
    }

    pub(super) fn get_all_mmap_files_path(
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> (PathBuf, PathBuf) {
        let headers_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            InvertedIndexMmapFileConfig::headers_file_name,
        );
        let postings_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            InvertedIndexMmapFileConfig::postings_file_name,
        );
        (headers_mmap_file_path, postings_mmap_file_path)
    }

    pub(super) fn get_index_meta_file_path(
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> PathBuf {
        let inverted_index_meta_file_path = Self::get_file_path(
            directory,
            segment_id,
            InvertedIndexMmapFileConfig::inverted_meta_file_name,
        );
        inverted_index_meta_file_path
    }

    pub(super) fn create_mmap_file(
        mmap_file_path: &Path,
        mmap_file_size: u64,
        advice: Advice,
    ) -> Result<MmapMut, io::Error> {
        create_and_ensure_length(mmap_file_path, mmap_file_size)?;
        let mmap: MmapMut = open_write_mmap(mmap_file_path)?;
        madvise::madvise(&mmap, advice)?;
        return Ok(mmap);
    }

    // TODO: Refine path parameter.
    pub fn write_mmap_files<P: AsRef<Path>, TW: QuantizedWeight>(
        directory: &PathBuf,
        segment_id: Option<&str>,
        inv_idx_ram: &InvertedIndexRam<TW>,
    ) -> crate::Result<(usize, usize, Arc<Mmap>, Arc<Mmap>)> {
        // compute posting_offsets and elements size.
        let total_headers_storage_size: usize = inv_idx_ram.size() * POSTING_HEADER_SIZE;

        let total_postings_elements_size: usize = inv_idx_ram
            .postings()
            .iter()
            .map(|posting| {
                match posting.element_type {
                    ElementType::SIMPLE => posting.len() * size_of::<SimpleElement<TW>>(),
                    ElementType::EXTENDED => posting.len() * size_of::<ExtendedElement<TW>>(),
                }
            })
            .sum();

        // Init two mmap file paths.
        let (headers_mmap_file_path, postings_mmap_file_path) =
            Self::get_all_mmap_files_path(&directory, segment_id);

        let mut headers_mmap = Self::create_mmap_file(
            headers_mmap_file_path.as_ref(),
            total_headers_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        let mut postings_mmap = Self::create_mmap_file(
            postings_mmap_file_path.as_ref(),
            total_postings_elements_size as u64,
            madvise::Advice::Normal,
        )?;

        Self::save_data_to_mmap::<TW>(&mut headers_mmap, &mut postings_mmap, inv_idx_ram);

        if total_headers_storage_size > 0 {
            headers_mmap.flush()?;
        }
        if total_postings_elements_size > 0 {
            postings_mmap.flush()?;
        }

        return Ok((
            total_headers_storage_size,
            total_postings_elements_size,
            Arc::new(headers_mmap.make_read_only()?),
            Arc::new(postings_mmap.make_read_only()?),
        ));
    }

    fn save_data_to_mmap<TW: QuantizedWeight>(
        headers_mmap: &mut MmapMut,
        postings_mmap: &mut MmapMut,
        inv_idx_ram: &InvertedIndexRam<TW>,
    ) {
        let mut cur_postings_storage_size = 0;

        for (dim_id, (posting, param)) in
            inv_idx_ram.postings().iter().zip(inv_idx_ram.quantized_params().iter()).enumerate()
        {
            // Step 1.1: Generate header
            let header_obj = PostingListHeader {
                start: cur_postings_storage_size,
                end: cur_postings_storage_size
                    + match posting.element_type {
                        ElementType::SIMPLE => posting.len() * size_of::<SimpleElement<TW>>(),
                        ElementType::EXTENDED => posting.len() * size_of::<ExtendedElement<TW>>(),
                    },
                quantized_params: param.clone(),
                row_ids_count: posting.len() as RowId,
                max_row_id: posting.elements.last().map(|e| e.row_id()).unwrap_or(0),
            };

            // Step 1.2 Save the header obj to mmap.
            let header_bytes = transmute_to_u8(&header_obj);
            let header_offset_left = dim_id * POSTING_HEADER_SIZE;
            let header_offset_right: usize = (dim_id + 1) * POSTING_HEADER_SIZE;
            headers_mmap[header_offset_left..header_offset_right].copy_from_slice(header_bytes);

            // Step 2.1: Store the posting list to mmap
            match posting.element_type {
                ElementType::SIMPLE => {
                    let elements: Vec<SimpleElement<TW>> = posting.elements.into_iter().map(|e: GenericElement<TW>|e.as_simple().clone()).collect();
                    let posting_elements_bytes = transmute_to_u8_slice(&elements);
                    postings_mmap[cur_postings_storage_size
                        ..(cur_postings_storage_size + posting_elements_bytes.len())]
                        .copy_from_slice(posting_elements_bytes);
                    cur_postings_storage_size += posting_elements_bytes.len();
                },
                ElementType::EXTENDED => {
                    let elements: Vec<ExtendedElement<TW>> = posting.elements.into_iter().map(|e: GenericElement<TW>|e.as_extended().clone()).collect();
                    let posting_elements_bytes = transmute_to_u8_slice(&elements);
                    postings_mmap[cur_postings_storage_size
                        ..(cur_postings_storage_size + posting_elements_bytes.len())]
                        .copy_from_slice(posting_elements_bytes);
                    cur_postings_storage_size += posting_elements_bytes.len();
                },
            }
        }
    }
}
