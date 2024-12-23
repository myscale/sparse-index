use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use memmap2::{Mmap, MmapMut};

use crate::core::{
    create_and_ensure_length,
    madvise::{self, Advice},
    open_write_mmap, transmute_to_u8, transmute_to_u8_slice, CompressedBlockType,
    CompressedInvertedIndexRam, ExtendedCompressedPostingBlock, InvertedIndexRamAccess,
    QuantizedWeight, SimpleCompressedPostingBlock,
};

use super::{
    CompressedInvertedIndexMmapConfig, CompressedPostingListHeader, COMPRESSED_POSTING_HEADER_SIZE,
};

pub struct CompressedMmapManager;

impl CompressedMmapManager {
    // TODO: Figure out why not move `where` block into args list.
    pub(super) fn get_file_path<F>(directory: &PathBuf, segment_id: Option<&str>, f: F) -> PathBuf
    where
        F: Fn(Option<&str>) -> String,
    {
        directory.join(f(segment_id))
    }

    pub(super) fn get_all_files(
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> (PathBuf, PathBuf, PathBuf) {
        let headers_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::headers_file_name,
        );
        let row_ids_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::row_ids_file_name,
        );
        let blocks_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::blocks_file_name,
        );
        (headers_mmap_file_path, row_ids_mmap_file_path, blocks_mmap_file_path)
    }

    pub(super) fn get_temp_row_ids_and_blocks_mmap_files(
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> (PathBuf, PathBuf) {
        let row_ids_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::row_ids_temp_file_name,
        );
        let blocks_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::blocks_temp_file_name,
        );
        (row_ids_mmap_file_path, blocks_mmap_file_path)
    }

    pub(super) fn get_row_ids_and_blocks_mmap_files(
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> (PathBuf, PathBuf) {
        let row_ids_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::row_ids_file_name,
        );
        let blocks_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::blocks_file_name,
        );
        (row_ids_mmap_file_path, blocks_mmap_file_path)
    }

    pub(super) fn remove_temp_mmap_file(directory: &PathBuf, segment_id: Option<&str>) {
        let row_ids_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::row_ids_temp_file_name,
        );
        let blocks_mmap_file_path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::blocks_temp_file_name,
        );

        // TODO 完善异常处理
        let _ = fs::remove_file(row_ids_mmap_file_path);
        let _ = fs::remove_file(blocks_mmap_file_path);
    }
    pub(super) fn get_index_meta_file_path(
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> PathBuf {
        let path = Self::get_file_path(
            directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::meta_file_name,
        );
        path
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

    pub fn write_mmap_files<TW: QuantizedWeight>(
        directory: &PathBuf,
        segment_id: Option<&str>,
        compressed_inv_index_ram: &CompressedInvertedIndexRam<TW>,
    ) -> crate::Result<(usize, usize, usize, usize, Arc<Mmap>, Arc<Mmap>, Arc<Mmap>)> {
        // compute posting_offsets and elements size.
        let total_headers_storage_size: usize =
            compressed_inv_index_ram.size() * COMPRESSED_POSTING_HEADER_SIZE;

        let (total_row_ids_storage_size, total_blocks_storage_size): (usize, usize) =
            compressed_inv_index_ram.postings().iter().fold(
                (0, 0),
                |(acc_rows, acc_blocks), posting| {
                    let posting_view = posting.view();
                    (
                        acc_rows + posting_view.row_ids_storage_size(),
                        acc_blocks + posting_view.blocks_storage_size(),
                    )
                },
            );

        // 初始化 3 个文件路径.
        let (headers_mmap_file_path, row_ids_mmap_file_path, blocks_mmap_file_path) =
            Self::get_all_files(directory, segment_id);

        // 创建 3 个 mmap 文件.
        let mut headers_mmap = Self::create_mmap_file(
            headers_mmap_file_path.as_ref(),
            total_headers_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        let mut row_ids_mmap = Self::create_mmap_file(
            row_ids_mmap_file_path.as_ref(),
            total_row_ids_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        let mut blocks_mmap = Self::create_mmap_file(
            blocks_mmap_file_path.as_ref(),
            total_blocks_storage_size as u64,
            madvise::Advice::Normal,
        )?;

        let total_blocks_count = Self::save_data_to_mmap::<TW>(
            &mut headers_mmap,
            &mut row_ids_mmap,
            &mut blocks_mmap,
            compressed_inv_index_ram,
        );

        if total_headers_storage_size > 0 {
            headers_mmap.flush()?;
        }
        if total_row_ids_storage_size > 0 {
            row_ids_mmap.flush()?;
        }
        if total_blocks_storage_size > 0 {
            blocks_mmap.flush()?;
        }

        return Ok((
            total_blocks_count,
            total_row_ids_storage_size,
            total_blocks_storage_size,
            total_headers_storage_size,
            Arc::new(headers_mmap.make_read_only()?),
            Arc::new(row_ids_mmap.make_read_only()?),
            Arc::new(blocks_mmap.make_read_only()?),
        ));
    }

    fn save_data_to_mmap<TW: QuantizedWeight>(
        headers_mmap: &mut MmapMut,
        row_ids_mmap: &mut MmapMut,
        blocks_mmap: &mut MmapMut,
        compressed_inv_index_ram: &CompressedInvertedIndexRam<TW>,
    ) -> usize {
        let mut cur_row_ids_storage_size = 0;
        let mut cur_blocks_storage_size = 0;
        let mut total_blocks_count = 0;
        for (dim_id, compressed_posting) in compressed_inv_index_ram.postings().iter().enumerate() {
            let compressed_posting_view = compressed_posting.view();
            // Step 1.1: Generate header
            let header_obj = CompressedPostingListHeader {
                compressed_row_ids_start: cur_row_ids_storage_size,
                compressed_row_ids_end: cur_row_ids_storage_size
                    + compressed_posting_view.row_ids_storage_size(),

                compressed_blocks_start: cur_blocks_storage_size,
                compressed_blocks_end: cur_blocks_storage_size
                    + compressed_posting_view.blocks_storage_size(),

                quantized_params: compressed_posting_view.quantization_params,
                row_ids_count: compressed_posting_view.row_ids_count,
                max_row_id: compressed_posting_view.max_row_id,
                compressed_block_type: CompressedBlockType::from(
                    compressed_inv_index_ram.element_type(),
                ),
            };

            // Step 1.2: Save the offset object to mmap.
            let header_bytes = transmute_to_u8(&header_obj);
            let header_offset_left = dim_id * COMPRESSED_POSTING_HEADER_SIZE;
            let header_offset_right = (dim_id + 1) * COMPRESSED_POSTING_HEADER_SIZE;
            headers_mmap[header_offset_left..header_offset_right].copy_from_slice(header_bytes);

            // Step 2: Store row_ids
            row_ids_mmap[cur_row_ids_storage_size..header_obj.compressed_row_ids_end]
                .copy_from_slice(&compressed_posting_view.row_ids_compressed);

            // Step 3: Store posting blocks
            match compressed_posting_view.compressed_block_type {
                CompressedBlockType::Simple => {
                    let blocks: &[SimpleCompressedPostingBlock<TW>] =
                        compressed_posting_view.simple_blocks;
                    let block_bytes = transmute_to_u8_slice(blocks);
                    blocks_mmap[cur_blocks_storage_size..header_obj.compressed_blocks_end]
                        .copy_from_slice(block_bytes);
                    total_blocks_count += compressed_posting_view.simple_blocks.len();
                }
                CompressedBlockType::Extended => {
                    let blocks: &[ExtendedCompressedPostingBlock<TW>] =
                        compressed_posting_view.extended_blocks;
                    let block_bytes = transmute_to_u8_slice(blocks);
                    blocks_mmap[cur_blocks_storage_size..header_obj.compressed_blocks_end]
                        .copy_from_slice(block_bytes);
                    total_blocks_count += compressed_posting_view.extended_blocks.len();
                }
            }

            // increase offsets.
            cur_row_ids_storage_size = header_obj.compressed_row_ids_end;
            cur_blocks_storage_size = header_obj.compressed_blocks_end;
        }

        return total_blocks_count;
    }
}
