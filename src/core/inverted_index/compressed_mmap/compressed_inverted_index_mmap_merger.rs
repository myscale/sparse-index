use std::{
    cmp::{max, min},
    marker::PhantomData,
    mem::size_of,
    path::PathBuf,
    sync::Arc,
};

use log::{debug, info};

use crate::{
    core::{
        atomic_save_json, madvise, transmute_from_u8, transmute_to_u8, transmute_to_u8_slice,
        CompressedPostingBlock, CompressedPostingListIterator, CompressedPostingListMerger,
        CompressedPostingListView, DimId, InvertedIndexMeta, InvertedIndexMmapAccess,
        QuantizedWeight, Revision, Version, WeightType, COMPRESSION_BLOCK_SIZE,
    },
    RowId,
};

use super::{
    compressed_posting_list_header::COMPRESSED_POSTING_HEADER_SIZE, CompressedInvertedIndexMmap,
    CompressedMmapInvertedIndexMeta, CompressedMmapManager, CompressedPostingListHeader,
};

pub struct CompressedInvertedIndexMmapMerger<'a, OW: QuantizedWeight, TW: QuantizedWeight> {
    compressed_inverted_index_mmaps: &'a Vec<&'a CompressedInvertedIndexMmap<OW, TW>>,
}

impl<'a, OW: QuantizedWeight, TW: QuantizedWeight> CompressedInvertedIndexMmapMerger<'a, OW, TW> {
    pub fn new(
        compressed_inverted_index_mmaps: &'a Vec<&'a CompressedInvertedIndexMmap<OW, TW>>,
    ) -> Self {
        Self {
            compressed_inverted_index_mmaps,
        }
    }

    fn get_compressed_posting_iterators_with_dim(
        &self,
        dim_id: DimId,
    ) -> Vec<CompressedPostingListIterator<'_, TW, OW>> {
        let mut compressed_postings_iterators = vec![];
        debug!("cur tw:{:?}, ow:{:?}", TW::weight_type(), OW::weight_type());
        for &mmap_index in self.compressed_inverted_index_mmaps {
            let iter_opt = mmap_index.iter(&dim_id);
            if iter_opt.is_some() {
                compressed_postings_iterators.push(iter_opt.unwrap());
            }
        }
        debug!("cur compressed iterators size:{}", compressed_postings_iterators.len());
        compressed_postings_iterators
    }

    pub fn merge(
        &self,
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<CompressedInvertedIndexMmap<OW, TW>> {
        // 记录所有 segments 下 inverted index 的 min_dim 和 max_dim
        let mut min_dim_id = 0;
        let mut max_dim_id = 0;
        // 记录 min_row_id 与 max_row_id
        let mut min_row_id = RowId::MAX;
        let mut max_row_id = RowId::MIN;
        // 记录所有 segments 对应的 vector counts
        let mut total_vector_counts = 0;
        // 对于 Compressed 类型来说，可能简单的加减法计算出来的 size 是不准确的
        let mut total_postings = 0;

        debug!("comp - merger -1");

        let mut approximate_row_ids_storage_size = 0;
        let mut approximate_blocks_storage_size = 0;
        for inverted_index in self.compressed_inverted_index_mmaps.iter() {
            let metrics = inverted_index.metrics();
            min_dim_id = min(min_dim_id, metrics.min_dim_id);
            max_dim_id = max(max_dim_id, metrics.max_dim_id);
            min_row_id = min(min_row_id, metrics.min_row_id);
            max_row_id = max(max_row_id, metrics.max_row_id);

            // TODO 这里是计算最终生成的 mmap 文件的大小
            total_vector_counts += metrics.vector_count;
            approximate_row_ids_storage_size += inverted_index.meta.row_ids_storage_size;
            approximate_blocks_storage_size += inverted_index.meta.blocks_storage_size;
            total_postings += inverted_index.size();
        }
        debug!("comp - merger -2");
        let total_headers_storage_size =
            (max_dim_id - min_dim_id + 1) as u64 * COMPRESSED_POSTING_HEADER_SIZE as u64;
        // row_ids storage size 大致会占用的存储空间
        // let approximate_row_ids_storage_size =
        //     ((total_vector_counts * size_of::<u8>()) as f32 * 1.15) as u64;
        // let approximate_blocks_storage_size =
        //     ((total_vector_counts / COMPRESSION_BLOCK_SIZE + 1 + total_postings)
        //         * size_of::<CompressedPostingBlock<TW>>()) as u64;

        // 初始化 2 个文件路径.
        let (headers_mmap_file_path, _, _) =
            CompressedMmapManager::get_all_files(&directory.clone().to_path_buf(), segment_id);

        let (row_ids_mmap_temp_path, blocks_mmap_temp_path) =
            CompressedMmapManager::get_temp_row_ids_and_blocks_mmap_files(
                &directory.clone(),
                segment_id,
            );

        // 先进行合并，合并完了之后再考虑 创建 2 个 mmap 文件，否则无法确定合并后 Posting 的 size
        let mut headers_mmap = CompressedMmapManager::create_mmap_file(
            headers_mmap_file_path.as_ref(),
            total_headers_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        let mut row_ids_temp_mmap = CompressedMmapManager::create_mmap_file(
            row_ids_mmap_temp_path.as_ref(),
            approximate_row_ids_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        let mut blocks_temp_mmap = CompressedMmapManager::create_mmap_file(
            blocks_mmap_temp_path.as_ref(),
            approximate_blocks_storage_size as u64,
            madvise::Advice::Normal,
        )?;

        let mut true_row_ids_storage_size = 0;
        let mut true_blocks_storage_size = 0;
        let mut total_blocks_count = 0;

        debug!("comp - merger -3");
        for dim_id in min_dim_id..(max_dim_id + 1) {
            // 合并当前 dim 维度下所有 segments 的 postings
            debug!("comp - merger -3 -1");
            let mut compressed_posting_iterators = self.get_compressed_posting_iterators_with_dim(dim_id);
            debug!("comp - merger -3 -2 iterators len {}", compressed_posting_iterators.len());
            // TODO 搞清楚这里的生命周期注解，该如何使用？
            let (merged_compressed_posting, quantized_param) = 
                CompressedPostingListMerger::merge_posting_lists::<OW, TW>(&mut compressed_posting_iterators);
            debug!("comp - merger -3 -3");
            // 参考 manager 里面的代码
            // view 里面的 TW 表示实际上存储的数据类型，即量化后的类型
            let compressed_posting_view: CompressedPostingListView<'_, TW> =
                merged_compressed_posting.view();
            debug!("comp - merger -3 -4");
            // Step 1.1: Generate header
            let header_obj = CompressedPostingListHeader {
                compressed_row_ids_start: true_row_ids_storage_size,
                compressed_row_ids_end: true_row_ids_storage_size
                    + compressed_posting_view.row_ids_storage_size(),

                compressed_blocks_start: true_blocks_storage_size,
                compressed_blocks_end: true_blocks_storage_size
                    + compressed_posting_view.blocks_storage_size(),

                quantized_params: quantized_param,
                row_ids_count: compressed_posting_view.row_ids_count,
                max_row_id: compressed_posting_view.max_row_id,
            };
            // Step 1.2: Save the offset object to mmap.
            let header_bytes = transmute_to_u8(&header_obj);
            let header_offset_left = (dim_id as usize) * COMPRESSED_POSTING_HEADER_SIZE;
            let header_offset_right = (dim_id + 1) as usize * COMPRESSED_POSTING_HEADER_SIZE;
            debug!("comp - merger -3 -5 header/ left:{}, right:{}, copy:{}, total:{}, header:{:?}", header_offset_left, header_offset_right, header_bytes.len(), total_headers_storage_size, header_obj);
            headers_mmap[header_offset_left..header_offset_right].copy_from_slice(header_bytes);
            debug!("comp - merger -3 -6");

            // Step 2: Store row_ids
            debug!("comp - merger -3 -7 row_ids/ left:{}, right:{}, try_copy:{} total:{}", header_obj.compressed_row_ids_start, header_obj.compressed_row_ids_end, compressed_posting_view.row_ids_compressed.len(), approximate_row_ids_storage_size);
            row_ids_temp_mmap
                [header_obj.compressed_row_ids_start..header_obj.compressed_row_ids_end]
                .copy_from_slice(&compressed_posting_view.row_ids_compressed);

            // Step 3: Store posting blocks
            debug!("comp - merger -3 -8 blocks/ left:{}, right:{}, try_copy:{} total:{}", header_obj.compressed_blocks_start, header_obj.compressed_blocks_end, compressed_posting_view.blocks.len(), approximate_blocks_storage_size);
            let block_bytes = transmute_to_u8_slice(&compressed_posting_view.blocks);
            blocks_temp_mmap[header_obj.compressed_blocks_start..header_obj.compressed_blocks_end]
                .copy_from_slice(block_bytes);
            debug!("comp - merger -3 -9");

            total_blocks_count += compressed_posting_view.blocks.len();

            // increase offsets.
            true_row_ids_storage_size += compressed_posting_view.row_ids_storage_size();
            true_blocks_storage_size += compressed_posting_view.blocks_storage_size();
        }

        debug!("comp - merger -4");

        // 写入 header 数据
        if total_headers_storage_size > 0 {
            headers_mmap.flush()?;
        }
        // TODO 写入 mmap
        // TODO 在 flush 操作之前，这些数据都是存储在哪里的，是内存里面吗？如果 flush 之前存储在内存，那么 flush 这个操作可以考虑更加频繁的调用
        if true_row_ids_storage_size > 0 {
            row_ids_temp_mmap.flush()?;
        }
        if true_blocks_storage_size > 0 {
            blocks_temp_mmap.flush()?;
        }

        // 初始化最终 mmap 文件
        let (row_ids_mmap_path, blocks_mmap_path) =
            CompressedMmapManager::get_row_ids_and_blocks_mmap_files(
                &directory.clone(),
                segment_id,
            );
        let mut row_ids_mmap = CompressedMmapManager::create_mmap_file(
            row_ids_mmap_path.as_ref(),
            true_row_ids_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        let mut blocks_mmap = CompressedMmapManager::create_mmap_file(
            blocks_mmap_path.as_ref(),
            true_blocks_storage_size as u64,
            madvise::Advice::Normal,
        )?;
        info!("comp - merger -5");
        for dim_id in min_dim_id..(max_dim_id + 1) {
            let header_start = dim_id as usize * COMPRESSED_POSTING_HEADER_SIZE;
            let header_obj: CompressedPostingListHeader =
                transmute_from_u8::<CompressedPostingListHeader>(
                    &headers_mmap[header_start..(header_start + COMPRESSED_POSTING_HEADER_SIZE)],
                )
                .clone();
            let row_ids_compressed = &row_ids_temp_mmap
                [header_obj.compressed_row_ids_start..header_obj.compressed_row_ids_end];
            row_ids_mmap[header_obj.compressed_row_ids_start..header_obj.compressed_row_ids_end]
                .copy_from_slice(row_ids_compressed);
            let blocks = &blocks_temp_mmap
                [header_obj.compressed_blocks_start..header_obj.compressed_blocks_end];
            blocks_mmap[header_obj.compressed_blocks_start..header_obj.compressed_blocks_end]
                .copy_from_slice(blocks);
        }

        if true_row_ids_storage_size > 0 {
            row_ids_mmap.flush()?;
        }
        if true_blocks_storage_size > 0 {
            blocks_mmap.flush()?;
        }
        CompressedMmapManager::remove_temp_mmap_file(&directory.clone(), segment_id);

        info!("comp - merger -6");
        // save header properties 实际上就是 meta data
        let meta: CompressedMmapInvertedIndexMeta = CompressedMmapInvertedIndexMeta {
            inverted_index_meta: InvertedIndexMeta {
                posting_count: (max_dim_id - min_dim_id + 1) as usize,
                vector_count: total_vector_counts,
                min_row_id,
                max_row_id,
                min_dim_id,
                max_dim_id,
                quantized: (TW::weight_type() == WeightType::WeightU8)
                    && (OW::weight_type() != TW::weight_type()),
                version: Version::compressed_mmap(Revision::V1),
            },
            row_ids_storage_size: true_row_ids_storage_size as u64,
            total_blocks_count: total_blocks_count as u64,
            blocks_storage_size: true_blocks_storage_size as u64,
            headers_storage_size: total_headers_storage_size,
        };
        let meta_file_path =
            CompressedMmapManager::get_index_meta_file_path(&directory.clone(), segment_id);
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
