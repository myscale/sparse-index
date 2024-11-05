use std::{
    cmp::{max, min},
    mem::size_of,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use crate::{
    core::{
        atomic_save_json, create_and_ensure_length, madvise, open_write_mmap, transmute_to_u8,
        transmute_to_u8_slice, DimId, InvertedIndex, InvertedIndexMeta, InvertedIndexMmap,
        InvertedIndexMmapFileConfig, InvertedIndexRam, PostingElementEx, PostingList,
        PostingListMerge, PostingListOffset, Revision, Version, DEFAULT_MAX_NEXT_WEIGHT,
        POSTING_OFFSET_SIZE,
    },
    index::{Segment, SegmentReader},
    RowId,
};
use log::info;
use memmap2::MmapMut;

/// Segment's max doc must be `< MAX_DOC_LIMIT`.
///
/// We do not allow segments with more than
pub const MAX_DOC_LIMIT: u32 = 1 << 31;

fn estimate_total_num_tokens_in_single_segment(reader: &SegmentReader) -> crate::Result<u64> {
    // TODO 通过 segment reader 拿到包含的 row 数量，是否需要细致到每个 dim 元素？

    return Ok(100);
}

// 获取一组 segment 所有的 tokens（所有的 dim 元素）
fn estimate_total_num_tokens(readers: &[SegmentReader]) -> crate::Result<u64> {
    let mut total_num_tokens: u64 = 0;
    for reader in readers {
        total_num_tokens += estimate_total_num_tokens_in_single_segment(reader)?;
    }
    Ok(total_num_tokens)
}

pub struct IndexMerger {
    pub(crate) readers: Vec<SegmentReader>,
}

impl IndexMerger {
    pub fn open(segments: &[Segment]) -> crate::Result<IndexMerger> {
        // WARN 编译器隐式转换, map 操作应该是返回 Vec<Result>，但是返回的是 Result<Vec>，这是编译器的隐式转换，如果所有的元素都是 Ok，就正常，若有一个是 Error，就会立刻返回 Error
        let segment_readers: Vec<SegmentReader> = segments
            .iter()
            .map(|seg| SegmentReader::open(seg))
            .collect::<crate::Result<_>>()?;

        Ok(IndexMerger {
            readers: segment_readers,
        })
    }

    fn get_postings_with_dim(&self, dim_id: DimId) -> Vec<&[PostingElementEx]> {
        let mut postings: Vec<&[PostingElementEx]> = vec![];
        let empty_posting: &[PostingElementEx] = &[];

        for segment_reader in self.readers.iter() {
            let inv_idx: &InvertedIndexMmap = segment_reader.get_inverted_index();
            let posting: Option<&[PostingElementEx]> = inv_idx.get(&dim_id);
            postings.push(posting.unwrap_or(empty_posting));
        }

        postings
    }

    /// reduce memory usage when merging
    pub fn merge_v2(
        &self,
        directory: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<InvertedIndexMmap> {
        info!("[merge_v2] record metrics");

        // 记录所有 segments 下 inverted index 的 min_dim 和 max_dim
        let mut min_dim_id = 0;
        let mut max_dim_id = 0;
        // 记录 min_row_id 与 max_row_id
        let mut min_row_id = RowId::MAX;
        let mut max_row_id = RowId::MIN;
        // 记录所有 segments 对应的 vector counts
        let mut total_vector_counts = 0;
        // 记录所有 segments 对应的 postings 占据的字节总数
        let mut total_posting_elements_size: usize = 0;

        for segment_reader in self.readers.iter() {
            let inverted_index = segment_reader.get_inverted_index();
            min_dim_id = min(min_dim_id, inverted_index.min_dim_id());
            max_dim_id = max(max_dim_id, inverted_index.max_dim_id());
            total_posting_elements_size += inverted_index.meta.postings_raw_bytes_size();
            total_vector_counts += inverted_index.meta.vector_count();
            min_row_id = min(min_row_id, inverted_index.meta.min_row_id());
            max_row_id = max(max_row_id, inverted_index.meta.max_row_id());
        }

        info!("[merge_v2] init mmap files, min_dim_id:{}, max_dim_id:{}, min_row_id:{}, max_row_id:{}, total_vector_counts:{}, total_posting_elements_size:{}", min_dim_id, max_dim_id, min_row_id, max_row_id, total_vector_counts, total_posting_elements_size);
        let meta_file_path = directory.join(
            InvertedIndexMmapFileConfig::get_inverted_meta_file_name(segment_id),
        );
        let offsets_mmap_file_path = directory.join(
            InvertedIndexMmapFileConfig::get_posting_offset_file_name(segment_id),
        );
        let postings_mmap_file_path = directory.join(
            InvertedIndexMmapFileConfig::get_posting_data_file_name(segment_id),
        );

        // 创建 offsets 文件
        let total_posting_offsets_size = (max_dim_id + 1) as u64 * POSTING_OFFSET_SIZE as u64;
        create_and_ensure_length(offsets_mmap_file_path.as_ref(), total_posting_offsets_size)?;
        let mut offsets_mmap: MmapMut = open_write_mmap(offsets_mmap_file_path.as_ref())?;
        madvise::madvise(&offsets_mmap, madvise::Advice::Normal)?;

        // 创建 postings 文件
        create_and_ensure_length(
            postings_mmap_file_path.as_ref(),
            total_posting_elements_size as u64,
        )?;
        let mut postings_mmap: MmapMut = open_write_mmap(postings_mmap_file_path.as_ref())?;
        madvise::madvise(&postings_mmap, madvise::Advice::Normal)?;

        info!(
            "[merge_v2] write into files, dim-id range is: [{}, {}]",
            min_dim_id, max_dim_id
        );
        // TODO 是否是要使用 max_dim_id + 1？
        let mut current_element_offset = 0;
        for dim_id in min_dim_id..(max_dim_id + 1) {
            // info!("[merge_v2] > mergings dim id {}", dim_id);
            // 合并当前 dim 维度下所有 segments 的 postings
            let postings: Vec<&[PostingElementEx]> = self.get_postings_with_dim(dim_id);
            // info!("[merge_v2] > got postings with dim {}, postings len: {}", dim_id, postings.len());

            let merged_posting: PostingList = PostingListMerge::merge_posting_lists(&postings);

            // info!("[merge_v2] > merge postings success for dim {}, merged_posting len: {}", dim_id, merged_posting.len());

            // 构造 offset obj 并序列化存储
            let offset_obj = PostingListOffset {
                start_offset: current_element_offset,
                end_offset: current_element_offset
                    + (merged_posting.len() * size_of::<PostingElementEx>()),
            };
            // info!("try transmute to u8 for offset_obj");
            let offset_obj_bytes = transmute_to_u8(&offset_obj);
            let offset_obj_start = dim_id as usize * POSTING_OFFSET_SIZE;
            let offset_obj_end = (dim_id + 1) as usize * POSTING_OFFSET_SIZE;

            // info!("try copy from slice for offset_obj, start:{}, end:{}, bytes:{:?}", offset_obj_start, offset_obj_end, offset_obj_bytes);
            offsets_mmap[offset_obj_start..offset_obj_end].copy_from_slice(offset_obj_bytes);

            // 存储 postings 内部 elements
            // info!("try transmute to u8 for merged_posting");
            let merged_posting_elements_bytes = transmute_to_u8_slice(&merged_posting.elements);
            // info!("try copy from slice for merged_posting");
            postings_mmap[current_element_offset
                ..(current_element_offset + merged_posting_elements_bytes.len())]
                .copy_from_slice(merged_posting_elements_bytes);

            current_element_offset += merged_posting_elements_bytes.len();
        }
        info!("[merge_v2] > try write mmap");

        // 写入 mmap
        if total_posting_offsets_size > 0 {
            offsets_mmap.flush()?;
        }
        if total_posting_elements_size > 0 {
            postings_mmap.flush()?;
        }
        info!("[merge_v2] generate mmap idx");
        // save header properties 实际上就是 meta data
        let meta = InvertedIndexMeta::new(
            (max_dim_id - min_dim_id + 1) as usize,
            total_vector_counts,
            min_row_id,
            max_row_id,
            min_dim_id,
            max_dim_id,
            total_posting_elements_size,
            total_posting_elements_size,
            Version::memory(Revision::V1),
        );

        atomic_save_json(&meta_file_path, &meta)?;

        // info!("merge_v2 sleep 300");
        // panic!("");
        // std::thread::sleep(Duration::from_secs(300));

        Ok(InvertedIndexMmap {
            path: directory.to_owned(),
            offsets_mmap: Arc::new(offsets_mmap.make_read_only()?),
            postings_mmap: Arc::new(postings_mmap.make_read_only()?),
            meta,
        })
    }
}
