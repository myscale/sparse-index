use crate::core::common::ops::*;
use crate::core::common::types::DimId;
use crate::core::inverted_index::common::{
    InvertedIndexMeta, InvertedIndexMetrics, Revision, Version,
};
use crate::core::{
    CompressedBlockType, CompressedInvertedIndexRam, CompressedPostingListIterator,
    CompressedPostingListView, ExtendedCompressedPostingBlock, InvertedIndexMmapAccess,
    InvertedIndexMmapInit, InvertedIndexRam, InvertedIndexRamAccess, PostingListIter,
    PostingListIterAccess, QuantizedWeight, SimpleCompressedPostingBlock, WeightType,
};
use crate::{thread_name, RowId};
use log::{debug, warn};
use memmap2::Mmap;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::COMPRESSED_POSTING_HEADER_SIZE;
use super::{
    CompressedInvertedIndexMmapConfig, CompressedMmapInvertedIndexMeta, CompressedMmapManager,
    CompressedPostingListHeader,
};

/// CompressedInvertedIndexMmap
///
/// OW: weight storage size before quantized.
/// TW: weight storage size after quantized.
#[derive(Debug, Clone)]
pub struct CompressedInvertedIndexMmap<OW: QuantizedWeight, TW: QuantizedWeight> {
    pub path: PathBuf,
    pub headers_mmap: Arc<Mmap>,
    pub row_ids_mmap: Arc<Mmap>,
    pub blocks_mmap: Arc<Mmap>,
    pub meta: CompressedMmapInvertedIndexMeta,
    pub(crate) _ow: PhantomData<OW>,
    pub(crate) _tw: PhantomData<TW>,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexMmapInit<OW, TW>
    for CompressedInvertedIndexMmap<OW, TW>
{
    fn open(path: &Path, segment_id: Option<&str>) -> std::io::Result<Self> {
        Self::load_under_segment(path.to_path_buf(), segment_id)
    }

    fn from_ram_index(
        ram_index: Cow<InvertedIndexRam<TW>>,
        path: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Self> {
        let compressed_inverted_index_ram: CompressedInvertedIndexRam<TW> =
            CompressedInvertedIndexRam::from_ram_index(ram_index, path.clone(), segment_id)?;
        Self::convert_and_save(&compressed_inverted_index_ram, &path, segment_id)
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> PostingListIterAccess<OW, TW>
    for CompressedInvertedIndexMmap<OW, TW>
{
    type Iter<'a> = CompressedPostingListIterator<'a, OW, TW>;

    fn iter(&self, dim_id: &DimId) -> Option<Self::Iter<'_>> {
        let res_opt: Option<CompressedPostingListView<'_, TW>> = self.posting_with_param(dim_id);
        if res_opt.is_none() {
            return None;
        }
        let view = res_opt.unwrap();

        // When using iterator peek func, you will get a `OW` type of weight.
        let iterator: CompressedPostingListIterator<'_, OW, TW> =
            CompressedPostingListIterator::<OW, TW>::new(&view);

        debug!(
            "[{}]-[cmp-mmap]-[iter] TW:{:?}, OW:{:?}, quantize param:{:?}, iter size:{}",
            thread_name!(),
            TW::weight_type(),
            OW::weight_type(),
            view.quantization_params,
            iterator.remains()
        );
        return Some(iterator);
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexMmapAccess<OW, TW>
    for CompressedInvertedIndexMmap<OW, TW>
{
    fn size(&self) -> usize {
        self.meta.inverted_index_meta.posting_count
    }

    // fn check_exists(&self, path: &Path, segment_id: Option<&str>) -> std::io::Result<()> {
    //     debug_assert_eq!(path, self.path);
    //     for file in self.files(segment_id) {
    //         debug_assert!(file.exists());
    //     }
    //     Ok(())
    // }

    fn posting_len(&self, dim_id: &DimId) -> Option<usize> {
        let res_opt: Option<CompressedPostingListView<'_, TW>> = self.posting_with_param(dim_id);
        if res_opt.is_none() {
            return None;
        }
        let posting_list_view = res_opt.unwrap();

        Some(posting_list_view.row_ids_count as usize)
    }

    fn files(&self, segment_id: Option<&str>) -> Vec<PathBuf> {
        // relative paths
        let get_all_files = CompressedInvertedIndexMmapConfig::get_all_files(segment_id);
        get_all_files.iter().map(|p| PathBuf::from(p)).collect()
    }

    fn metrics(&self) -> InvertedIndexMetrics {
        InvertedIndexMetrics {
            min_row_id: self.meta.inverted_index_meta.min_row_id,
            max_row_id: self.meta.inverted_index_meta.max_row_id,
            min_dim_id: self.meta.inverted_index_meta.min_dim_id,
            max_dim_id: self.meta.inverted_index_meta.max_dim_id,
            vector_count: self.meta.inverted_index_meta.vector_count,
        }
    }

    fn empty(&self) -> bool {
        self.size() == 0
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> CompressedInvertedIndexMmap<OW, TW> {
    pub fn min_row_id(&self) -> RowId {
        self.meta.inverted_index_meta.min_row_id
    }

    pub fn max_row_id(&self) -> RowId {
        self.meta.inverted_index_meta.max_row_id
    }

    fn slice_part<T>(&self, start: u64, count: u64) -> &[T] {
        let start = start as usize;
        let end = start + count as usize * size_of::<T>();
        transmute_from_u8_to_slice(&self.blocks_mmap[start..end])
    }

    /// Get `CompressedPostingList` with given dim-id.
    /// Not need consider about quantized.
    /// `TW` means weight storage type in disk.
    pub fn posting_with_param(&self, dim_id: &DimId) -> Option<CompressedPostingListView<TW>> {
        // check that the id is not out of bounds (posting_count includes the empty zeroth entry)
        if *dim_id >= self.meta.inverted_index_meta.posting_count as DimId {
            warn!(
                "dim_id is overflow, dim_id should smaller than {}",
                self.meta.inverted_index_meta.posting_count
            );
            return None;
        }
        // loding header-obj with given offsets.
        let header_start = *dim_id as usize * COMPRESSED_POSTING_HEADER_SIZE;
        let header_obj: CompressedPostingListHeader =
            transmute_from_u8::<CompressedPostingListHeader>(
                &self.headers_mmap[header_start..(header_start + COMPRESSED_POSTING_HEADER_SIZE)],
            )
            .clone();

        // TODO: Figure out about transfer of owner ship.
        let row_ids_compressed = &self.row_ids_mmap
            [header_obj.compressed_row_ids_start..header_obj.compressed_row_ids_end];
        let blocks: &[u8] =
            &self.blocks_mmap[header_obj.compressed_blocks_start..header_obj.compressed_blocks_end];
        // Convert into Blocks type.
        match header_obj.compressed_block_type {
            CompressedBlockType::Simple => {
                let raw_simple_blocks: &[SimpleCompressedPostingBlock<TW>] =
                    transmute_from_u8_to_slice(blocks);
                Some(CompressedPostingListView {
                    row_ids_compressed,
                    simple_blocks: raw_simple_blocks,
                    extended_blocks: &[],
                    compressed_block_type: header_obj.compressed_block_type,
                    quantization_params: header_obj.quantized_params,
                    row_ids_count: header_obj.row_ids_count,
                    max_row_id: header_obj.max_row_id,
                })
            }
            CompressedBlockType::Extended => {
                let raw_extended_blocks: &[ExtendedCompressedPostingBlock<TW>] =
                    transmute_from_u8_to_slice(blocks);
                Some(CompressedPostingListView {
                    row_ids_compressed,
                    simple_blocks: &[],
                    extended_blocks: raw_extended_blocks,
                    compressed_block_type: header_obj.compressed_block_type,
                    quantization_params: header_obj.quantized_params,
                    row_ids_count: header_obj.row_ids_count,
                    max_row_id: header_obj.max_row_id,
                })
            }
        }
    }

    /// Store inverted-index-ram into mmap files.
    pub fn convert_and_save(
        compressed_inv_index_ram: &CompressedInvertedIndexRam<TW>,
        directory: &PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Self> {
        let (
            total_blocks_count,
            total_row_ids_storage_size,
            total_blocks_storage_size,
            total_headers_storage_size,
            headers_mmap,
            row_ids_mmap,
            blocks_mmap,
        ) = CompressedMmapManager::write_mmap_files(
            directory,
            segment_id,
            compressed_inv_index_ram,
        )?;

        // TODO: Refine pathBuf.
        let meta_file_path = CompressedMmapManager::get_file_path(
            &directory,
            segment_id,
            CompressedInvertedIndexMmapConfig::meta_file_name,
        );

        let meta: CompressedMmapInvertedIndexMeta = CompressedMmapInvertedIndexMeta {
            inverted_index_meta: InvertedIndexMeta::new(
                compressed_inv_index_ram.size(),
                compressed_inv_index_ram.metrics().vector_count,
                compressed_inv_index_ram.metrics().min_row_id,
                compressed_inv_index_ram.metrics().max_row_id,
                compressed_inv_index_ram.metrics().min_dim_id,
                compressed_inv_index_ram.metrics().max_dim_id,
                (TW::weight_type() == WeightType::WeightU8) && (OW::weight_type() != TW::weight_type()),
                compressed_inv_index_ram.element_type(),
                Version::compressed_mmap(Revision::V1),
            ),
            row_ids_storage_size: total_row_ids_storage_size as u64,
            headers_storage_size: total_headers_storage_size as u64,
            blocks_storage_size: total_blocks_storage_size as u64,
            total_blocks_count: total_blocks_count as u64,
        };

        atomic_save_json(&meta_file_path, &meta)?;

        Ok(Self {
            path: directory.clone(),
            headers_mmap,
            row_ids_mmap,
            blocks_mmap,
            meta,
            _ow: PhantomData,
            _tw: PhantomData,
        })
    }

    /// load without segment name.
    pub fn load(path: PathBuf) -> std::io::Result<Self> {
        Self::load_under_segment(path, None)
    }

    /// load with given segment name.
    pub fn load_under_segment(path: PathBuf, segment_id: Option<&str>) -> std::io::Result<Self> {
        // init directory
        let (headers_mmap_file_path, row_ids_mmap_file_path, blocks_mmap_file_path) =
            CompressedMmapManager::get_all_files(&path.clone(), segment_id);
        let meta_file_path = CompressedMmapManager::get_file_path(
            &path.clone(),
            segment_id,
            CompressedInvertedIndexMmapConfig::meta_file_name,
        );

        // read meta file data.
        let meta: CompressedMmapInvertedIndexMeta = read_json(&meta_file_path)?;
        // read inverted index data.
        let headers_mmap = open_read_mmap(headers_mmap_file_path.as_ref())?;
        let row_ids_mmap = open_read_mmap(row_ids_mmap_file_path.as_ref())?;
        let blocks_mmap = open_read_mmap(blocks_mmap_file_path.as_ref())?;

        // TODO: Compare different advice's influence on QPS.
        madvise::madvise(&headers_mmap, madvise::Advice::Normal)?;
        madvise::madvise(&row_ids_mmap, madvise::Advice::Normal)?;
        madvise::madvise(&blocks_mmap, madvise::Advice::Normal)?;

        Ok(Self {
            path: path.clone(),
            headers_mmap: Arc::new(headers_mmap),
            row_ids_mmap: Arc::new(row_ids_mmap),
            blocks_mmap: Arc::new(blocks_mmap),
            meta,
            _ow: PhantomData,
            _tw: PhantomData,
        })
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::core::InvertedIndexRamBuilder;

//     use super::*;
//     use tempfile::Builder;

//     fn compare_indexes(
//         inverted_index_ram: &InvertedIndexRam<f32>,
//         compressed_inverted_index_mmap: &CompressedInvertedIndexMmap<f32>,
//     ) {
//         for id in 0..inverted_index_ram.size() as DimId {
//             let posting_list_ram = inverted_index_ram.get(&id).unwrap().elements.as_slice();
//             let posting_list_mmap = compressed_inverted_index_mmap.get(&id).unwrap();
//             assert_eq!(posting_list_ram.len(), posting_list_mmap.len());
//             for i in 0..posting_list_ram.len() {
//                 assert_eq!(posting_list_ram[i], posting_list_mmap[i]);
//             }
//         }
//     }

//     #[test]
//     fn test_inverted_index_mmap() {
//         // skip 4th dimension
//         let mut builder = InvertedIndexRamBuilder::new();
//         builder.add(1, [(1, 10.0), (2, 10.0), (3, 10.0), (5, 10.0)].into());
//         builder.add(2, [(1, 20.0), (2, 20.0), (3, 20.0), (5, 20.0)].into());
//         builder.add(3, [(1, 30.0), (2, 30.0), (3, 30.0)].into());
//         builder.add(4, [(1, 1.0), (2, 1.0)].into());
//         builder.add(5, [(1, 2.0)].into());
//         builder.add(6, [(1, 3.0)].into());
//         builder.add(7, [(1, 4.0)].into());
//         builder.add(8, [(1, 5.0)].into());
//         builder.add(9, [(1, 6.0)].into());
//         let inverted_index_ram = builder.build();

//         let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();

//         {
//             let inverted_index_mmap =
//             CompressedInvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path, None)
//                     .unwrap();

//             compare_indexes(&inverted_index_ram, &inverted_index_mmap);
//         }
//         let inverted_index_mmap = CompressedInvertedIndexMmap::load(&tmp_dir_path).unwrap();
//         // posting_count: 0th entry is always empty + 1st + 2nd + 3rd + 4th empty + 5th
//         assert_eq!(inverted_index_mmap.size(), 6);
//         assert_eq!(inverted_index_mmap.vector_count(), 9);

//         compare_indexes(&inverted_index_ram, &inverted_index_mmap);

//         assert!(inverted_index_mmap.get(&0).unwrap().is_empty()); // the first entry is always empty as dimension ids start at 1
//         assert_eq!(inverted_index_mmap.get(&1).unwrap().len(), 9);
//         assert_eq!(inverted_index_mmap.get(&2).unwrap().len(), 4);
//         assert_eq!(inverted_index_mmap.get(&3).unwrap().len(), 3);
//         assert!(inverted_index_mmap.get(&4).unwrap().is_empty()); // return empty posting list info for intermediary empty ids
//         assert_eq!(inverted_index_mmap.get(&5).unwrap().len(), 2);
//         // index after the last values are None
//         assert!(inverted_index_mmap.get(&6).is_none());
//         assert!(inverted_index_mmap.get(&7).is_none());
//         assert!(inverted_index_mmap.get(&100).is_none());
//     }
// }
