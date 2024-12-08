use crate::core::common::ops::*;
use crate::core::common::types::{DimId, DimOffset};
use crate::core::posting_list::{ExtendedElement, PostingListIterator};
use crate::core::{
    InvertedIndexMeta, InvertedIndexMetrics, InvertedIndexMmapAccess, InvertedIndexRam,
    InvertedIndexRamAccess, QuantizedParam, QuantizedWeight, Revision, Version, WeightType,
};
use log::warn;
use memmap2::Mmap;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{
    InvertedIndexMmapFileConfig, MmapInvertedIndexMeta, MmapManager, PostingListHeader,
    POSTING_HEADER_SIZE,
};

/// InvertedIndexMmap
/// 
/// OW: weight storage size before quantized.
/// TW: weight storage size after quantized, 
#[derive(Debug, Clone)]
pub struct InvertedIndexMmap<OW: QuantizedWeight, TW: QuantizedWeight> {
    pub path: PathBuf,
    pub headers_mmap: Arc<Mmap>,
    pub postings_mmap: Arc<Mmap>,
    pub meta: MmapInvertedIndexMeta,
    pub _phantom_w: PhantomData<OW>,
    pub _phantom_t: PhantomData<TW>,
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexMmapAccess<OW, TW>
    for InvertedIndexMmap<OW, TW>
{
    // Pay attention to these weight type order.
    type Iter<'a> = PostingListIterator<'a, TW, OW>;

    fn size(&self) -> usize {
        self.meta.inverted_index_meta.posting_count
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

    fn open(path: &Path, segment_id: Option<&str>) -> std::io::Result<Self> {
        Self::load_under_segment(path.to_path_buf(), segment_id)
    }

    fn check_exists(&self, path: &Path, segment_id: Option<&str>) -> std::io::Result<()> {
        debug_assert_eq!(path, self.path);
        for file in self.files(segment_id) {
            debug_assert!(file.exists());
        }
        Ok(())
    }

    fn iter(&self, dim_id: &DimOffset) -> Option<Self::Iter<'_>> {
        let res_opt = self.posting_with_param(dim_id);
        if res_opt.is_none() {
            return None;
        }
        let (posting_list, quantized_param) = res_opt.unwrap();
        let iterator: PostingListIterator<'_, TW, OW> =
            PostingListIterator::new(posting_list, quantized_param);
        Some(iterator)
    }

    fn posting_len(&self, dim_id: &DimId) -> Option<usize> {
        let res_opt = self.posting_with_param(dim_id);
        if res_opt.is_none() {
            return None;
        }
        let (posting_list, _) = res_opt.unwrap();
        Some(posting_list.len())
    }

    fn files(&self, segment_id: Option<&str>) -> Vec<PathBuf> {
        // Only get relative path.
        let get_all_files = InvertedIndexMmapFileConfig::get_all_files(segment_id);
        get_all_files.iter().map(|p| PathBuf::from(p)).collect()
    }

    fn from_ram_index(
        ram_index: Cow<InvertedIndexRam<TW>>,
        path: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Self> {
        Self::convert_and_save(&ram_index, path, segment_id)
    }
}

impl<OW: QuantizedWeight, TW: QuantizedWeight> InvertedIndexMmap<OW, TW> {
    /// Get PostingList obj with given dim-id, the weight type should be TW(may be quantized).
    pub fn posting_with_param(
        &self,
        dim_id: &DimId,
    ) -> Option<(&[ExtendedElement<TW>], Option<QuantizedParam>)> {
        // check that the id is not out of bounds (posting_count includes the empty zeroth entry)
        if *dim_id >= self.size() as DimId {
            warn!("dim_id is overflow, dim_id should smaller than {}", self.size());
            return None;
        }
        // loading header obj with offsets.
        let offset_left = *dim_id as usize * POSTING_HEADER_SIZE;
        let header: PostingListHeader = transmute_from_u8::<PostingListHeader>(
            &self.headers_mmap[offset_left..(offset_left + POSTING_HEADER_SIZE)],
        )
        .clone();

        // loading posting obj
        let elements_bytes = &self.postings_mmap[header.start as usize..header.end as usize];

        // TODO: Make sure this weight type convert operation is safe.
        let posting_slice: &[ExtendedElement<TW>] = transmute_from_u8_to_slice(elements_bytes);

        Some((posting_slice, header.quantized_params))
    }

    /// Converting inverted-index-ram into mmap files.
    /// the weight type in inverted-index-ram may already been quantized.
    pub fn convert_and_save(
        inverted_index_ram: &InvertedIndexRam<TW>,
        directory: PathBuf,
        segment_id: Option<&str>,
    ) -> crate::Result<Self> {
        let (total_headers_storage_size, total_postings_storage_size, headers_mmap, postings_mmap) =
            MmapManager::write_mmap_files(directory.clone(), segment_id, inverted_index_ram)?;

        let meta_file_path = MmapManager::get_index_meta_file_path(&directory.clone(), segment_id);

        let meta = MmapInvertedIndexMeta {
            inverted_index_meta: InvertedIndexMeta {
                posting_count: inverted_index_ram.size(),
                vector_count: inverted_index_ram.metrics().vector_count,
                min_row_id: inverted_index_ram.metrics().min_row_id,
                max_row_id: inverted_index_ram.metrics().max_row_id,
                min_dim_id: inverted_index_ram.metrics().min_dim_id,
                max_dim_id: inverted_index_ram.metrics().max_dim_id,
                quantized: (TW::weight_type() == WeightType::WeightU8)
                    && (OW::weight_type() != TW::weight_type()),
                version: Version::mmap(Revision::V1),
            },
            headers_storage_size: total_headers_storage_size as u64,
            postings_storage_size: total_postings_storage_size as u64,
        };

        atomic_save_json(&meta_file_path, &meta)?;

        Ok(Self {
            path: directory.clone(),
            headers_mmap: headers_mmap.clone(),
            postings_mmap: postings_mmap.clone(),
            meta,
            _phantom_w: PhantomData,
            _phantom_t: PhantomData,
        })
    }

    /// load without segment name.
    pub fn load(path: PathBuf) -> std::io::Result<Self> {
        Self::load_under_segment(path, None)
    }

    /// load with given segment name.
    pub fn load_under_segment(path: PathBuf, segment_id: Option<&str>) -> std::io::Result<Self> {
        // read meta file data.
        let meta_file_path = MmapManager::get_index_meta_file_path(&path, segment_id);
        let meta_data: MmapInvertedIndexMeta = read_json(&meta_file_path)?;

        // read inverted index data.
        let (headers_mmap_file_path, postings_mmap_file_path) =
            MmapManager::get_all_mmap_files_path(&path, segment_id);
        let headers_mmap = open_read_mmap(headers_mmap_file_path.as_ref())?;
        let postings_mmap = open_read_mmap(postings_mmap_file_path.as_ref())?;

        // TODO: Compare different advice's influence on QPS.
        madvise::madvise(&headers_mmap, madvise::Advice::Normal)?;
        madvise::madvise(&postings_mmap, madvise::Advice::Normal)?;

        Ok(Self {
            path: path.clone(),
            headers_mmap: Arc::new(headers_mmap),
            postings_mmap: Arc::new(postings_mmap),
            meta: meta_data,
            _phantom_w: PhantomData,
            _phantom_t: PhantomData,
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
//         inverted_index_mmap: &InvertedIndexMmap<f32, f32>,
//     ) {
//         for id in 0..inverted_index_ram.size() as DimId {
//             let posting_list_ram = inverted_index_ram.get(&id).unwrap().elements.as_slice();
//             let posting_list_mmap = inverted_index_mmap.get(&id).unwrap();
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
//                 InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path, None)
//                     .unwrap();

//             compare_indexes(&inverted_index_ram, &inverted_index_mmap);
//         }
//         let inverted_index_mmap = InvertedIndexMmap::load(&tmp_dir_path).unwrap();
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
