use crate::core::common::types::{DimId, DimOffset, ElementOffsetType, Weight};
use crate::core::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use crate::core::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::core::inverted_index::InvertedIndex;
use crate::core::posting_list::{
    CompressedPostingBuilder, CompressedPostingList, CompressedPostingListIterator, PostingListIter,
};
use crate::core::sparse_vector::SparseVector;
use std::borrow::Cow;
use std::path::Path;

use super::InvertedIndexConfig;

#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexCompressedImmutableRam<W: Weight> {
    pub(super) postings: Vec<CompressedPostingList<W>>,
    pub(super) vector_count: usize,
}

impl<W: Weight> InvertedIndexCompressedImmutableRam<W> {
    pub(super) fn into_postings(self) -> Vec<CompressedPostingList<W>> {
        self.postings
    }
}

impl<W: Weight> InvertedIndex for InvertedIndexCompressedImmutableRam<W> {
    type Iter<'a> = CompressedPostingListIterator<'a, W>;

    type Version = <InvertedIndexCompressedMmap<W> as InvertedIndex>::Version;

    fn open_with_config(path: &Path, config: InvertedIndexConfig) -> std::io::Result<Self> {
        // 使用 mmap 加载索引文件
        let mmap_inverted_index = InvertedIndexCompressedMmap::load_with_config(path, config)?;

        // 初始化 ram 类型的 inverted index
        let mut inverted_index: InvertedIndexCompressedImmutableRam<W> = InvertedIndexCompressedImmutableRam {
            postings: Vec::with_capacity(mmap_inverted_index.file_header.posting_count),
            vector_count: mmap_inverted_index.file_header.vector_count,
        };

        // 填充 postings 至 ram 类型的 inverted index 内
        for i in 0..mmap_inverted_index.file_header.posting_count as DimId {
            let posting_list = mmap_inverted_index.get(&i).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Posting list {} not found", i),
                )
            })?;
            inverted_index.postings.push(posting_list.to_owned());
        }

        Ok(inverted_index)
    }

    fn open(path: &Path) -> std::io::Result<Self> {
        return InvertedIndexCompressedImmutableRam::open_with_config(path, InvertedIndexConfig::default());
    }

    fn save_with_config(&self, path: &Path, config: InvertedIndexConfig) -> std::io::Result<()> {
        InvertedIndexCompressedMmap::convert_and_save(self, path, config)?;
        Ok(())
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        return InvertedIndexCompressedImmutableRam::save_with_config(&self, path, InvertedIndexConfig::default());
    }

    fn get(&self, id: &DimId) -> Option<Self::Iter<'_>> {
        self.postings
            .get(*id as usize)
            .map(|posting_list| posting_list.iter())
    }

    fn len(&self) -> usize {
        self.postings.len()
    }

    fn posting_size(&self, id: &DimOffset) -> Option<usize> {
        self.get(id).map(|posting_list| posting_list.len_to_end())
    }

    fn files(path: &Path, config: InvertedIndexConfig) -> Vec<std::path::PathBuf> {
        InvertedIndexCompressedMmap::<W>::files(path, config)
    }

    fn remove(&mut self, _id: ElementOffsetType, _old_vector: SparseVector) {
        panic!("Cannot remove from a read-only RAM inverted core")
    }

    fn upsert(
        &mut self,
        _id: ElementOffsetType,
        _vector: SparseVector,
        _old_vector: Option<SparseVector>,
    ) {
        panic!("Cannot upsert into a read-only RAM inverted core")
    }

    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
        _config: Option<InvertedIndexConfig>,
    ) -> std::io::Result<Self> {
        let mut postings = Vec::with_capacity(ram_index.postings.len());
        for old_posting_list in &ram_index.postings {
            let mut new_posting_list = CompressedPostingBuilder::new();
            for elem in &old_posting_list.elements {
                new_posting_list.add(elem.row_id, elem.weight);
            }
            postings.push(new_posting_list.build());
        }
        Ok(InvertedIndexCompressedImmutableRam {
            postings,
            vector_count: ram_index.vector_count,
        })
    }

    fn vector_count(&self) -> usize {
        self.vector_count
    }

    fn max_index(&self) -> Option<DimOffset> {
        self.postings
            .len()
            .checked_sub(1)
            .map(|len| len as DimOffset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::common::types::QuantizedU8;
    use crate::core::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;
    use crate::core::sparse_vector::utils::random_sparse_vector;
    use tempfile::Builder;

    #[test]
    fn test_save_load_tiny() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap());
        builder.add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap());
        builder.add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap());
        let inverted_index_ram = builder.build();

        check_save_load::<f32>(&inverted_index_ram);
        check_save_load::<half::f16>(&inverted_index_ram);
        check_save_load::<u8>(&inverted_index_ram);
        check_save_load::<QuantizedU8>(&inverted_index_ram);
    }

    // #[test]
    // fn test_save_load_large() {
    //     let mut rnd_gen = rand::thread_rng();
    //     let mut builder = InvertedIndexBuilder::new();
    //     // Enough elements to put some of them into chunks
    //     for i in 0..1024 {
    //         builder.add(i, random_sparse_vector(&mut rnd_gen, 3).into_remapped());
    //     }
    //     let inverted_index_ram = builder.build();

    //     check_save_load::<f32>(&inverted_index_ram);
    //     check_save_load::<half::f16>(&inverted_index_ram);
    //     check_save_load::<u8>(&inverted_index_ram);
    //     check_save_load::<QuantizedU8>(&inverted_index_ram);
    // }

    fn check_save_load<W: Weight>(inverted_index_ram: &InvertedIndexRam) {
        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();
        let inverted_index_immutable_ram =
            InvertedIndexCompressedImmutableRam::<W>::from_ram_index(
                Cow::Borrowed(inverted_index_ram),
                tmp_dir_path.path(),
                None
            )
            .unwrap();
        inverted_index_immutable_ram
            .save(tmp_dir_path.path())
            .unwrap();

        let loaded_inverted_index =
            InvertedIndexCompressedImmutableRam::<W>::open(tmp_dir_path.path()).unwrap();
        assert_eq!(inverted_index_immutable_ram, loaded_inverted_index);
    }
}
