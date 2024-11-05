use itertools::Combinations;

use crate::core::common::types::{DimId, DimOffset, ElementOffsetType};
use crate::core::inverted_index::inverted_index_mmap::InvertedIndexMmap;
use crate::core::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::core::inverted_index::InvertedIndex;
use crate::core::posting_list::{PostingList, PostingListIterator};
use crate::core::sparse_vector::SparseVector;
use std::borrow::Cow;
use std::path::Path;

use super::InvertedIndexConfig;

/// A wrapper around [`InvertedIndexRam`].
/// Will be replaced with the new compressed implementation eventually.
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexImmutableRam {
    pub inner: InvertedIndexRam,
}

impl InvertedIndex for InvertedIndexImmutableRam {
    type Iter<'a> = PostingListIterator<'a>;

    type Version = <InvertedIndexMmap as InvertedIndex>::Version;

    fn open(path: &Path, config: Option<InvertedIndexConfig>) -> std::io::Result<Self> {
        let mmap_inverted_index = InvertedIndexMmap::load_with_config(path, config.unwrap_or_default())?;
        let mut inverted_index = InvertedIndexRam {
            postings: Default::default(),
            vector_count: mmap_inverted_index.file_header.vector_count,
            min_row_id: mmap_inverted_index.file_header.min_row_id,
            max_row_id: mmap_inverted_index.file_header.max_row_id,
        };

        for i in 0..mmap_inverted_index.file_header.posting_count as DimId {
            let posting_list = mmap_inverted_index.get(&i).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Posting list {} not found", i),
                )
            })?;
            inverted_index.postings.push(PostingList {
                elements: posting_list.to_owned(),
            });
        }

        Ok(InvertedIndexImmutableRam {
            inner: inverted_index,
        })
    }

    fn save(&self, path: &Path, config: Option<InvertedIndexConfig>) -> std::io::Result<()> {
        InvertedIndexMmap::convert_and_save(&self.inner, path, config.unwrap_or_default())?;
        Ok(())
    }

    fn iter(&self, id: &DimOffset) -> Option<PostingListIterator> {
        InvertedIndex::iter(&self.inner, id)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn posting_size(&self, id: &DimOffset) -> Option<usize> {
        self.inner.posting_size(id)
    }

    fn files(path: &Path, config: InvertedIndexConfig) -> Vec<std::path::PathBuf> {
        InvertedIndexMmap::files(path, config)
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


    // TODO 压缩 immutable ram 的逻辑和这里不一样
    fn from_ram_index<P: AsRef<Path>>(
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
        _config: Option<InvertedIndexConfig>
    ) -> std::io::Result<Self> {
        Ok(InvertedIndexImmutableRam {
            inner: ram_index.into_owned(),
        })
    }

    fn vector_count(&self) -> usize {
        self.inner.vector_count()
    }

    fn max_index(&self) -> Option<DimOffset> {
        self.inner.max_index()
    }
}
