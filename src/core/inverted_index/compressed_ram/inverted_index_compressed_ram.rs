use std::borrow::Cow;

use crate::core::{
    CompressedPostingBuilder, CompressedPostingList, DimId, InvertedIndexMetrics, InvertedIndexRam,
    InvertedIndexRamAccess, PostingList, QuantizedWeight,
};

#[derive(Debug, Clone, PartialEq)]
pub struct CompressedInvertedIndexRam<TW: QuantizedWeight> {
    pub(super) postings: Vec<CompressedPostingList<TW>>,
    pub(super) metrics: InvertedIndexMetrics,
}

impl<TW: QuantizedWeight> CompressedInvertedIndexRam<TW> {
    pub fn postings(&self) -> &Vec<CompressedPostingList<TW>> {
        &self.postings
    }

    pub fn get(&self, dim_id: &DimId) -> Option<&CompressedPostingList<TW>> {
        self.postings.get(*dim_id as usize)
    }

    // TODO: Refine ram trait.
    pub fn from_ram_index<P: AsRef<std::path::Path>>(
        ram_index: Cow<InvertedIndexRam<TW>>,
        _path: P,
        _segment_id: Option<&str>,
    ) -> std::io::Result<Self> {
        let mut postings = Vec::with_capacity(ram_index.size());
        for dim_id in 0..ram_index.size() {
            let posting_opt = ram_index.get(&(dim_id as DimId));
            if posting_opt.is_none() {
                let empty_posting: CompressedPostingList<TW> = CompressedPostingList::<TW> {
                    row_ids_compressed: vec![],
                    blocks: vec![],
                    quantization_params: None,
                    row_ids_count: 0,
                    max_row_id: None,
                };
                postings.push(empty_posting);
            } else {
                let posting: &PostingList<TW> = posting_opt.unwrap();
                let mut compressed_posting_builder: CompressedPostingBuilder<TW, TW> =
                    CompressedPostingBuilder::<TW, TW>::new();
                for element in &posting.elements {
                    compressed_posting_builder.add(element.row_id, TW::to_f32(element.weight));
                }
                let mut compressed_posting_list = compressed_posting_builder.build();
                // TODO: Refine unwrap
                let quantized_param = ram_index.quantized_params().get(dim_id).unwrap().clone();
                compressed_posting_list.quantization_params = quantized_param;
                postings.push(compressed_posting_list);
            }
        }

        Ok(Self { postings, metrics: ram_index.metrics() })
    }
}

impl<TW: QuantizedWeight> InvertedIndexRamAccess for CompressedInvertedIndexRam<TW> {

    fn size(&self) -> usize {
        self.postings.len()
    }

    fn metrics(&self) -> InvertedIndexMetrics {
        self.metrics
    }
}
