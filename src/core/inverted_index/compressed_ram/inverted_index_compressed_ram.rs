use std::borrow::Cow;

use log::error;

use crate::core::{
    inverted_index::common::InvertedIndexMetrics, CompressedBlockType, CompressedPostingBuilder,
    CompressedPostingList, DimId, ElementRead, ElementType, InvertedIndexRam,
    InvertedIndexRamAccess, QuantizedParam, QuantizedWeight,
};

#[derive(Debug, Clone)]
pub struct CompressedInvertedIndexRam<TW: QuantizedWeight> {
    pub(super) postings: Vec<CompressedPostingList<TW>>,
    pub(super) element_type: ElementType,
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
        let element_type = ram_index.element_type();

        for dim_id in 0..ram_index.size() {
            let compressed_posting_list = ram_index
                .get(&(dim_id as DimId))
                .map_or(
                    CompressedPostingList::<TW> {
                        row_ids_compressed: vec![],
                        simple_blocks: vec![],
                        extended_blocks: vec![],
                        compressed_block_type: CompressedBlockType::from(ram_index.element_type()),
                        quantization_params: match ram_index.need_quantized {
                            true => Some(QuantizedParam::default()),
                            false => None,
                        },
                        row_ids_count: 0,
                        max_row_id: None,

                    },
                    |posting| {
                        let mut compressed_posting_builder: CompressedPostingBuilder<TW, TW> = CompressedPostingBuilder::<TW, TW>::new(element_type, false, false);

                        for element in &posting.elements {
                            compressed_posting_builder.add(element.row_id(), TW::to_f32(element.weight()));
                        }

                        let mut compressed_posting_list = compressed_posting_builder.build();

                        compressed_posting_list.quantization_params = match ram_index.quantized_params().get(dim_id) {
                            Some(param) => param.clone(),
                            None => {
                                let error_msg = "This error should not occur because the posting must exist in `ram_index`. Its occurrence suggests potential bugs during the construction of `ram_index`.";
                                error!("{}", error_msg);
                                panic!("{}", error_msg);
                            },
                        };

                        compressed_posting_list
                    }
                );
            postings.push(compressed_posting_list);
        }

        Ok(Self { postings, metrics: ram_index.metrics(), element_type })
    }
}

impl<TW: QuantizedWeight> InvertedIndexRamAccess for CompressedInvertedIndexRam<TW> {
    fn size(&self) -> usize {
        self.postings.len()
    }

    fn metrics(&self) -> InvertedIndexMetrics {
        self.metrics
    }

    fn element_type(&self) -> crate::core::ElementType {
        self.element_type
    }
}
