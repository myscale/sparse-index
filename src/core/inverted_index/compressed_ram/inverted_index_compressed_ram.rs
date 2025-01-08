use std::{borrow::Cow, path::PathBuf};

use log::error;

use crate::core::{
    inverted_index::common::InvertedIndexMetrics, CompressedBlockType, CompressedPostingBuilder, CompressedPostingList, DimId, ElementRead, ElementType, InvertedIndexRam,
    InvertedIndexRamAccess, PostingList, QuantizedParam, QuantizedWeight,
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
    pub fn from_ram_index(ram_index: Cow<InvertedIndexRam<TW>>, _path: PathBuf, _segment_id: Option<&str>) -> crate::Result<Self> {
        let mut postings = Vec::with_capacity(ram_index.size());
        let element_type = ram_index.element_type();

        let empty_posting_list = PostingList::<TW>::new(ram_index.element_type());

        for dim_id in 0..ram_index.size() {
            // Get the posting list from the ram index.
            let posting_list_in_ram: &PostingList<TW> = ram_index.get(&(dim_id as DimId)).unwrap_or(&empty_posting_list);

            // Compress the posting list.
            let mut compressed_posting_builder: CompressedPostingBuilder<TW, TW> = CompressedPostingBuilder::<TW, TW>::new(element_type, true, false)?;

            // TODO 这个流程可以优化，并不需要逐个的添加到 builder 里面
            for element in &posting_list_in_ram.elements {
                compressed_posting_builder.add(element.row_id(), TW::to_f32(element.weight()));
            }

            let compressed_posting_list = compressed_posting_builder.build()?;

            // TODO 这里的 quantized param 是重新生成了，估计也没有必要从 ram index 中获取
            // compressed_posting_list.quantization_params = match ram_index.quantized_params().get(dim_id) {
            //     Some(param) => param.clone(),
            //     None => {
            //         let error_msg = "This error should not occur because the posting must exist in `ram_index`. Its occurrence suggests potential bugs during the construction of `ram_index`.";
            //         error!("{}", error_msg);
            //         panic!("{}", error_msg);
            //     },
            // };
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
