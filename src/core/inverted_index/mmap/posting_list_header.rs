use crate::core::QuantizedParam;

#[derive(Debug, Default, Clone)]
pub struct PostingListHeader {
    // offset for postings
    pub start: usize,
    pub end: usize,

    // Fix sized: header for compressed posting
    pub quantized_params: Option<QuantizedParam>,

    // TODO: refine these vars.
    pub row_ids_count: u32,
    pub max_row_id: u32,
}

pub const POSTING_HEADER_SIZE: usize = std::mem::size_of::<PostingListHeader>();
