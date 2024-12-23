mod index_reader_bridge;
mod index_writer_bridge;

pub use index_reader_bridge::*;
pub use index_writer_bridge::*;

use once_cell::sync::Lazy;

// Cache store IndexWriterBridgeCache.
pub(super) static FFI_INDEX_WRITER_CACHE: Lazy<IndexWriterBridgeCache> =
    Lazy::new(|| IndexWriterBridgeCache::new());

// Cache store IndexReaderBridgeCache.
pub(super) static FFI_INDEX_SEARCHER_CACHE: Lazy<IndexReaderBridgeCache> =
    Lazy::new(|| IndexReaderBridgeCache::new());
