mod ffi_index_manager;
mod utils;
mod simple_test;
mod ffi_index_reader;
mod cache;

mod ffi_logger_setup;

pub use ffi_index_manager::{
    ffi_commit_index, ffi_create_index, ffi_create_index_with_parameter, ffi_insert_sparse_vector, ffi_free_index_writer
};
pub use ffi_index_reader::{ffi_load_index_reader, ffi_free_index_reader, ffi_sparse_search};

pub use ffi_logger_setup::{sparse_index_log4rs_initialize, sparse_index_log4rs_initialize_with_callback};