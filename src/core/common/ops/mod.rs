pub mod madvise;

mod bytes_ops;
mod file_ops;
mod mmap_ops;

pub use bytes_ops::*;
pub use file_ops::*;
pub use mmap_ops::*;
