use core::fmt;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::INDEX_FORMAT_VERSION;

/// Structure version for the index.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub index_format_version: u32,
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub static VERSION: Lazy<Version> = Lazy::new(|| Version {
    major: env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap(),
    minor: env!("CARGO_PKG_VERSION_MINOR").parse().unwrap(),
    patch: env!("CARGO_PKG_VERSION_PATCH").parse().unwrap(),
    index_format_version: INDEX_FORMAT_VERSION,
});

impl ToString for Version {
    fn to_string(&self) -> String {
        format!("tantivy v{}.{}.{}, index_format v{}", self.major, self.minor, self.patch, self.index_format_version)
    }
}

static VERSION_STRING: Lazy<String> = Lazy::new(|| VERSION.to_string());

/// Expose the current version of tantivy as found in Cargo.toml during compilation.
/// eg. "0.11.0" as well as the compression scheme used in the docstore.
pub fn version() -> &'static Version {
    &VERSION
}

/// Exposes the complete version of tantivy as found in Cargo.toml during compilation as a string.
/// eg. "tantivy v0.11.0, index_format v1, store_compression: lz4".
pub fn version_string() -> &'static str {
    VERSION_STRING.as_str()
}
