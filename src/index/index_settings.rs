use serde::{Deserialize, Serialize};

use crate::sparse_index::SparseIndexConfig;

/// Search Index Settings.
///
/// Contains settings which are applied on the whole
/// index, like presort documents.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct IndexSettings {
    pub config: SparseIndexConfig,
}
