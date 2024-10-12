use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use super::AddBatchReceiver;
use crate::core::SparseRowContent;

#[derive(Clone)]
pub(crate) struct IndexWriterStatus {
    inner: Arc<Inner>,
}

impl IndexWriterStatus {
    /// Returns true if the index writer is alive.
    pub fn is_alive(&self) -> bool {
        self.inner.as_ref().is_alive()
    }

    /// Returns a copy of the operation receiver.
    /// If the index writer was killed, returns `None`.
    pub fn operation_receiver(&self) -> Option<AddBatchReceiver> {
        let rlock = self
            .inner
            .receive_channel
            .read()
            .expect("This lock should never be poisoned");
        rlock.as_ref().cloned()
    }

    /// Create an index writer bomb.
    /// If dropped, the index writer status will be killed.
    pub(crate) fn create_bomb(&self) -> IndexWriterBomb {
        IndexWriterBomb {
            inner: Some(self.inner.clone()),
        }
    }
}

struct Inner {
    is_alive: AtomicBool,
    receive_channel: RwLock<Option<AddBatchReceiver>>,
}

impl Inner {
    fn is_alive(&self) -> bool {
        self.is_alive.load(Ordering::Relaxed)
    }

    fn kill(&self) {
        self.is_alive.store(false, Ordering::Relaxed);
        self.receive_channel
            .write()
            .expect("This lock should never be poisoned")
            .take();
    }
}

impl From<AddBatchReceiver> for IndexWriterStatus {
    fn from(receiver: AddBatchReceiver) -> Self {
        IndexWriterStatus {
            inner: Arc::new(Inner {
                is_alive: AtomicBool::new(true),
                receive_channel: RwLock::new(Some(receiver)),
            }),
        }
    }
}

/// If dropped, the index writer will be killed.
/// To prevent this, clients can call `.defuse()`.
pub(crate) struct IndexWriterBomb {
    inner: Option<Arc<Inner>>,
}

impl IndexWriterBomb {
    /// Defuses the bomb.
    ///
    /// This is the only way to drop the bomb without killing
    /// the index writer.
    pub fn defuse(mut self) {
        self.inner = None;
    }
}

impl Drop for IndexWriterBomb {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use crossbeam_channel as channel;

    use super::IndexWriterStatus;

    #[test]
    fn test_bomb_goes_boom() {
        let (_tx, rx) = channel::bounded(10);
        let index_writer_status: IndexWriterStatus = IndexWriterStatus::from(rx);
        assert!(index_writer_status.operation_receiver().is_some());
        let bomb = index_writer_status.create_bomb();
        assert!(index_writer_status.operation_receiver().is_some());
        mem::drop(bomb);
        // boom!
        assert!(index_writer_status.operation_receiver().is_none());
    }

    #[test]
    fn test_bomb_defused() {
        let (_tx, rx) = channel::bounded(10);
        let index_writer_status: IndexWriterStatus = IndexWriterStatus::from(rx);
        assert!(index_writer_status.operation_receiver().is_some());
        let bomb = index_writer_status.create_bomb();
        bomb.defuse();
        assert!(index_writer_status.operation_receiver().is_some());
    }
}
