use crate::common::errors::SparseError;
use crate::common::executor::Executor;
use census::Inventory;
use log::error;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use super::searcher::{Searcher, SearcherGeneration};

pub const GC_INTERVAL: Duration = Duration::from_secs(1);

/// `Warmer` can be used to maintain segment-level state e.g. caches.
///
/// They must be registered with the [`IndexReaderBuilder`](super::IndexReaderBuilder).
pub trait Warmer: Sync + Send {
    /// Perform any warming work using the provided [`Searcher`].
    fn warm(&self, searcher: &Searcher) -> crate::Result<()>;

    /// Discards internal state for any [`SearcherGeneration`] not provided.
    fn garbage_collect(&self, live_generations: &[&SearcherGeneration]);
}

/// Warming-related state with interior mutability.
#[derive(Clone)]
pub(crate) struct WarmingState(Arc<Mutex<WarmingStateInner>>);

impl WarmingState {
    pub fn new(
        num_warming_threads: usize,
        warmers: Vec<Weak<dyn Warmer>>,
        searcher_generation_inventory: Inventory<SearcherGeneration>,
    ) -> crate::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(WarmingStateInner {
            num_warming_threads,
            warmers,
            gc_thread: None,
            warmed_generation_ids: Default::default(),
            searcher_generation_inventory,
        }))))
    }

    /// Start tracking a new generation of [`Searcher`], and [`Warmer::warm`] it if there are active
    /// warmers.
    ///
    /// A background GC thread for [`Warmer::garbage_collect`] calls is uniquely created if there
    /// are active warmers.
    pub fn warm_new_searcher_generation(&self, searcher: &Searcher) -> crate::Result<()> {
        self.0.lock().unwrap().warm_new_searcher_generation(searcher, &self.0)
    }

    #[cfg(test)]
    fn gc_maybe(&self) -> bool {
        self.0.lock().unwrap().gc_maybe()
    }
}

struct WarmingStateInner {
    num_warming_threads: usize,
    warmers: Vec<Weak<dyn Warmer>>,
    gc_thread: Option<JoinHandle<()>>,
    // Contains all generations that have been warmed up.
    // This list is used to avoid triggers the individual Warmer GCs
    // if no warmed generation needs to be collected.
    warmed_generation_ids: HashSet<u64>,
    searcher_generation_inventory: Inventory<SearcherGeneration>,
}

impl WarmingStateInner {
    /// Start tracking provided searcher as an exemplar of a new generation.
    /// If there are active warmers, warm them with the provided searcher, and kick background GC
    /// thread if it has not yet been kicked. Otherwise, prune state for dropped searcher
    /// generations inline.
    fn warm_new_searcher_generation(
        &mut self,
        searcher: &Searcher,
        this: &Arc<Mutex<Self>>,
    ) -> crate::Result<()> {
        let warmers = self.pruned_warmers();
        if warmers.is_empty() {
            return Ok(());
        }

        self.start_gc_thread_maybe(this)?;
        self.warmed_generation_ids.insert(searcher.generation().generation_id());
        warming_executor(self.num_warming_threads.min(warmers.len()))?
            .map(|warmer| warmer.warm(searcher), warmers.into_iter())?;
        Ok(())
    }

    /// Upgrade and clean up weak references of Warmers, returning strong references (Weak -> Arc).
    fn pruned_warmers(&mut self) -> Vec<Arc<dyn Warmer>> {
        // Upgrade and collect each strong reference (ignore failed upgrades that return None; only collect successful Some).
        let strong_warmers =
            self.warmers.iter().flat_map(|weak_warmer| weak_warmer.upgrade()).collect::<Vec<_>>();

        // Convert the successfully upgraded strong references back to weak references.
        self.warmers = strong_warmers.iter().map(Arc::downgrade).collect();
        strong_warmers
    }

    /// [`Warmer::garbage_collect`] active warmers if some searcher generation is observed to have
    /// been dropped.
    fn gc_maybe(&mut self) -> bool {
        // Get all active SearchGenerations.
        let live_generations = self.searcher_generation_inventory.list();
        // Generate a set of IDs for all active SearchGenerations.
        let live_generation_ids: HashSet<u64> = live_generations
            .iter()
            .map(|searcher_generation| searcher_generation.generation_id())
            .collect();

        // Check if GC is needed.
        // If all warmed SearchGenerations are active, then GC is not required.
        let gc_not_required = self
            .warmed_generation_ids
            .iter()
            .all(|warmed_up_generation| live_generation_ids.contains(warmed_up_generation));
        if gc_not_required {
            return false;
        }

        // Get references to each element in live generations and collect them into a vector.
        let live_generation_refs = live_generations.iter().map(Deref::deref).collect::<Vec<_>>();
        for warmer in self.pruned_warmers() {
            warmer.garbage_collect(&live_generation_refs);
        }
        self.warmed_generation_ids = live_generation_ids;
        true
    }

    /// Trigger the GC thread to run (at most one thread will be triggered).
    fn start_gc_thread_maybe(&mut self, this: &Arc<Mutex<Self>>) -> crate::Result<bool> {
        if self.gc_thread.is_some() {
            return Ok(false);
        }
        let weak_inner = Arc::downgrade(this);
        let handle = std::thread::Builder::new()
            .name("sparse-warm-gc".to_owned())
            .spawn(|| Self::gc_loop(weak_inner))
            .map_err(|_| SparseError::SystemError("Failed to spawn warmer GC thread".to_owned()))?;
        self.gc_thread = Some(handle);
        Ok(true)
    }

    /// Every [`GC_INTERVAL`] attempt to GC, with panics caught and logged using
    /// [`std::panic::catch_unwind`].
    // Periodically execute GC operations, catching and logging any panics that occur.
    fn gc_loop(inner: Weak<Mutex<WarmingStateInner>>) {
        // `crossbeam_channel::tick` generates a timed signal, triggering the loop according to GC_INTERVAL.
        for _ in crossbeam_channel::tick(GC_INTERVAL) {
            if let Some(inner) = inner.upgrade() {
                // rely on deterministic gc in tests

                // In non-test environments, execute the `gc_maybe` function to attempt garbage collection.
                #[cfg(not(test))]
                if let Err(err) = std::panic::catch_unwind(|| inner.lock().unwrap().gc_maybe()) {
                    error!("Panic in Warmer GC {:?}", err);
                }
                // Avoid unused variable warning in tests.
                #[cfg(test)]
                drop(inner);
            }
        }
    }
}

fn warming_executor(num_threads: usize) -> crate::Result<Executor> {
    if num_threads <= 1 {
        Ok(Executor::single_thread())
    } else {
        Executor::multi_thread(num_threads, "tantivy-warm-")
    }
}
