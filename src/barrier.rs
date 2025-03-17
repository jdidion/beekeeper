use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::{self, ThreadId};

/// Enables multiple threads to synchronize the beginning of some computation. Unlike
/// [`std::sync::Barrier`], this one keeps track of which threads have reached it and only
/// recognizes the first wait from each thread.
#[derive(Clone)]
pub struct IndexedBarrier(Arc<Inner>);

struct Inner {
    barrier: Barrier,
    threads_seen: RwLock<HashSet<ThreadId>>,
    is_crossed: AtomicBool,
}

impl IndexedBarrier {
    pub fn new(num_threads: usize) -> Self {
        Self(Arc::new(Inner {
            barrier: Barrier::new(num_threads + 1),
            threads_seen: RwLock::new(HashSet::with_capacity(num_threads + 1)),
            is_crossed: AtomicBool::new(false),
        }))
    }

    /// Wait for all threads to reach this barrier. Returns `None` if the barrier has already been
    /// crossed, or if this thread has already called `wait` on this barrier (which should never
    /// happen). Otherwise returns `Some(is_leader)`, where `is_leader is `true` for a single,
    /// arbitrary thread.
    pub fn wait(&self) -> Option<bool> {
        if self.0.is_crossed.load(Ordering::Acquire) {
            return None;
        }
        let thread_id = thread::current().id();
        if !self.0.threads_seen.read().contains(&thread_id) {
            self.0.threads_seen.write().insert(thread_id);
        } else {
            return None;
        }
        let is_leader = self.0.barrier.wait().is_leader();
        if is_leader {
            self.0.is_crossed.store(true, Ordering::Release);
        }
        Some(is_leader)
    }
}
