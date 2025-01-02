use crate::atomic::{Atomic, AtomicUsize};
use parking_lot::{Condvar, Mutex};

#[derive(Debug, Default)]
pub struct MutexCondvar {
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl MutexCondvar {
    /// Waits on the condition variable while the condition evaluates to true. The condition is
    /// checked first to avoid aquiring the mutex lock unnecessarily.
    pub fn wait_while<F>(&self, condition: F)
    where
        F: Fn() -> bool,
    {
        if condition() {
            let mut lock = self.mutex.lock();
            while condition() {
                self.condvar.wait(&mut lock);
            }
        }
    }

    pub fn notify_all(&self) {
        let _lock = self.mutex.lock();
        self.condvar.notify_all();
    }
}

/// Like `MutexCondvar`, but the `wait_while` method also depends on a `generation` that changes
/// each time the condition evaluates to `false` after first evaluating to `true`. This prevents a
/// condition that changes rapidly from keeping a thread continually locked.
#[derive(Debug, Default)]
pub struct PhasedCondvar {
    mutex: Mutex<()>,
    condvar: Condvar,
    generation: AtomicUsize,
}

impl PhasedCondvar {
    /// Waits on the condition variable while the condition evaluates to true. The condition is
    /// checked first to avoid aquiring the mutex lock unnecessarily.
    pub fn wait_while<F>(&self, condition: F)
    where
        F: Fn() -> bool,
    {
        if condition() {
            let generation = self.generation.get();
            let mut lock = self.mutex.lock();
            while generation == self.generation.get() && condition() {
                self.condvar.wait(&mut lock);
            }
            // increase generation for the first thread to come out of the loop
            let _ = self
                .generation
                .set_when(generation, generation.wrapping_add(1));
        }
    }

    pub fn notify_all(&self) {
        let _lock = self.mutex.lock();
        self.condvar.notify_all();
    }
}
