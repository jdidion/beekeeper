//! Implementations of a `gate` that blocks threads waiting on a condition.
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Wraps a `Mutex` and a `Condvar`, and provides methods for threads to wait on a condition and be
/// notified when the condition may have changed.
#[derive(Debug, Default)]
pub struct Gate {
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl Gate {
    /// Waits on the condition variable while the condition evaluates to true. The condition is
    /// checked first to avoid acquiring the mutex lock unnecessarily.
    #[inline]
    pub fn wait_while<F: Fn() -> bool>(&self, condition: F) {
        if condition() {
            let mut lock = self.mutex.lock();
            while condition() {
                self.condvar.wait(&mut lock);
            }
        }
    }

    /// Notifies all waiting threads that the condition may have changed.
    pub fn notify_all(&self) {
        let _lock = self.mutex.lock();
        self.condvar.notify_all();
    }
}

/// A `Gate`, whose `wait_while` method also depends on a `phase` that changes each time the
/// condition evaluates to `false` after first evaluating to `true`. This prevents a condition that
/// changes rapidly from keeping a thread continually locked.
#[derive(Debug, Default)]
pub struct PhasedGate {
    mutex: Mutex<()>,
    condvar: Condvar,
    phase: AtomicUsize,
}

impl PhasedGate {
    /// Waits on the condition variable while the condition evaluates to true *and* the phase
    /// hasn't changed. The first thread to finish waiting during a given phase increments the
    /// phase number. The condition is checked first to avoid aquiring the mutex lock unnecessarily.
    #[inline]
    pub fn wait_while<F: Fn() -> bool>(&self, condition: F) {
        if condition() {
            let phase = self.phase.load(Ordering::SeqCst);
            let mut lock = self.mutex.lock();
            while phase == self.phase.load(Ordering::Relaxed) && condition() {
                self.condvar.wait(&mut lock);
            }
            // increase phase for the first thread to come out of the loop
            let _ = self.phase.compare_exchange(
                phase,
                phase.wrapping_add(1),
                Ordering::SeqCst,
                Ordering::Relaxed,
            );
        }
    }

    /// Notifies all waiting threads that the condition may have changed.
    pub fn notify_all(&self) {
        let _lock = self.mutex.lock();
        self.condvar.notify_all();
    }
}
