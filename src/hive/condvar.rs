use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

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
    phase: AtomicUsize,
}

impl PhasedCondvar {
    /// Waits on the condition variable while the condition evaluates to true. The condition is
    /// checked first to avoid aquiring the mutex lock unnecessarily.
    #[inline]
    pub fn wait_while<F>(&self, condition: F)
    where
        F: Fn() -> bool,
    {
        if condition() {
            println!("condition true");
            let phase = self.phase.load(Ordering::SeqCst);
            let mut lock = self.mutex.lock();
            while phase == self.phase.load(Ordering::Relaxed) && condition() {
                self.condvar.wait(&mut lock);
            }
            println!(
                "past wait, phase: {}, condition: {}",
                self.phase.load(Ordering::Relaxed),
                condition()
            );
            // increase generation for the first thread to come out of the loop
            let _ = self.phase.compare_exchange(
                phase,
                phase.wrapping_add(1),
                Ordering::SeqCst,
                Ordering::Relaxed,
            );
        } else {
            println!("condition false");
        }
    }

    pub fn notify_all(&self) {
        let _lock = self.mutex.lock();
        self.condvar.notify_all();
    }
}
