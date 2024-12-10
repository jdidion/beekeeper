use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// Context for a task.
#[derive(Debug, Default)]
pub struct Context {
    index: usize,
    attempt: u32,
    cancelled: Arc<AtomicBool>,
}

impl Context {
    /// Creates a new `Context` with the given index and shared cancellation status.
    pub fn new(index: usize, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            index,
            attempt: 0,
            cancelled,
        }
    }

    /// Creates an empty `Context`. This is primarily for testing.
    pub fn empty() -> Self {
        Self {
            index: 0,
            attempt: 0,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The index of this task within the `Hive`.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The current retry attempt. The value is `0` for the first attempt and increments by `1` for
    /// each retry attempt (if any).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) fn inc_attempt(&mut self) {
        self.attempt += 1;
    }

    /// Returns `true` if the task has been cancelled. A long-running `Worker` should check this
    /// periodically and exit early if it returns `true`.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}
