//! The context for a task processed by a `Worker`.
use crate::atomic::{Atomic, AtomicBool};
use std::fmt::Debug;
use std::sync::Arc;

pub type TaskId = usize;

/// Context for a task.
#[derive(Debug, Default)]
pub struct Context {
    task_id: TaskId,
    cancelled: Arc<AtomicBool>,
    #[cfg(feature = "retry")]
    attempt: u32,
}

impl Context {
    /// Creates a new `Context` with the given task_id and shared cancellation status.
    pub fn new(task_id: TaskId, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            task_id,
            cancelled,
            #[cfg(feature = "retry")]
            attempt: 0,
        }
    }

    /// Creates an empty `Context`.
    pub fn empty() -> Self {
        Self::new(0, Arc::new(AtomicBool::from(false)))
    }

    /// The task_id of this task within the `Hive`.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns `true` if the task has been cancelled. A long-running `Worker` should check this
    /// periodically and, if it returns `true`, exit early with an `ApplyError::Cancelled` result.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.get()
    }
}

#[cfg(feature = "retry")]
impl Context {
    /// The current retry attempt. The value is `0` for the first attempt and increments by `1` for
    /// each retry attempt (if any).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Increments the retry attempt.
    pub(crate) fn inc_attempt(&mut self) {
        self.attempt += 1;
    }
}
