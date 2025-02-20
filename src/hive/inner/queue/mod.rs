mod channel;
#[cfg(feature = "retry")]
mod retry;
mod status;
mod workstealing;

pub use self::channel::ChannelTaskQueues;
pub use self::workstealing::WorkstealingTaskQueues;

#[cfg(feature = "retry")]
use self::retry::RetryQueue;
use self::status::Status;
use super::{Config, Task, Token};
use crate::bee::Worker;

/// Errors that may occur when trying to pop tasks from the global queue.
#[derive(thiserror::Error, Debug)]
pub enum PopTaskError {
    #[error("Global task queue is empty")]
    Empty,
    #[error("Global task queue is closed")]
    Closed,
}

/// Trait that encapsulates the global and local task queues used by a `Hive` for managing tasks
/// within and between worker threads.
///
/// This trait is sealed - it cannot be implemented outside of this crate.
pub trait TaskQueues<W: Worker>: Sized + Send + Sync + 'static {
    type WorkerQueues: WorkerQueues<W>;

    /// Returns a new instance.
    ///
    /// The private `Token` is used to prevent this method from being called externally.
    fn new(token: Token) -> Self;

    /// Initializes the local queues for the given range of worker thread indices.
    fn init_for_threads(&self, start_index: usize, end_index: usize, config: &Config);

    /// Updates the queue settings from `config` for the given range of worker threads.
    fn update_for_threads(&self, start_index: usize, end_index: usize, config: &Config);

    /// Tries to add a task to the global queue.
    ///
    /// Returns an error with the task if the queue is disconnected.
    fn try_push_global(&self, task: Task<W>) -> Result<(), Task<W>>;

    /// Returns a `WorkerQueues` instance for the worker thread with the given `index`.
    fn worker_queues(&self, thread_index: usize) -> Self::WorkerQueues;

    /// Closes this `GlobalQueue` so no more tasks may be pushed.
    ///
    /// If `urgent` is `true`, this also prevents queued tasks from being popped.
    ///
    /// The private `Token` is used to prevent this method from being called externally.
    fn close(&self, urgent: bool, token: Token);

    /// Consumes this `TaskQueues` and Drains all tasks from all global and local queues and
    /// returns them as a `Vec`.
    ///
    /// This method panics if `close` has not been called.
    fn drain(self) -> Vec<Task<W>>;
}

/// Trait that provides access to the task queues to each worker thread. Implementations of this
/// trait can hold thread-local types that are not Send/Sync.
pub trait WorkerQueues<W: Worker> {
    /// Attempts to add a task to the local queue if space is available, otherwise adds it to the
    /// global queue. If adding to the global queue fails, the task is added to a local "abandoned"
    /// queue from which it may be popped or will otherwise be converted to an `Unprocessed`
    /// outcome.
    fn push(&self, task: Task<W>);

    /// Attempts to remove a task from the local queue for the given worker thread index. If there
    /// are no local queues, or if the local queues are empty, falls back to taking a task from the
    /// global queue.
    ///
    /// Returns an error if a task is not available, where each implementation may have a different
    /// definition of "available".
    ///
    /// Also returns an error if the queues are closed.
    fn try_pop(&self) -> Result<Task<W>, PopTaskError>;

    /// Attempts to add `task` to the local retry queue.
    ///
    /// Returns the earliest `Instant` at which it might be retried. If the task could not be added
    /// to the retry queue (e.g., if the queue is full), the task returned as an error.
    #[cfg(feature = "retry")]
    fn try_push_retry(&self, task: Task<W>) -> Result<std::time::Instant, Task<W>>;
}
