mod channel;
#[cfg(feature = "retry")]
mod delay;
//mod workstealing;

pub use self::channel::ChannelTaskQueues;

use super::{Shared, Task, Token};
use crate::bee::{Queen, Worker};

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
    /// Returns a new instance.
    fn new(token: Token) -> Self;

    /// Initializes the local queues for the given range of worker thread indices.
    fn init_for_threads<Q: Queen<Kind = W>>(
        &self,
        start_index: usize,
        end_index: usize,
        shared: &Shared<Q, Self>,
    );

    /// Changes the size of the local queues to `new_size`.
    #[cfg(feature = "batching")]
    fn resize_local<Q: Queen<Kind = W>>(
        &self,
        start_index: usize,
        end_index: usize,
        new_size: usize,
        shared: &Shared<Q, Self>,
    );

    /// Tries to add a task to the global queue.
    ///
    /// Returns an error with the task if the queue is disconnected.
    fn try_push_global(&self, task: Task<W>) -> Result<(), Task<W>>;

    /// Attempts to add a task to the local queue if space is available, otherwise adds it to the
    /// global queue.
    ///
    /// If adding to the global queue fails, the task is abandoned (converted to an
    /// `Outcome::Unprocessed` and sent to the outcome channel or stored in the hive).
    fn push_local<Q: Queen<Kind = W>>(
        &self,
        task: Task<W>,
        thread_index: usize,
        shared: &Shared<Q, Self>,
    );

    /// Attempts to remove a task from the local queue for the given worker thread index. If there
    /// are no local queues, or if the local queues are empty, falls back to taking a task from the
    /// global queue.
    ///
    /// Returns an error if a task is not available, where each implementation may have a different
    /// definition of "available".
    ///
    /// Also returns an error if the queue is empty or disconnected.
    fn try_pop<Q: Queen<Kind = W>>(
        &self,
        thread_index: usize,
        shared: &Shared<Q, Self>,
    ) -> Result<Task<W>, PopTaskError>;

    /// Drains all tasks from all global and local queues and returns them as a `Vec`.
    fn drain(&self, token: Token) -> Vec<Task<W>>;

    /// Attempts to add `task` to the local retry queue.
    ///
    /// Returns the earliest `Instant` at which it might be retried. If the task could not be added
    /// to the retry queue (e.g., if the queue is full), the task is abandoned (converted to
    /// `Outcome::Unprocessed` and sent to the outcome channel or stored in the hive) and this
    /// method returns `None`.
    #[cfg(feature = "retry")]
    fn retry<Q: Queen<Kind = W>>(
        &self,
        task: Task<W>,
        thread_index: usize,
        shared: &Shared<Q, Self>,
    ) -> Option<std::time::Instant>;

    /// Closes this `GlobalQueue` so no more tasks may be pushed.
    fn close(&self, token: Token);
}
