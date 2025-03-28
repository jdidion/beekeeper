mod batch;
mod r#impl;
mod iter;
mod queue;
mod store;

pub use self::batch::OutcomeBatch;
pub use self::iter::OutcomeIteratorExt;
pub use self::queue::OutcomeQueue;
pub use self::store::OutcomeStore;

pub(super) use self::store::{DerefOutcomes, OwnedOutcomes};

use crate::bee::{TaskId, Worker};
use crate::panic::Panic;
use derive_more::Debug;

/// The possible outcomes of a task execution.
///
/// Each outcome includes the task ID of the task that produced it. Tasks that submitted
/// subtasks (via [`crate::bee::Context::submit`]) produce `Outcome` variants that have
/// `subtask_ids`.
///
/// Note that `Outcome`s can only be compared or ordered with other `Outcome`s produced by the same
/// `Hive`, because comparison/ordering is completely based on the task ID.
#[derive(Debug)]
pub enum Outcome<W: Worker> {
    /// The task was executed successfully.
    Success {
        #[debug(skip)]
        value: W::Output,
        task_id: TaskId,
    },
    /// The task was executed successfully, and it also submitted one or more subtask_ids to the
    /// `Hive`.
    SuccessWithSubtasks {
        #[debug(skip)]
        value: W::Output,
        task_id: TaskId,
        subtask_ids: Vec<TaskId>,
    },
    /// The task failed with an error that was not retryable. The input value that caused the
    /// failure is provided if possible.
    Failure {
        #[debug(skip)]
        input: Option<W::Input>,
        error: W::Error,
        task_id: TaskId,
    },
    /// The task failed with an error that was not retryable, but it submitted one or more subtask_ids
    /// before failing. The input value that caused the failure is provided if possible.
    FailureWithSubtasks {
        #[debug(skip)]
        input: Option<W::Input>,
        error: W::Error,
        task_id: TaskId,
        subtask_ids: Vec<TaskId>,
    },
    /// The task was not executed before the Hive was dropped, or processing of the task was
    /// interrupted (e.g., by `suspend`ing the `Hive`).
    Unprocessed {
        #[debug(skip)]
        input: W::Input,
        task_id: TaskId,
    },
    /// The task was not executed before the Hive was dropped, or processing of the task was
    /// interrupted (e.g., by `suspend`ing the `Hive`), but it first submitted one or more subtask_ids.
    UnprocessedWithSubtasks {
        #[debug(skip)]
        input: W::Input,
        task_id: TaskId,
        subtask_ids: Vec<TaskId>,
    },
    /// The task with the given task_id was not found in the `Hive` or iterator from which it was
    /// being requested.
    Missing { task_id: TaskId },
    /// The task panicked. The input value that caused the panic is provided if possible.
    Panic {
        #[debug(skip)]
        input: Option<W::Input>,
        payload: Panic<String>,
        task_id: TaskId,
    },
    /// The task panicked, but it submitted one or more subtask_ids before panicking. The input value
    /// that caused the panic is provided if possible.
    PanicWithSubtasks {
        #[debug(skip)]
        input: Option<W::Input>,
        payload: Panic<String>,
        task_id: TaskId,
        subtask_ids: Vec<TaskId>,
    },
    /// The task's weight was larger than the configured limit for the `Hive`.
    #[cfg(feature = "local-batch")]
    WeightLimitExceeded {
        #[debug(skip)]
        input: W::Input,
        weight: u32,
        task_id: TaskId,
    },
    /// The task failed after retrying the maximum number of times.
    #[cfg(feature = "retry")]
    MaxRetriesAttempted {
        #[debug(skip)]
        input: W::Input,
        error: W::Error,
        task_id: TaskId,
    },
}
