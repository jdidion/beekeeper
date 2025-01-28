use crate::bee::{ApplyError, TaskId, Worker, WorkerResult};
use crate::panic::Panic;
use std::cmp::Ordering;
use std::fmt::Debug;

/// The possible outcomes of a task execution.
///
/// Each outcome includes the task ID of the task that produced it.
///
/// Note that `Outcome`s can only be compared or ordered with other `Outcome`s produced by the same
/// `Hive`, because comparison/ordering is completely based on the task ID.
#[derive(Debug)]
pub enum Outcome<W: Worker> {
    /// The task was executed successfully.
    Success { value: W::Output, task_id: TaskId },
    /// The task failed with an error that was not retryable. The input value that caused the
    /// failure is provided if possible.
    Failure {
        input: Option<W::Input>,
        error: W::Error,
        task_id: TaskId,
    },
    /// The task was not executed before the Hive was dropped, or processing of the task was
    /// interrupted (e.g., by `suspend`ing the `Hive`).
    Unprocessed { input: W::Input, task_id: TaskId },
    /// The task with the given task_id was not found in the `Hive` or iterator from which it was
    /// being requested.
    Missing { task_id: TaskId },
    /// The task panicked. The input value that caused the panic is provided if possible.
    Panic {
        input: Option<W::Input>,
        payload: Panic<String>,
        task_id: TaskId,
    },
    /// The task failed after retrying the maximum number of times.
    #[cfg(feature = "retry")]
    MaxRetriesAttempted {
        input: W::Input,
        error: W::Error,
        task_id: TaskId,
    },
}

impl<W: Worker> Outcome<W> {
    /// Converts a worker `result` into an `Outcome` with the given task_id.
    pub(in crate::hive) fn from_worker_result(result: WorkerResult<W>, task_id: TaskId) -> Self {
        match result {
            Ok(value) => Self::Success { task_id, value },
            Err(ApplyError::Retryable { input, error }) => {
                #[cfg(feature = "retry")]
                {
                    Self::MaxRetriesAttempted {
                        input,
                        error,
                        task_id,
                    }
                }
                #[cfg(not(feature = "retry"))]
                {
                    Self::Failure {
                        input: Some(input),
                        error,
                        task_id,
                    }
                }
            }
            Err(ApplyError::Fatal { input, error }) => Self::Failure {
                input,
                error,
                task_id,
            },
            Err(ApplyError::Cancelled { input }) => Self::Unprocessed { input, task_id },
            Err(ApplyError::Panic { input, payload }) => Self::Panic {
                input,
                payload,
                task_id,
            },
        }
    }

    /// Returns `true` if this is a `Success` outcome.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    /// Returns `true` if this outcome represents an unprocessed task input.
    pub fn is_unprocessed(&self) -> bool {
        matches!(self, Self::Unprocessed { .. })
    }

    /// Returns `true` if this outcome represents a task processing failure.
    pub fn is_failure(&self) -> bool {
        match self {
            Self::Failure { .. } | Self::Panic { .. } => true,
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { .. } => true,
            _ => false,
        }
    }

    /// Returns the task_id of the task that produced this outcome.
    pub fn task_id(&self) -> &TaskId {
        match self {
            Self::Success { task_id, .. }
            | Self::Failure { task_id, .. }
            | Self::Unprocessed { task_id, .. }
            | Self::Missing { task_id }
            | Self::Panic { task_id, .. } => task_id,
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { task_id, .. } => task_id,
        }
    }

    /// Consumes this `Outcome` and returns the value if it is a `Success`, otherwise panics.
    pub fn unwrap(self) -> W::Output {
        self.success().expect("not a Success outcome")
    }

    /// Consumes this `Outcome` and returns the output value if it is a `Success`, otherwise `None`.
    pub fn success(self) -> Option<W::Output> {
        match self {
            Self::Success { value, .. } => Some(value),
            _ => None,
        }
    }

    /// Consumes this `Outcome` and returns the input value if available, otherwise `None`.
    pub fn try_into_input(self) -> Option<W::Input> {
        match self {
            Self::Success { .. } => None,
            Self::Failure { input, .. } => input,
            Self::Unprocessed { input, .. } => Some(input),
            Self::Missing { .. } => None,
            Self::Panic { input, .. } => input,
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { input, .. } => Some(input),
        }
    }

    /// Consumes this `Outcome` and depending on the variant:
    /// * Returns the wrapped error if this is a `Failure` or `MaxRetriesAttempted`,
    /// * Resumes unwinding if this is a `Panic` outcome,
    /// * Otherwise returns `None`.
    pub fn try_into_error(self) -> Option<W::Error> {
        match self {
            Self::Success { .. } => None,
            Self::Failure { error, .. } => Some(error),
            Self::Unprocessed { .. } => None,
            Self::Missing { .. } => None,
            Self::Panic { payload, .. } => payload.resume(),
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { error, .. } => Some(error),
        }
    }
}

impl<W: Worker> PartialEq for Outcome<W> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Success { task_id: a, .. }, Self::Success { task_id: b, .. }) => a == b,
            (Self::Failure { task_id: a, .. }, Self::Failure { task_id: b, .. }) => a == b,
            (Self::Unprocessed { task_id: a, .. }, Self::Unprocessed { task_id: b, .. }) => a == b,
            (Self::Missing { task_id: a }, Self::Missing { task_id: b }) => a == b,
            (Self::Panic { task_id: a, .. }, Self::Panic { task_id: b, .. }) => a == b,
            #[cfg(feature = "retry")]
            (
                Self::MaxRetriesAttempted { task_id: a, .. },
                Self::MaxRetriesAttempted { task_id: b, .. },
            ) => a == b,
            _ => false,
        }
    }
}

impl<W: Worker> Eq for Outcome<W> {}

impl<W: Worker> PartialOrd for Outcome<W> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<W: Worker> Ord for Outcome<W> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.task_id().cmp(other.task_id())
    }
}

#[cfg(test)]
mod tests {
    
}