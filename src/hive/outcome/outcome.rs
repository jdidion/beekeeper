use crate::panic::Panic;
use crate::task::{ApplyError, Worker, WorkerResult};
use std::cmp::Ordering;
use std::fmt::Debug;

/// The possible outcomes of a task execution. Each outcome includes the index of the task that
/// produced it.
///
/// Note that `Outcome`s can only be compared or ordered with other `Outcome`s proced by the same
/// `Hive`, because comparison/ordering is completely based on the `index`.
#[derive(Debug)]
pub enum Outcome<W: Worker> {
    /// The task was executed successfully.
    Success { value: W::Output, index: usize },
    /// The task failed with an error that was not retryable. The input value that caused the
    /// failure is provided if possible.
    Failure {
        input: Option<W::Input>,
        error: W::Error,
        index: usize,
    },
    /// The task failed after retrying the maximum number of times.
    MaxRetriesAttempted {
        input: W::Input,
        error: W::Error,
        index: usize,
    },
    /// The task was not executed before the Hive was closed.
    Unprocessed { input: W::Input, index: usize },
    /// The task panicked. The input value that caused the panic is provided if possible.
    Panic {
        input: Option<W::Input>,
        payload: Panic<String>,
        index: usize,
    },
}

impl<W: Worker> Outcome<W> {
    pub(in crate::hive) fn from_worker_result(
        result: WorkerResult<W>,
        index: usize,
        retryable: bool,
    ) -> Self {
        match result {
            Ok(value) => Self::Success { index, value },
            Err(ApplyError::Retryable { input, error }) if retryable => Self::MaxRetriesAttempted {
                input,
                error,
                index,
            },
            Err(ApplyError::Retryable { input, error, .. }) => Self::Failure {
                input: Some(input),
                error,
                index,
            },
            Err(ApplyError::NotRetryable { input, error }) => Self::Failure {
                input,
                error,
                index,
            },
            Err(ApplyError::Cancelled { input }) => Self::Unprocessed { input, index },
            Err(ApplyError::Panic { input, payload }) => Self::Panic {
                input,
                payload,
                index,
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

    /// Returns `true` if this outcome represents a task failure.
    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            Self::Failure { .. } | Self::MaxRetriesAttempted { .. } | Self::Panic { .. }
        )
    }

    /// Returns the index of the task that produced this outcome.
    pub fn index(&self) -> usize {
        match self {
            Self::Success { index, .. }
            | Self::Failure { index, .. }
            | Self::MaxRetriesAttempted { index, .. }
            | Self::Unprocessed { index, .. }
            | Self::Panic { index, .. } => *index,
        }
    }

    /// Consumes this `Outcome` and returns the value of this `Success` outcome. Panics if this is
    /// not a `Success` outcome.
    pub fn unwrap(self) -> W::Output {
        match self {
            Self::Success { value, .. } => value,
            _ => panic!("not a Success outcome"),
        }
    }

    /// Returns the input value if available, otherwise `None`.
    pub fn into_input(self) -> Option<W::Input> {
        match self {
            Self::Success { .. } => None,
            Self::Failure { input, .. } => input,
            Self::MaxRetriesAttempted { input, .. } => Some(input),
            Self::Unprocessed { input, .. } => Some(input),
            Self::Panic { input, .. } => input,
        }
    }

    /// Consumes this `Outcome` and depending on the variant:
    /// * Returns the wrapped error if this is a `Failure` or `MaxRetriesAttempted`,
    /// * Panics if this is a `Success` or `Unprocessed` outcome,
    /// * Resumes unwinding if this is a `Panic` outcome.
    pub fn into_error(self) -> W::Error {
        match self {
            Self::Success { .. } => panic!("not an error outcome"),
            Self::Failure { error, .. } | Self::MaxRetriesAttempted { error, .. } => error,
            Self::Unprocessed { .. } => panic!("unprocessed input"),
            Self::Panic { payload, .. } => payload.resume(),
        }
    }
}

impl<W: Worker> PartialEq for Outcome<W> {
    fn eq(&self, other: &Self) -> bool {
        self.index() == other.index()
    }
}

impl<W: Worker> Eq for Outcome<W> {}

impl<W: Worker> PartialOrd for Outcome<W> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.index().partial_cmp(&other.index())
    }
}

impl<W: Worker> Ord for Outcome<W> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
