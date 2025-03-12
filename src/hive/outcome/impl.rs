use super::Outcome;
use crate::bee::{ApplyError, TaskId, TaskMeta, Worker, WorkerResult};
use std::cmp::Ordering;
use std::mem;

impl<W: Worker> Outcome<W> {
    /// Converts a worker `result` into an `Outcome` with the given task_id and optional subtask ids.
    pub(in crate::hive) fn from_worker_result(
        result: WorkerResult<W>,
        task_meta: TaskMeta,
        subtask_ids: Option<Vec<TaskId>>,
    ) -> Self {
        let task_id = task_meta.id();
        match (result, subtask_ids) {
            (Ok(value), Some(subtask_ids)) => Self::SuccessWithSubtasks {
                value,
                task_id,
                subtask_ids,
            },
            (Ok(value), None) => Self::Success { value, task_id },
            (Err(ApplyError::Retryable { input, error, .. }), Some(subtask_ids)) => {
                Self::FailureWithSubtasks {
                    input: Some(input),
                    error,
                    task_id,
                    subtask_ids,
                }
            }
            (Err(ApplyError::Retryable { input, error }), None) => {
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
            (Err(ApplyError::Fatal { input, error }), Some(subtask_ids)) => {
                Self::FailureWithSubtasks {
                    input,
                    error,
                    task_id,
                    subtask_ids,
                }
            }
            (Err(ApplyError::Fatal { input, error }), None) => Self::Failure {
                input,
                error,
                task_id,
            },
            (Err(ApplyError::Cancelled { input }), Some(subtask_ids)) => {
                Self::UnprocessedWithSubtasks {
                    input,
                    task_id,
                    subtask_ids,
                }
            }
            (Err(ApplyError::Cancelled { input }), None) => Self::Unprocessed { input, task_id },
            (Err(ApplyError::Panic { input, payload }), Some(subtask_ids)) => {
                Self::PanicWithSubtasks {
                    input,
                    payload,
                    task_id,
                    subtask_ids,
                }
            }
            (Err(ApplyError::Panic { input, payload }), None) => Self::Panic {
                input,
                payload,
                task_id,
            },
        }
    }

    /// Creates a new `Outcome::Fatal` from the given input, task metadata, and error.
    #[cfg(feature = "retry")]
    pub(in crate::hive) fn from_fatal(
        input: W::Input,
        task_meta: TaskMeta,
        error: W::Error,
    ) -> Self {
        Self::Failure {
            input: Some(input),
            error,
            task_id: task_meta.id(),
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
            | Self::SuccessWithSubtasks { task_id, .. }
            | Self::Failure { task_id, .. }
            | Self::FailureWithSubtasks { task_id, .. }
            | Self::Unprocessed { task_id, .. }
            | Self::UnprocessedWithSubtasks { task_id, .. }
            | Self::Missing { task_id }
            | Self::Panic { task_id, .. }
            | Self::PanicWithSubtasks { task_id, .. } => task_id,
            #[cfg(feature = "local-batch")]
            Self::WeightLimitExceeded { task_id, .. } => task_id,
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { task_id, .. } => task_id,
        }
    }

    /// Returns the IDs of the tasks submitted by the task that produced this outcome, or `None`
    /// if the task did not submit any subtasks.
    pub fn subtask_ids(&self) -> Option<&Vec<TaskId>> {
        match self {
            Self::SuccessWithSubtasks { subtask_ids, .. }
            | Self::FailureWithSubtasks { subtask_ids, .. }
            | Self::UnprocessedWithSubtasks { subtask_ids, .. }
            | Self::PanicWithSubtasks { subtask_ids, .. } => Some(subtask_ids),
            _ => None,
        }
    }

    /// Consumes this `Outcome` and returns the value if it is a `Success`, otherwise panics.
    pub fn unwrap(self) -> W::Output {
        match self {
            Self::Success { value, .. } | Self::SuccessWithSubtasks { value, .. } => value,
            outcome => panic!("Not a success outcome: {:?}", outcome),
        }
    }

    /// Consumes this `Outcome` and returns the output value if it is a `Success`, otherwise `None`.
    pub fn success(self) -> Option<W::Output> {
        match self {
            Self::Success { value, .. } | Self::SuccessWithSubtasks { value, .. } => Some(value),
            _ => None,
        }
    }

    /// Consumes this `Outcome` and returns the input value if available, otherwise `None`.
    pub fn try_into_input(self) -> Option<W::Input> {
        match self {
            Self::Failure { input, .. }
            | Self::FailureWithSubtasks { input, .. }
            | Self::Panic { input, .. }
            | Self::PanicWithSubtasks { input, .. } => input,
            Self::Unprocessed { input, .. } | Self::UnprocessedWithSubtasks { input, .. } => {
                Some(input)
            }
            Self::Success { .. } | Self::SuccessWithSubtasks { .. } | Self::Missing { .. } => None,
            #[cfg(feature = "local-batch")]
            Self::WeightLimitExceeded { input, .. } => Some(input),
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { input, .. } => Some(input),
        }
    }

    /// Retursn a reference to the wrapped error, if any.
    pub fn error(&self) -> Option<&W::Error> {
        match self {
            Self::Failure { error, .. } | Self::FailureWithSubtasks { error, .. } => Some(error),
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { error, .. } => Some(error),
            _ => None,
        }
    }

    /// Consumes this `Outcome` and depending on the variant:
    /// * Returns the wrapped error if this is a `Failure` or `MaxRetriesAttempted`,
    /// * Resumes unwinding if this is a `Panic` outcome,
    /// * Otherwise returns `None`.
    pub fn try_into_error(self) -> Option<W::Error> {
        match self {
            Self::Failure { error, .. } | Self::FailureWithSubtasks { error, .. } => Some(error),
            Self::Panic { payload, .. } | Self::PanicWithSubtasks { payload, .. } => {
                payload.resume()
            }
            Self::Success { .. }
            | Self::SuccessWithSubtasks { .. }
            | Self::Unprocessed { .. }
            | Self::UnprocessedWithSubtasks { .. }
            | Self::Missing { .. } => None,
            #[cfg(feature = "local-batch")]
            Self::WeightLimitExceeded { .. } => None,
            #[cfg(feature = "retry")]
            Self::MaxRetriesAttempted { error, .. } => Some(error),
        }
    }
}

impl<W: Worker> PartialEq for Outcome<W> {
    fn eq(&self, other: &Self) -> bool {
        mem::discriminant(self) == mem::discriminant(other) && self.task_id() == other.task_id()
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
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::Outcome;
    use crate::bee::stock::EchoWorker;
    use crate::bee::{ApplyError, TaskMeta, WorkerResult};
    use crate::panic::Panic;

    type Worker = EchoWorker<usize>;
    type WorkerOutcome = Outcome<Worker>;

    #[test]
    fn test_success() {
        let outcome = WorkerOutcome::Success {
            value: 42,
            task_id: 1,
        };
        assert_eq!(outcome.success(), Some(42));
    }

    #[test]
    fn test_unwrap() {
        let outcome = WorkerOutcome::Success {
            value: 42,
            task_id: 1,
        };
        assert_eq!(outcome.unwrap(), 42);
    }

    #[test]
    fn test_success_on_error() {
        let outcome = WorkerOutcome::Failure {
            input: Some(42),
            error: (),
            task_id: 1,
        };
        assert!(matches!(outcome.success(), None))
    }

    #[test]
    #[should_panic]
    fn test_unwrap_panics_on_error() {
        let outcome = WorkerOutcome::Failure {
            input: Some(42),
            error: (),
            task_id: 1,
        };
        let _ = outcome.unwrap();
    }

    #[test]
    fn test_retry_with_subtasks_into_failure() {
        let input = 1;
        let task_id = 1;
        let error = ();
        let result = WorkerResult::<Worker>::Err(ApplyError::Retryable { input, error });
        let task_meta = TaskMeta::new(task_id);
        let subtask_ids = vec![2, 3, 4];
        let outcome =
            WorkerOutcome::from_worker_result(result, task_meta, Some(subtask_ids.clone()));
        let expected_outcome = WorkerOutcome::FailureWithSubtasks {
            input: Some(input),
            error,
            task_id,
            subtask_ids,
        };
        assert_eq!(outcome, expected_outcome);
    }

    #[test]
    fn test_subtasks() {
        let input = 1;
        let task_id = 1;
        let error = ();
        let task_meta = TaskMeta::new(task_id);
        let subtask_ids = vec![2, 3, 4];

        let result = WorkerResult::<Worker>::Err(ApplyError::Fatal {
            input: Some(input),
            error,
        });
        let outcome =
            WorkerOutcome::from_worker_result(result, task_meta.clone(), Some(subtask_ids.clone()));
        let expected_outcome = WorkerOutcome::FailureWithSubtasks {
            input: Some(1),
            task_id: 1,
            error: (),
            subtask_ids: vec![2, 3, 4],
        };
        assert_eq!(outcome, expected_outcome);

        let result = WorkerResult::<Worker>::Err(ApplyError::Cancelled { input });
        let outcome =
            WorkerOutcome::from_worker_result(result, task_meta.clone(), Some(subtask_ids.clone()));
        let expected_outcome = WorkerOutcome::UnprocessedWithSubtasks {
            input: 1,
            task_id: 1,
            subtask_ids: vec![2, 3, 4],
        };
        assert_eq!(outcome, expected_outcome);

        let result = WorkerResult::<Worker>::Err(ApplyError::Panic {
            input: Some(input),
            payload: Panic::new("panicked", None),
        });
        let outcome =
            WorkerOutcome::from_worker_result(result, task_meta.clone(), Some(subtask_ids.clone()));
        let expected_outcome = WorkerOutcome::PanicWithSubtasks {
            input: Some(1),
            task_id: 1,
            subtask_ids: vec![2, 3, 4],
            payload: Panic::new("panicked", None),
        };
        assert_eq!(outcome, expected_outcome);
    }

    #[test]
    fn test_try_into_input() {
        let outcome = WorkerOutcome::Success {
            value: 42,
            task_id: 1,
        };
        assert_eq!(outcome.try_into_input(), None);

        let outcome = WorkerOutcome::Failure {
            input: None,
            error: (),
            task_id: 2,
        };
        assert_eq!(outcome.try_into_input(), None);

        let outcome = WorkerOutcome::Failure {
            input: Some(42),
            error: (),
            task_id: 2,
        };
        assert_eq!(outcome.try_into_input(), Some(42));

        let outcome = WorkerOutcome::Unprocessed {
            input: 42,
            task_id: 3,
        };
        assert_eq!(outcome.try_into_input(), Some(42));

        let outcome = WorkerOutcome::Missing { task_id: 4 };
        assert_eq!(outcome.try_into_input(), None);

        let outcome = WorkerOutcome::Panic {
            input: None,
            payload: Panic::try_call(None, || panic!()).unwrap_err(),
            task_id: 5,
        };
        assert_eq!(outcome.try_into_input(), None);

        let outcome = WorkerOutcome::Panic {
            input: Some(42),
            payload: Panic::try_call(None, || panic!()).unwrap_err(),
            task_id: 5,
        };
        assert_eq!(outcome.try_into_input(), Some(42));
    }

    #[test]
    fn test_try_into_error() {
        let outcome = WorkerOutcome::Success {
            value: 42,
            task_id: 1,
        };
        assert_eq!(outcome.try_into_error(), None);

        let outcome = WorkerOutcome::Failure {
            input: None,
            error: (),
            task_id: 2,
        };
        assert_eq!(outcome.try_into_error(), Some(()));

        let outcome = WorkerOutcome::Failure {
            input: Some(42),
            error: (),
            task_id: 2,
        };
        assert_eq!(outcome.try_into_error(), Some(()));

        let outcome = WorkerOutcome::Unprocessed {
            input: 42,
            task_id: 3,
        };
        assert_eq!(outcome.try_into_error(), None);

        let outcome = WorkerOutcome::Missing { task_id: 4 };
        assert_eq!(outcome.try_into_error(), None);
    }

    #[test]
    #[should_panic]
    fn test_try_into_error_panic() {
        WorkerOutcome::Panic {
            input: None,
            payload: Panic::try_call(None, || panic!()).unwrap_err(),
            task_id: 5,
        }
        .try_into_error();
    }

    #[test]
    fn test_eq() {
        let outcome1 = WorkerOutcome::Success {
            value: 42,
            task_id: 1,
        };
        let outcome2 = WorkerOutcome::Success {
            value: 42,
            task_id: 1,
        };
        assert_eq!(outcome1, outcome2);

        let outcome3 = WorkerOutcome::Success {
            value: 42,
            task_id: 2,
        };
        assert_ne!(outcome1, outcome3);

        let outcome4 = WorkerOutcome::Failure {
            input: None,
            error: (),
            task_id: 1,
        };
        assert_ne!(outcome1, outcome4);
    }
}

#[cfg(all(test, feature = "retry"))]
mod retry_tests {
    use super::Outcome;
    use crate::bee::TaskMeta;
    use crate::bee::stock::EchoWorker;

    type Worker = EchoWorker<usize>;
    type WorkerOutcome = Outcome<Worker>;

    #[test]
    fn test_try_into_input() {
        let outcome = WorkerOutcome::MaxRetriesAttempted {
            input: 42,
            error: (),
            task_id: 1,
        };
        assert_eq!(outcome.try_into_input(), Some(42));
    }

    #[test]
    fn test_try_into_error() {
        let outcome = WorkerOutcome::MaxRetriesAttempted {
            input: 42,
            error: (),
            task_id: 1,
        };
        assert_eq!(outcome.try_into_error(), Some(()));
    }

    #[test]
    fn test_from_fatal() {
        let input = 1;
        let task_id = 1;
        let error = ();
        let outcome = WorkerOutcome::from_fatal(input, TaskMeta::new(task_id), error);
        let expected_outcome = WorkerOutcome::Failure {
            input: Some(input),
            task_id,
            error,
        };
        assert_eq!(outcome, expected_outcome);
    }
}
