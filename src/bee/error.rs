//! Error types that may be returned by `Worker`s.
use crate::panic::Panic;
use std::fmt::Debug;

/// Error that can result from applying a `Worker`'s function to an input.
#[derive(thiserror::Error, Debug)]
pub enum ApplyError<I, E> {
    /// The task failed due to a fatal error that cannot be retried.
    #[error("Task failed (and is not retryable)")]
    Fatal { input: Option<I>, error: E },
    /// The task failed due to a (possibly) transient error and can be retried.
    #[error("Task failed, but is retryable")]
    Retryable { input: I, error: E },
    /// The task was cancelled before it completed.
    #[error("Task was cancelled")]
    Cancelled { input: I },
    /// The task panicked.
    #[error("Task panicked")]
    Panic {
        input: Option<I>,
        payload: Panic<String>,
    },
}

impl<I, E> ApplyError<I, E> {
    /// Returns the input value associated with this error, if available.
    pub fn input(&self) -> Option<&I> {
        match self {
            Self::Fatal { input, .. } => input.as_ref(),
            Self::Retryable { input, .. } => Some(input),
            Self::Cancelled { input, .. } => Some(input),
            Self::Panic { input, .. } => input.as_ref(),
        }
    }

    /// Consumes this `ApplyError` and returns the input value associated with it, if available.
    pub fn into_input(self) -> Option<I> {
        match self {
            Self::Fatal { input, .. } => input,
            Self::Retryable { input, .. } => Some(input),
            Self::Cancelled { input, .. } => Some(input),
            Self::Panic { input, .. } => input,
        }
    }

    /// Consumes this `ApplyError` and:
    /// * Panics, if this is a `Panic` variant,
    /// * Returns `None`, if this is a `Cancelled` variant,
    /// * Returns `Some(E)` otherwise
    pub fn into_source(self) -> Option<E> {
        match self {
            Self::Fatal { input: _, error } => Some(error),
            Self::Retryable { input: _, error } => Some(error),
            Self::Cancelled { .. } => None,
            Self::Panic { input: _, payload } => payload.resume(),
        }
    }
}

/// Error that can result from applying a `RefWorker`'s function to an input.
#[derive(thiserror::Error, Debug)]
pub enum ApplyRefError<E> {
    /// The task failed due to a fatal error that cannot be retried.
    #[error("Error is not retryable")]
    Fatal(E),
    /// The task failed due to a (possibly) transient error and can be retried.
    #[error("Error is retryable")]
    Retryable(E),
    /// The task was cancelled before it completed.
    #[error("Task was cancelled")]
    Cancelled,
}

impl<E> ApplyRefError<E> {
    pub(super) fn into_apply_error<I>(self, input: I) -> ApplyError<I, E> {
        match self {
            Self::Fatal(error) => ApplyError::Fatal {
                input: Some(input),
                error,
            },
            Self::Retryable(error) => ApplyError::Retryable { input, error },
            Self::Cancelled => ApplyError::Cancelled { input },
        }
    }
}

impl<E> From<E> for ApplyRefError<E> {
    fn from(e: E) -> Self {
        ApplyRefError::Fatal(e)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::ApplyError;
    use crate::panic::Panic;

    type TestError<'a> = ApplyError<usize, &'a str>;

    impl<I, E> ApplyError<I, E> {
        pub fn panic(input: Option<I>, detail: Option<String>) -> Self {
            Self::Panic {
                input,
                payload: Panic::new("test", detail),
            }
        }
    }

    #[test]
    fn test_input() {
        let cancelled: TestError = ApplyError::Cancelled { input: 42 };
        assert_eq!(&42, cancelled.input().unwrap());
        assert_eq!(42, cancelled.into_input().unwrap());

        let retryable: TestError = ApplyError::Retryable {
            input: 42,
            error: "bork",
        };
        assert_eq!(&42, retryable.input().unwrap());
        assert_eq!(42, retryable.into_input().unwrap());

        let not_retryable: TestError = ApplyError::Fatal {
            input: Some(42),
            error: "bork",
        };
        assert_eq!(&42, not_retryable.input().unwrap());
        assert_eq!(42, not_retryable.into_input().unwrap());

        let panic: TestError = ApplyError::panic(Some(42), None);
        assert_eq!(&42, panic.input().unwrap());
        assert_eq!(42, panic.into_input().unwrap());
    }

    #[test]
    fn test_error() {
        let cancelled: TestError = ApplyError::Cancelled { input: 42 };
        assert_eq!(None, cancelled.into_source());

        let retryable: TestError = ApplyError::Retryable {
            input: 42,
            error: "bork",
        };
        assert_eq!(Some("bork"), retryable.into_source());

        let not_retryable: TestError = ApplyError::Fatal {
            input: Some(42),
            error: "bork",
        };
        assert_eq!(Some("bork"), not_retryable.into_source());
    }

    #[test]
    #[should_panic]
    fn test_panic() {
        let panic: TestError = ApplyError::panic(Some(42), Some("borked".to_string()));
        let _ = panic.into_source();
    }
}
