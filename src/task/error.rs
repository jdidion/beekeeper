use crate::Panic;
use std::fmt::Debug;

/// Error that can result from applying a `Worker`'s function to an input.
#[derive(thiserror::Error, Debug)]
pub enum ApplyError<I, E> {
    /// The task was cancelled before it completed.
    #[error("Task was cancelled")]
    Cancelled { input: I },
    /// The task failed due to a (possibly) transient error and can be retried.
    #[error("Error is retryable")]
    Retryable { input: I, error: E },
    /// The task failed due to a fatal error that cannot be retried.
    #[error("Error is not retryable")]
    NotRetryable { input: Option<I>, error: E },
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
            Self::Cancelled { input, .. } => Some(input),
            Self::Retryable { input, .. } => Some(input),
            Self::NotRetryable { input, .. } => input.as_ref(),
            Self::Panic { input, .. } => input.as_ref(),
        }
    }

    /// Consumes this `ApplyError` and returns the input value associated with it, if available.
    pub fn into_input(self) -> Option<I> {
        match self {
            Self::Cancelled { input, .. } => Some(input),
            Self::Retryable { input, .. } => Some(input),
            Self::NotRetryable { input, .. } => input,
            Self::Panic { input, .. } => input,
        }
    }

    /// Consumes this `ApplyError` and:
    /// * Panics, if this is a `Panic` variant,
    /// * Returns `None`, if this is a `Cancelled` variant,
    /// * Returns `Some(E)` otherwise
    pub fn unwrap_error(self) -> Option<E> {
        match self {
            Self::Cancelled { .. } => None,
            Self::Retryable { input: _, error } => Some(error),
            Self::NotRetryable { input: _, error } => Some(error),
            Self::Panic { input: _, payload } => payload.resume(),
        }
    }
}
