use crate::task::{ApplyError, Context, Worker, WorkerResult};
use crate::Panic;
use std::fmt::Debug;

/// Alias for an `ApplyRefError` whose `I`nput and `E`rror paramters are taken from `W::Input` and
/// `W::Error` respectively.
pub type RefWorkerError<W> = ApplyRefError<<W as RefWorker>::Error>;
pub type RefWorkerResult<W> = Result<<W as RefWorker>::Output, RefWorkerError<W>>;

/// Error that can result from applying a `RefWorker`'s function to an input.
#[derive(thiserror::Error, Debug)]
pub enum ApplyRefError<E> {
    /// The task was cancelled before it completed.
    #[error("Task was cancelled")]
    Cancelled,
    /// The task failed due to a (possibly) transient error and can be retried.
    #[error("Error is retryable")]
    Retryable(E),
    /// The task failed due to a fatal error that cannot be retried.
    #[error("Error is not retryable")]
    NotRetryable(E),
}

impl<E> ApplyRefError<E> {
    fn into_apply_error<I>(self, input: I) -> ApplyError<I, E> {
        match self {
            Self::Cancelled => ApplyError::Cancelled { input },
            Self::Retryable(error) => ApplyError::Retryable { input, error },
            Self::NotRetryable(error) => ApplyError::NotRetryable {
                input: Some(input),
                error,
            },
        }
    }
}

impl<E> From<E> for ApplyRefError<E> {
    fn from(e: E) -> Self {
        ApplyRefError::NotRetryable(e)
    }
}

/// A trait for stateful, fallible, idempotent functions that take a reference to their input.
pub trait RefWorker: Debug + Sized + 'static {
    /// The type of the input to this funciton.
    type Input: Send;
    /// The type of the output from this function.
    type Output: Send;
    /// The type of error produced by this function.
    type Error: Send + Debug;

    fn apply_ref(&mut self, _: &Self::Input, _: &Context) -> RefWorkerResult<Self>;
}

/// Blanket implementation of `Worker` for `RefWorker` that calls `apply_ref` and catches any
/// panic. This enables the `input` to be preserved on panic, whereas it is lost when implementing
/// `Worker` directly (without manual panic handling).
impl<I, O, E, T: RefWorker<Input = I, Output = O, Error = E>> Worker for T
where
    I: Send,
    O: Send,
    E: Send + Debug,
{
    type Input = I;
    type Output = O;
    type Error = E;

    fn apply(&mut self, input: Self::Input, ctx: &Context) -> WorkerResult<Self> {
        match Panic::try_call(None, || self.apply_ref(&input, ctx)) {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) => Err(error.into_apply_error(input)),
            Err(payload) => Err(ApplyError::Panic {
                input: Some(input),
                payload,
            }),
        }
    }
}
