use crate::task::Worker;
use crate::Panic;
use std::fmt::Debug;

pub type HiveResult<T, W> = Result<T, HiveError<W>>;
pub type TaskResult<W> = HiveResult<<W as Worker>::Output, W>;

/// The possible errors that can occur during task execution.
#[derive(thiserror::Error, Debug)]
pub enum HiveError<W: Worker> {
    #[error("Task failed")]
    Failed(W::Error),
    #[error("Task retried the maximum number of times")]
    MaxRetriesAttempted(W::Error),
    #[error("Task input was not processed")]
    Unprocessed(W::Input),
    #[error("Task panicked")]
    Panic(Panic<String>),
}

pub trait HiveResultExt<T, W: Worker> {
    /// Depending on variant of `self`:
    /// * Returns `Ok(t)` if the result is `Ok(t)`
    /// * Returns `Err(e)` if the result is `Err(Error::Failed(e))`
    /// * Resumes unwinding if the result is `Err(Error::Panic(ctx))`
    /// * Otherwise panics
    fn ok_or_unwrap_error(self) -> Result<T, W::Error>;
}

impl<T, W: Worker> HiveResultExt<T, W> for HiveResult<T, W> {
    fn ok_or_unwrap_error(self) -> Result<T, W::Error> {
        match self {
            Ok(value) => Ok(value),
            Err(HiveError::Failed(error)) => Err(error),
            Err(HiveError::Panic(panic)) => panic.resume(),
            Err(_) => panic!("unexpected error variant"),
        }
    }
}

pub trait TaskResultIteratorExt<W: Worker>: IntoIterator<Item = TaskResult<W>> + Sized {
    /// Consumes this iterator and returns an iterator over its `Ok` values. Panics if any `Err`
    /// value is encountered.
    fn into_outputs(self) -> impl Iterator<Item = W::Output> {
        self.into_iter().map(|result| match result {
            Ok(value) => value,
            Err(e) => panic!("unexpected error: {e}"),
        })
    }
}

impl<W: Worker, T: Iterator<Item = TaskResult<W>>> TaskResultIteratorExt<W> for T {}
