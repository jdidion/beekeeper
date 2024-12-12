use crate::task::Worker;
use crate::Panic;
use std::fmt::Debug;

pub type TaskResult<W> = Result<<W as Worker>::Output, HiveError<W>>;

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

impl<W: Worker> HiveError<W> {
    /// Depending on variant of `self`:
    /// * Returns `Ok(input)` if the result is `Unprocessed(input)`
    /// * Returns `Err(error)` if the result is `Error::Failed(error)` or
    /// `Error::MaxRetriesAttempted(error)`
    /// * Resumes unwinding if the result is `Error::Panic`
    pub fn unwrap(self) -> Result<W::Input, W::Error> {
        match self {
            HiveError::Unprocessed(input) => Ok(input),
            HiveError::Failed(error) | HiveError::MaxRetriesAttempted(error) => Err(error),
            HiveError::Panic(panic) => panic.resume(),
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
