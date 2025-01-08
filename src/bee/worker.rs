use super::{ApplyError, Context};
use crate::panic::Panic;
use std::fmt::Debug;

/// Alias for an `ApplyError` whose `I`nput and `E`rror paramters are taken from `W::Input` and
/// `W::Error` respectively.
pub type WorkerError<W> = ApplyError<<W as Worker>::Input, <W as Worker>::Error>;
pub type WorkerResult<W> = Result<<W as Worker>::Output, WorkerError<W>>;

/// A trait for stateful, fallible, idempotent functions.
pub trait Worker: Debug + Sized + 'static {
    /// The type of the input to this funciton.
    type Input: Send;
    /// The type of the output from this function.
    type Output: Send;
    /// The type of error produced by this function.
    type Error: Send + Debug;

    /// Applies this `Worker`'s function to the given input of type `Self::Input` and returns a
    /// `Result` containing the output of type `Self::Output` or an error that indicates whether
    /// the task can be retried.
    ///
    /// The `Context` parameter provides additional context for the task, including:
    /// * index: the index of the task within the `Hive`. This value is used for ordering results.
    /// * attempt: the retry attempt number. The attempt value is `0` the first time the task is
    ///   attempted and increases by `1` for each subsequent retry attempt.
    /// * cancelled: whether the task has been cancelled and should exit early with an
    ///   `ApplyError::Cancelled` result.
    ///
    /// This method should not panic. If it may panic, then `Panic::try_call` should be used to
    /// catch the panic and turn it into an `ApplyError::Panic` error.
    fn apply(&mut self, _: Self::Input, _: &Context) -> WorkerResult<Self>;

    /// Applies this `Worker`'s function sequentially to an iterator of inputs and returns a
    /// iterator over the outputs.
    fn map(
        &mut self,
        inputs: impl IntoIterator<Item = Self::Input>,
    ) -> impl Iterator<Item = Result<Self::Output, Self::Error>> {
        let ctx = Context::empty();
        inputs.into_iter().map(move |input| {
            self.apply(input, &ctx).map_err(|error| match error {
                ApplyError::Retryable { error, .. } => error,
                ApplyError::Fatal { error, .. } => error,
                _ => panic!("unexpected error"),
            })
        })
    }
}

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
            Self::NotRetryable(error) => ApplyError::Fatal {
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

#[cfg(test)]
mod tests {
    use super::{ApplyRefError, RefWorker, RefWorkerResult, Worker, WorkerResult};
    use crate::bee::{ApplyError, Context};

    #[derive(Debug)]
    struct MyWorker;

    impl Worker for MyWorker {
        type Input = u8;
        type Output = u8;
        type Error = ();

        fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
            Ok(input + 1)
        }
    }

    #[test]
    fn test_map() {
        let mut worker = MyWorker;
        assert_eq!(
            55u8,
            worker
                .map(0..10)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .into_iter()
                .sum()
        );
    }

    #[derive(Debug)]
    struct MyRefWorker;

    impl RefWorker for MyRefWorker {
        type Input = u8;
        type Output = u8;
        type Error = ();

        fn apply_ref(&mut self, input: &Self::Input, _: &Context) -> RefWorkerResult<Self> {
            match *input {
                0 => Err(ApplyRefError::Retryable(())),
                1 => Err(ApplyRefError::NotRetryable(())),
                2 => Err(ApplyRefError::Cancelled),
                i => Ok(i + 1),
            }
        }
    }

    #[test]
    fn test_apply() {
        let mut worker = MyRefWorker;
        let ctx = Context::empty();
        assert!(matches!(worker.apply(5, &ctx), Ok(6)));
    }

    #[test]
    fn test_apply_fail() {
        let mut worker = MyRefWorker;
        let ctx = Context::empty();
        assert!(matches!(
            worker.apply(0, &ctx),
            Err(ApplyError::Retryable { input: 0, .. })
        ));
    }
}
