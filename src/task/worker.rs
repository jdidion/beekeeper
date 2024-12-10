use crate::task::Context;
use crate::Panic;
use std::fmt::Debug;

/// Alias for an `ApplyError` whose `I`nput and `E`rror paramters are taken from `W::Input` and
/// `W::Error` respectively.
pub type WorkerError<W> = ApplyError<<W as Worker>::Input, <W as Worker>::Error>;
pub type WorkerResult<W> = Result<<W as Worker>::Output, WorkerError<W>>;

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
    /// attempted and increases by `1` for each subsequent retry attempt.
    /// * cancelled: whether the task has been cancelled and should exit early with an
    /// `ApplyError::Cancelled` result.
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
                ApplyError::NotRetryable { error, .. } => error,
                _ => panic!("unexpected error"),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Worker, WorkerResult};
    use crate::task::Context;

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
}
