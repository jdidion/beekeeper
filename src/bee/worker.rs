//! Worker bee traits.
use super::{ApplyError, ApplyRefError, Context};
use crate::panic::Panic;
use std::fmt::Debug;

/// Alias for an `ApplyError` whose `I`nput and `E`rror paramters are taken from `W::Input` and
/// `W::Error` respectively.
pub type WorkerError<W> = ApplyError<<W as Worker>::Input, <W as Worker>::Error>;
pub type WorkerResult<W> = Result<<W as Worker>::Output, WorkerError<W>>;

/// A trait for stateful, fallible, idempotent functions.
pub trait Worker: Debug + Sized + 'static {
    /// The type of the input to this function.
    type Input: Send;
    /// The type of the output from this function.
    type Output: Send;
    /// The type of error produced by this function.
    type Error: Send + Debug;

    /// Applies this `Worker`'s function to the given input of type `Self::Input` and returns a
    /// `Result` containing the output of type `Self::Output` or an [`ApplyError`] that indicates
    /// whether the task can be retried.
    ///
    /// The [`Context`] parameter provides additional context for the task, including:
    /// * task_id: the ID of the task within the [`Hive`](crate::hive::Hive). This value is used
    ///   for ordering results.
    /// * attempt: the retry attempt number. The attempt value is `0` the first time the task is
    ///   attempted and increases by `1` for each subsequent retry attempt. (Note: retrying is only
    ///   supported when the `retry` feature is enabled.)
    /// * cancelled: whether the task has been cancelled and should exit early with an
    ///   [`ApplyError::Cancelled`] result.
    ///
    /// This method should not panic. If it may panic, then [`Panic::try_call`] should be used to
    /// catch the panic and turn it into an [`ApplyError::Panic`] error.
    fn apply(&mut self, _: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self>;

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

/// A trait for stateful, fallible, idempotent functions that take a reference to their input.
pub trait RefWorker: Debug + Sized + 'static {
    /// The type of the input to this funciton.
    type Input: Send;
    /// The type of the output from this function.
    type Output: Send;
    /// The type of error produced by this function.
    type Error: Send + Debug;

    fn apply_ref(&mut self, _: &Self::Input, _: &Context<Self::Input>) -> RefWorkerResult<Self>;
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

    fn apply(&mut self, input: Self::Input, ctx: &Context<Self::Input>) -> WorkerResult<Self> {
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
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::{ApplyRefError, RefWorker, RefWorkerResult, Worker, WorkerResult};
    use crate::bee::{ApplyError, Context};

    #[derive(Debug)]
    struct MyWorker;

    impl Worker for MyWorker {
        type Input = u8;
        type Output = u8;
        type Error = ();

        fn apply(&mut self, input: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
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

        fn apply_ref(
            &mut self,
            input: &Self::Input,
            _: &Context<Self::Input>,
        ) -> RefWorkerResult<Self> {
            match *input {
                0 => Err(ApplyRefError::Retryable(())),
                1 => Err(ApplyRefError::Fatal(())),
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
