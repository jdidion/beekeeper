mod call;
mod echo;
mod thunk;

pub use call::{Caller, OnceCaller, RefCaller, RetryCaller};
pub use echo::Echo;
pub use thunk::{FunkWorker, Thunk, ThunkWorker};

use crate::hive::{Builder, Hive, HiveResult, TaskResultIteratorExt};
use crate::task::{ApplyError, Context, DefaultQueen};
use std::fmt::Debug;

/// Convenience function that creates a `Hive` with `num_threads` worker threads that execute the
/// provided callable on the provided inputs and returns a `Vec` of the results.
///
/// The provided function should not panic. For falible operations, use one of the `try_map_*`
/// functions.
///
/// # Examples
///
/// # fn main() {
/// let outputs = drudge::util::map(4, 3..9usize, |i| i + 1);
/// assert_eq!(outputs.into_iter().sum(), 42);
/// # }
pub fn map<I, O, Inputs, F>(num_threads: usize, inputs: Inputs, f: F) -> Vec<O>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    Inputs: IntoIterator<Item = I>,
    F: FnMut(I) -> O + Send + Sync + Clone + 'static,
{
    Builder::default()
        .num_threads(num_threads)
        .build_with(Caller::of(f))
        .map(inputs)
        .into_outputs()
        .collect()
}

/// Convenience function that creates a `Hive` with `num_threads` worker threads that execute the
/// provided callable on the provided inputs and returns a `Vec` of the results, or an error if any
/// of the tasks failed.
///
/// # Examples
///
/// # fn main() {
/// let result = drudge::util:;try_map(
///     4, 0..10, |i| if i == 5 { Err("No fives allowed!") } else { Ok(i * i) }
/// );
/// assert!(result.is_err());
/// # }
pub fn try_map<I, O, E, Inputs, F>(
    num_threads: usize,
    inputs: Inputs,
    f: F,
) -> HiveResult<Vec<O>, OnceCaller<I, O, E, F>>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    E: Send + Sync + Debug + 'static,
    Inputs: IntoIterator<Item = I>,
    F: FnMut(I) -> Result<O, E> + Send + Sync + Clone + 'static,
{
    Builder::default()
        .num_threads(num_threads)
        .build_with(OnceCaller::of(f))
        .map(inputs)
        .collect()
}

/// Convenience function that creates a `Hive` with `num_threads` worker threads that execute the
/// provided callable on the provided inputs and returns a `Vec` of the results, or an error if any
/// of the tasks failed.
///
/// A task that fails with an `ApplyError::Retryable` error will be retried up to `max_retries`
/// times.
///
/// # Examples
///
/// # use drudge::task::ApplyError;
///
/// # fn main() {
/// let result = drudge::util::map_retryable(4, 3, 0..10, |i| if i == 5 {
///     Err(ApplyError::NotRetryable { input: i, error: "No fives allowed!".into() })
/// } else if i == 7 {
///     Err(ApplyError::Retryable { input: i, error: "Re-roll a 7".into() })
/// } else {
///     Ok(i * i)
/// });
/// assert!(result.is_err());
/// # }
pub fn try_map_retryable<I, O, E, Inputs, F>(
    num_threads: usize,
    max_retries: u32,
    inputs: Inputs,
    f: F,
) -> HiveResult<Vec<O>, RetryCaller<I, O, E, F>>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    E: Send + Sync + Debug + 'static,
    Inputs: IntoIterator<Item = I>,
    F: FnMut(I, &Context) -> Result<O, ApplyError<I, E>> + Send + Sync + Clone + 'static,
{
    Builder::default()
        .num_threads(num_threads)
        .max_retries(max_retries)
        .build_with(RetryCaller::of(f))
        .map(inputs)
        .collect()
}

/// Convenience function that returns a `Hive` configured with the global defaults, and the
/// specified number of `Echo` workers.
pub fn echo_hive<T: Send + Sync + Debug + 'static>(
    num_threads: usize,
) -> Hive<Echo<T>, DefaultQueen<Echo<T>>> {
    Builder::default()
        .num_threads(num_threads)
        .build_with_default()
}

/// Convenience function that returns a `Hive` configured with the global defaults, and the
/// specified number of workers that execute `Thunk<T>`s, i.e. closures that return `T`.
pub fn thunk_hive<'a, T: Send + Sync + Debug + 'static>(
    num_threads: usize,
) -> Hive<ThunkWorker<T>, DefaultQueen<ThunkWorker<T>>> {
    Builder::default()
        .num_threads(num_threads)
        .build_with_default()
}

/// Convenience function that returns a `Hive` configured with the global defaults, and the
/// specified number of workers that execute `Thunk<Result<T, E>>`s, i.e. closures that return a
/// `Result<T, E>` (a `funk` is a fallible `thunk`).
pub fn funk_hive<'a, T, E>(
    num_threads: usize,
) -> Hive<FunkWorker<T, E>, DefaultQueen<FunkWorker<T, E>>>
where
    T: Send + Sync + Debug + 'static,
    E: Send + Sync + Debug + 'static,
{
    Builder::default()
        .num_threads(num_threads)
        .build_with_default()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_map() {
        let outputs = super::map(4, 0..100, |i| i + 1);
        assert_eq!(outputs, (1..=100).collect::<Vec<_>>());
    }
}
