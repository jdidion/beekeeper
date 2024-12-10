use crate::task::{ApplyError, Context, Worker, WorkerResult};
use std::{fmt::Debug, marker::PhantomData};

/// A `Worker` that executes infallible `Thunk<T>`s when applied.
#[derive(Debug)]
pub struct ThunkWorker<T>(PhantomData<T>);

impl<T> Default for ThunkWorker<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: Send + Debug + 'static> Worker for ThunkWorker<T> {
    type Input = Thunk<'static, T>;
    type Output = T;
    type Error = ();

    #[inline]
    fn apply(&mut self, f: Self::Input, _: &Context) -> WorkerResult<Self> {
        Ok(f.0.call_box())
    }
}

/// A `Worker` that executes fallible `Thunk<Result<T, E>>`s when applied.
#[derive(Debug)]
pub struct FunkWorker<T, E>(PhantomData<T>, PhantomData<E>);

impl<T, E> Default for FunkWorker<T, E> {
    fn default() -> Self {
        Self(PhantomData, PhantomData)
    }
}

impl<T: Send + Debug + 'static, E: Send + Debug + 'static> Worker for FunkWorker<T, E> {
    type Input = Thunk<'static, Result<T, E>>;
    type Output = T;
    type Error = E;

    #[inline]
    fn apply(&mut self, f: Self::Input, _: &Context) -> WorkerResult<Self> {
        f.0.call_box()
            .map_err(|error| ApplyError::NotRetryable { error, input: None })
    }
}

/// A wrapper around a closure that can be executed exactly once by a worker in a `Hive`.
pub struct Thunk<'a, T>(Box<dyn BoxedFnOnce<Output = T> + Send + 'a>);

impl<'a, T> Thunk<'a, T> {
    pub fn of<F: FnOnce() -> T + Send + 'static>(f: F) -> Self {
        Self(Box::new(f))
    }
}

impl<'a, T, E> Thunk<'a, Result<T, E>> {
    pub fn fallible<F: FnOnce() -> Result<T, E> + Send + 'static>(f: F) -> Self {
        Self(Box::new(f))
    }
}

impl<'a, T> Debug for Thunk<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Thunk")
    }
}

trait BoxedFnOnce {
    type Output;

    fn call_box(self: Box<Self>) -> Self::Output;
}

impl<T, F: FnOnce() -> T> BoxedFnOnce for F {
    type Output = T;

    #[inline]
    fn call_box(self: Box<F>) -> Self::Output {
        (*self)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Context;

    #[test]
    fn test_thunk() {
        let mut worker = ThunkWorker::<u8>::default();
        let thunk = Thunk::of(|| 5);
        assert_eq!(5, worker.apply(thunk, &Context::empty()).unwrap());
    }

    #[test]
    fn test_funk_ok() {
        let mut worker = FunkWorker::<u8, String>::default();
        let funk = Thunk::fallible(|| Ok(1));
        assert_eq!(1, worker.apply(funk, &Context::empty()).unwrap())
    }

    #[test]
    fn test_funk_error() {
        let mut worker = FunkWorker::<u8, String>::default();
        let funk = Thunk::fallible(|| Err("failure".into()));
        let result = worker.apply(funk, &Context::empty());
        let _error = String::from("failure");
        assert!(matches!(
            result,
            Err(ApplyError::NotRetryable {
                input: None,
                error: _error
            })
        ));
    }
}
