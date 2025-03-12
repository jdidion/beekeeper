use crate::bee::{ApplyError, Context, Worker, WorkerResult};
use crate::boxed::BoxedFnOnce;
use crate::panic::Panic;
use derive_more::Debug;
use std::marker::PhantomData;
use std::{any, fmt};

/// A `Worker` that executes infallible `Thunk<T>`s when applied.
#[derive(Debug)]
#[debug("ThunkWorker<{}>", any::type_name::<T>())]
pub struct ThunkWorker<T>(PhantomData<T>);

impl<T> Default for ThunkWorker<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> Clone for ThunkWorker<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<T: Send + fmt::Debug + 'static> Worker for ThunkWorker<T> {
    type Input = Thunk<T>;
    type Output = T;
    type Error = ();

    #[inline]
    fn apply(&mut self, f: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
        Ok(f.0.call_box())
    }
}

/// A `Worker` that executes fallible `Thunk<Result<T, E>>`s when applied.
#[derive(Debug)]
#[debug("FunkWorker<{}, {}>", any::type_name::<T>(), any::type_name::<E>())]
pub struct FunkWorker<T, E>(PhantomData<T>, PhantomData<E>);

impl<T, E> Default for FunkWorker<T, E> {
    fn default() -> Self {
        Self(PhantomData, PhantomData)
    }
}

impl<T, E> Clone for FunkWorker<T, E> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<T, E> Worker for FunkWorker<T, E>
where
    T: Send + fmt::Debug + 'static,
    E: Send + fmt::Debug + 'static,
{
    type Input = Thunk<Result<T, E>>;
    type Output = T;
    type Error = E;

    #[inline]
    fn apply(&mut self, f: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
        f.0.call_box()
            .map_err(|error| ApplyError::Fatal { error, input: None })
    }
}

/// A `Worker` that executes `Thunk<T>`s that may panic. A panic is caught and returned as an
/// `ApplyError::Panic` error.
#[derive(Debug)]
#[debug("PunkWorker<{}>", any::type_name::<T>())]
pub struct PunkWorker<T>(PhantomData<T>);

impl<T> Default for PunkWorker<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> Clone for PunkWorker<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<T: Send + fmt::Debug + 'static> Worker for PunkWorker<T> {
    type Input = Thunk<T>;
    type Output = T;
    type Error = ();

    fn apply(&mut self, f: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
        Panic::try_call_boxed(None, f.0).map_err(|payload| ApplyError::Panic {
            input: None,
            payload,
        })
    }
}

/// A wrapper around a closure that can be executed exactly once by a worker in a `Hive`.
#[derive(Debug)]
#[debug("Thunk<{}>", any::type_name::<T>())]
pub struct Thunk<T>(Box<dyn BoxedFnOnce<Output = T> + Send>);

impl<T, F: FnOnce() -> T + Send + 'static> From<F> for Thunk<T> {
    fn from(f: F) -> Self {
        Self(Box::new(f))
    }
}

impl<T, E> Thunk<Result<T, E>> {
    pub fn fallible<F: FnOnce() -> Result<T, E> + Send + 'static>(f: F) -> Self {
        Self(Box::new(f))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::bee::Context;

    #[test]
    fn test_thunk() {
        let mut worker = ThunkWorker::<u8>::default();
        let thunk = Thunk::from(|| 5);
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
            Err(ApplyError::Fatal {
                input: None,
                error: _error
            })
        ));
    }
}
