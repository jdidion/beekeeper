//! Worker implementations that wrap callables (closures or function pointers that are `FnMut`).
use crate::bee::{
    ApplyError, ApplyRefError, Context, RefWorker, RefWorkerResult, Worker, WorkerResult,
};
use derive_more::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::{any, fmt};

/// Wraps a closure or function pointer and calls it when applied. For this `Callable` to be
/// useable by a `Worker`, the function must be `FnMut` *and* `Clone`able.
///
/// TODO: we could provide a better `Debug` implementation by providing a macro that can wrap a
/// closure and store the text of the function, and then change all the Workers to take a
/// `F: Deref<Target = Fn>`.
/// See https://users.rust-lang.org/t/is-it-possible-to-implement-debug-for-fn-type/14824/3
#[derive(Debug)]
struct Callable<I, O, E, F> {
    #[debug(skip)]
    f: F,
    #[debug("{}", any::type_name::<I>())]
    i: PhantomData<I>,
    #[debug("{}", any::type_name::<O>())]
    o: PhantomData<O>,
    #[debug("{}", any::type_name::<E>())]
    e: PhantomData<E>,
}

impl<I, O, E, F> Callable<I, O, E, F> {
    fn of(f: F) -> Self {
        Self {
            f,
            i: PhantomData,
            o: PhantomData,
            e: PhantomData,
        }
    }

    fn into_inner(self) -> F {
        self.f
    }
}

impl<I, O, E, F: Clone> Clone for Callable<I, O, E, F> {
    fn clone(&self) -> Self {
        Self::of(self.f.clone())
    }
}

impl<I, O, E, F> Deref for Callable<I, O, E, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.f
    }
}

impl<I, O, E, F> DerefMut for Callable<I, O, E, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.f
    }
}

/// A `Caller` that executes its function once on the input and returns the output. The function
/// should not panic.
#[derive(Debug)]
pub struct Caller<I, O, F> {
    callable: Callable<I, O, (), F>,
}

impl<I, O, F> Caller<I, O, F> {
    /// Returns the wrapped callable.
    pub fn into_inner(self) -> F {
        self.callable.into_inner()
    }
}

impl<I, O, F> From<F> for Caller<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: FnMut(I) -> O + Clone + 'static,
{
    fn from(f: F) -> Self {
        Caller {
            callable: Callable::of(f),
        }
    }
}

impl<I, O, F: Clone> Clone for Caller<I, O, F> {
    fn clone(&self) -> Self {
        Self {
            callable: self.callable.clone(),
        }
    }
}

impl<I, O, F> Worker for Caller<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: FnMut(I) -> O + Clone + 'static,
{
    type Input = I;
    type Output = O;
    type Error = ();

    #[inline]
    fn apply(&mut self, input: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
        Ok((self.callable)(input))
    }
}

/// A `Caller` that executes its function once on each input. The input value is consumed by the
/// function. If the function returns an error, it is wrapped in `ApplyError::Fatal`.
///
/// If ownership of the input value is not required, consider using `RefCaller` instead.
#[derive(Debug)]
pub struct OnceCaller<I, O, E, F> {
    callable: Callable<I, O, E, F>,
}

impl<I, O, E, F> OnceCaller<I, O, E, F> {
    /// Returns the wrapped callable.
    pub fn into_inner(self) -> F {
        self.callable.into_inner()
    }
}

impl<I, O, E, F> From<F> for OnceCaller<I, O, E, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    E: Send + Sync + fmt::Debug + 'static,
    F: FnMut(I) -> Result<O, E> + Clone + 'static,
{
    fn from(f: F) -> Self {
        OnceCaller {
            callable: Callable::of(f),
        }
    }
}

impl<I, O, E, F: Clone> Clone for OnceCaller<I, O, E, F> {
    fn clone(&self) -> Self {
        Self {
            callable: self.callable.clone(),
        }
    }
}

impl<I, O, E, F> Worker for OnceCaller<I, O, E, F>
where
    I: Send + 'static,
    O: Send + 'static,
    E: Send + fmt::Debug + 'static,
    F: FnMut(I) -> Result<O, E> + Clone + 'static,
{
    type Input = I;
    type Output = O;
    type Error = E;

    #[inline]
    fn apply(&mut self, input: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
        (self.callable)(input).map_err(|error| ApplyError::Fatal { error, input: None })
    }
}

/// A `Caller` that executes its function once on a reference to the input. If the function
/// returns an error, it is wrapped in `ApplyError::Fatal`.
///
/// The benefit of using `RefCaller` over `OnceCaller` is that the `Fatal` error
/// contains the input value for later recovery.
#[derive(Debug)]
pub struct RefCaller<I, O, E, F> {
    callable: Callable<I, O, E, F>,
}

impl<I, O, E, F> RefCaller<I, O, E, F> {
    /// Returns the wrapped callable.
    pub fn into_inner(self) -> F {
        self.callable.into_inner()
    }
}

impl<I, O, E, F> From<F> for RefCaller<I, O, E, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    E: Send + Sync + fmt::Debug + 'static,
    F: FnMut(&I) -> Result<O, E> + Clone + 'static,
{
    fn from(f: F) -> Self {
        RefCaller {
            callable: Callable::of(f),
        }
    }
}

impl<I, O, E, F: Clone> Clone for RefCaller<I, O, E, F> {
    fn clone(&self) -> Self {
        Self {
            callable: self.callable.clone(),
        }
    }
}

impl<I, O, E, F> RefWorker for RefCaller<I, O, E, F>
where
    I: Send + 'static,
    O: Send + 'static,
    E: Send + fmt::Debug + 'static,
    F: FnMut(&I) -> Result<O, E> + Clone + 'static,
{
    type Input = I;
    type Output = O;
    type Error = E;

    #[inline]
    fn apply_ref(
        &mut self,
        input: &Self::Input,
        _: &Context<Self::Input>,
    ) -> RefWorkerResult<Self> {
        (self.callable)(input).map_err(|error| ApplyRefError::Fatal(error))
    }
}

/// A `Caller` that returns a `Result<O, ApplyError>`. A result of `Err(ApplyError::Retryable)`
/// can be returned to indicate the task should be retried.
#[derive(Debug)]
pub struct RetryCaller<I, O, E, F> {
    callable: Callable<I, O, E, F>,
}

impl<I, O, E, F> RetryCaller<I, O, E, F> {
    /// Returns the wrapped callable.
    pub fn into_inner(self) -> F {
        self.callable.into_inner()
    }
}

impl<I, O, E, F> From<F> for RetryCaller<I, O, E, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    E: Send + Sync + fmt::Debug + 'static,
    F: FnMut(I, &Context<I>) -> Result<O, ApplyError<I, E>> + Clone + 'static,
{
    fn from(f: F) -> Self {
        RetryCaller {
            callable: Callable::of(f),
        }
    }
}

impl<I, O, E, F: Clone> Clone for RetryCaller<I, O, E, F> {
    fn clone(&self) -> Self {
        Self {
            callable: self.callable.clone(),
        }
    }
}

impl<I, O, E, F> Worker for RetryCaller<I, O, E, F>
where
    I: Send + 'static,
    O: Send + 'static,
    E: Send + fmt::Debug + 'static,
    F: FnMut(I, &Context<I>) -> Result<O, ApplyError<I, E>> + Clone + 'static,
{
    type Input = I;
    type Output = O;
    type Error = E;

    #[inline]
    fn apply(&mut self, input: Self::Input, ctx: &Context<Self::Input>) -> WorkerResult<Self> {
        (self.callable)(input, ctx)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::bee::Context;

    #[test]
    fn test_call() {
        let mut worker = Caller::from(|input: u8| input + 1);
        assert!(matches!(worker.apply(5, &Context::empty()), Ok(6)))
    }

    #[test]
    fn test_clone() {
        let worker1 = Caller::from(|input: u8| input + 1);
        let worker2 = worker1.clone();
        let f = worker2.into_inner();
        assert_eq!(f(5), 6);
    }

    #[allow(clippy::type_complexity)]
    fn try_caller() -> RetryCaller<
        (bool, u8),
        u8,
        String,
        impl FnMut((bool, u8), &Context<(bool, u8)>) -> Result<u8, ApplyError<(bool, u8), String>>
        + Clone
        + 'static,
    > {
        RetryCaller::from(|input: (bool, u8), _: &Context<(bool, u8)>| {
            if input.0 {
                Ok(input.1 + 1)
            } else {
                Err(ApplyError::Fatal {
                    input: Some(input),
                    error: "failure".into(),
                })
            }
        })
    }

    #[test]
    fn test_try_call_ok() {
        let mut worker = try_caller();
        assert!(matches!(worker.apply((true, 5), &Context::empty()), Ok(6)));
    }

    #[test]
    fn test_clone_retry_caller() {
        let worker1 = try_caller();
        let worker2 = worker1.clone();
        let mut f = worker2.into_inner();
        assert!(matches!(f((true, 5), &Context::empty()), Ok(6)));
    }

    #[test]
    fn test_try_call_fail() {
        let mut worker = try_caller();
        let result = worker.apply((false, 5), &Context::empty());
        let _error = String::from("failure");
        assert!(matches!(
            result,
            Err(ApplyError::Fatal {
                input: Some((false, 5)),
                error: _error
            })
        ));
    }

    #[allow(clippy::type_complexity)]
    fn once_caller() -> OnceCaller<
        (bool, u8),
        u8,
        String,
        impl FnMut((bool, u8)) -> Result<u8, String> + Clone + 'static,
    > {
        OnceCaller::from(|input: (bool, u8)| {
            if input.0 {
                Ok(input.1 + 1)
            } else {
                Err("failure".into())
            }
        })
    }

    #[test]
    fn test_once_call_ok() {
        let mut worker = once_caller();
        assert!(matches!(worker.apply((true, 5), &Context::empty()), Ok(6)));
    }

    #[test]
    fn test_clone_once_caller() {
        let worker1 = once_caller();
        let worker2 = worker1.clone();
        let mut f = worker2.into_inner();
        assert!(matches!(f((true, 5)), Ok(6)));
    }

    #[test]
    fn test_once_call_fail() {
        let mut worker = once_caller();
        let result = worker.apply((false, 5), &Context::empty());
        let _error = String::from("failure");
        assert!(matches!(
            result,
            Err(ApplyError::Fatal {
                input: None,
                error: _error
            })
        ));
    }

    #[allow(clippy::type_complexity)]
    fn ref_caller() -> RefCaller<
        (bool, u8),
        u8,
        String,
        impl FnMut(&(bool, u8)) -> Result<u8, String> + Clone + 'static,
    > {
        RefCaller::from(|input: &(bool, u8)| {
            if input.0 {
                Ok(input.1 + 1)
            } else {
                Err("failure".into())
            }
        })
    }

    #[test]
    fn test_ref_call_ok() {
        let mut worker = ref_caller();
        assert!(matches!(worker.apply((true, 5), &Context::empty()), Ok(6)));
    }

    #[test]
    fn test_clone_ref_caller() {
        let worker1 = ref_caller();
        let worker2 = worker1.clone();
        let mut f = worker2.into_inner();
        assert!(matches!(f(&(true, 5)), Ok(6)));
    }

    #[test]
    fn test_ref_call_fail() {
        let mut worker = ref_caller();
        let result = worker.apply((false, 5), &Context::empty());
        let _error = String::from("failure");
        assert!(matches!(
            result,
            Err(ApplyError::Fatal {
                input: Some((false, 5)),
                error: _error
            })
        ));
    }
}
