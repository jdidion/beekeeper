//! Worker implementations that wrap callables (closures or function pointers that are `FnMut`).
use crate::task::{
    ApplyError, ApplyRefError, Context, RefWorker, RefWorkerResult, Worker, WorkerResult,
};
use std::fmt::Debug;
use std::marker::PhantomData;

/// Wraps a closure or function pointer and calls it when applied. For this `Callable` to be
/// useable by a `Worker`, the function must be `FnMut` *and* `Clone`able.
struct Callable<I, O, E, F> {
    f: F,
    i: PhantomData<I>,
    o: PhantomData<O>,
    e: PhantomData<E>,
}

impl<I, O, E, F> Callable<I, O, E, F> {
    fn of(f: F) -> Self {
        Self {
            f: f,
            i: PhantomData,
            o: PhantomData,
            e: PhantomData,
        }
    }
}

impl<I, O, E, F: Clone> Clone for Callable<I, O, E, F> {
    fn clone(&self) -> Self {
        Self::of(self.f.clone())
    }
}

/// A `Caller` that executes its function once on the input and returns the output. The function
/// should not panic.
pub struct Caller<I, O, F>(Callable<I, O, (), F>);

impl<I, O, F> Caller<I, O, F> {
    pub fn of(f: F) -> Self
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: FnMut(I) -> O + Clone + 'static,
    {
        Caller(Callable::of(f))
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
    fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
        Ok((&mut self.0.f)(input))
    }
}

impl<I, O, F: FnMut(I) -> O> Debug for Caller<I, O, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Caller")
    }
}

impl<I, O, F: Clone> Clone for Caller<I, O, F> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<I, O, F: FnMut(I) -> O + Clone + 'static> From<F> for Caller<I, O, F> {
    fn from(f: F) -> Self {
        Caller(Callable::of(f))
    }
}

/// A `Caller` that executes its function once on each input. The input value is consumed by the
/// function. If the function returns an error, it is wrapped in `ApplyError::NotRetryable`.
///
/// If ownership of the input value is not required, consider using `RefCaller` instead.
pub struct OnceCaller<I, O, E, F>(Callable<I, O, E, F>);

impl<I, O, E, F> OnceCaller<I, O, E, F> {
    pub fn of(f: F) -> Self
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        E: Send + Sync + Debug + 'static,
        F: FnMut(I) -> Result<O, E> + Clone + 'static,
    {
        OnceCaller(Callable::of(f))
    }
}

impl<I, O, E, F> Worker for OnceCaller<I, O, E, F>
where
    I: Send + 'static,
    O: Send + 'static,
    E: Send + Debug + 'static,
    F: FnMut(I) -> Result<O, E> + Clone + 'static,
{
    type Input = I;
    type Output = O;
    type Error = E;

    #[inline]
    fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
        (&mut self.0.f)(input).map_err(|error| ApplyError::Fatal { error, input: None })
    }
}

impl<I, O, E, F: FnMut(I) -> Result<O, E>> Debug for OnceCaller<I, O, E, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OnceCaller")
    }
}

impl<I, O, E, F: Clone> Clone for OnceCaller<I, O, E, F> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<I, O, E, F> From<F> for OnceCaller<I, O, E, F>
where
    F: FnMut(I) -> Result<O, E> + Clone + 'static,
{
    fn from(f: F) -> Self {
        OnceCaller(Callable::of(f))
    }
}

/// A `Caller` that executes its function once on a reference to the input. If the function
/// returns an error, it is wrapped in `ApplyError::NotRetryable`.
///
/// The benefit of using `RefCaller` over `OnceCaller` is that the `NotRetryable` error
/// contains the input value for later recovery.
pub struct RefCaller<I, O, E, F>(Callable<I, O, E, F>);

impl<I, O, E, F> RefCaller<I, O, E, F> {
    pub fn of(f: F) -> Self
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        E: Send + Sync + Debug + 'static,
        F: FnMut(&I) -> Result<O, E> + Clone + 'static,
    {
        RefCaller(Callable::of(f))
    }
}

impl<I, O, E, F> RefWorker for RefCaller<I, O, E, F>
where
    I: Send + 'static,
    O: Send + 'static,
    E: Send + Debug + 'static,
    F: FnMut(&I) -> Result<O, E> + Clone + 'static,
{
    type Input = I;
    type Output = O;
    type Error = E;

    #[inline]
    fn apply_ref(&mut self, input: &Self::Input, _: &Context) -> RefWorkerResult<Self> {
        (&mut self.0.f)(input).map_err(|error| ApplyRefError::NotRetryable(error))
    }
}

impl<I, O, E, F: FnMut(&I) -> Result<O, E>> Debug for RefCaller<I, O, E, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RefCaller")
    }
}

impl<I, O, E, F: Clone> Clone for RefCaller<I, O, E, F> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<I, O, E, F> From<F> for RefCaller<I, O, E, F>
where
    F: FnMut(&I) -> Result<O, E> + Clone + 'static,
{
    fn from(f: F) -> Self {
        RefCaller(Callable::of(f))
    }
}

pub struct RetryCaller<I, O, E, F>(Callable<I, O, E, F>);

impl<I, O, E, F> RetryCaller<I, O, E, F> {
    pub fn of(f: F) -> Self
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        E: Send + Sync + Debug + 'static,
        F: FnMut(I, &Context) -> Result<O, ApplyError<I, E>> + Clone + 'static,
    {
        RetryCaller(Callable::of(f))
    }
}

impl<I, O, E, F> Worker for RetryCaller<I, O, E, F>
where
    I: Send + 'static,
    O: Send + 'static,
    E: Send + Debug + 'static,
    F: FnMut(I, &Context) -> Result<O, ApplyError<I, E>> + Clone + 'static,
{
    type Input = I;
    type Output = O;
    type Error = E;

    #[inline]
    fn apply(&mut self, input: Self::Input, ctx: &Context) -> WorkerResult<Self> {
        (&mut self.0.f)(input, ctx)
    }
}

impl<I, O, E, F: Clone> Clone for RetryCaller<I, O, E, F> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<I, O, E, F: FnMut(I, &Context) -> Result<O, ApplyError<I, E>>> Debug
    for RetryCaller<I, O, E, F>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RetryCaller")
    }
}

impl<I, O, E, F> From<F> for RetryCaller<I, O, E, F>
where
    F: FnMut(I, &Context) -> Result<O, ApplyError<I, E>> + Clone + 'static,
{
    fn from(f: F) -> Self {
        RetryCaller(Callable::of(f))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Context;

    #[test]
    fn test_call() {
        let mut worker = Caller::of(|input: u8| input + 1);
        assert!(matches!(worker.apply(5, &Context::empty()), Ok(6)))
    }

    fn try_caller() -> RetryCaller<
        (bool, u8),
        u8,
        String,
        impl FnMut((bool, u8), &Context) -> Result<u8, ApplyError<(bool, u8), String>> + Clone + 'static,
    > {
        RetryCaller::of(|input: (bool, u8), _: &Context| {
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

    fn once_caller() -> OnceCaller<
        (bool, u8),
        u8,
        String,
        impl FnMut((bool, u8)) -> Result<u8, String> + Clone + 'static,
    > {
        OnceCaller::of(|input: (bool, u8)| {
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

    fn ref_caller() -> RefCaller<
        (bool, u8),
        u8,
        String,
        impl FnMut(&(bool, u8)) -> Result<u8, String> + Clone + 'static,
    > {
        RefCaller::of(|input: &(bool, u8)| {
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
