use crate::bee::{Context, Worker, WorkerResult};
use derive_more::Debug;
use std::marker::PhantomData;
use std::{any, fmt};

/// A `Worker` that simply returns the input.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[debug("EchoWorker<{}>", any::type_name::<T>())]
pub struct EchoWorker<T>(PhantomData<T>);

impl<T: Send + fmt::Debug + 'static> Worker for EchoWorker<T> {
    type Input = T;
    type Output = T;
    type Error = ();

    #[inline]
    fn apply(&mut self, input: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
        Ok(input)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::bee::Context;

    #[test]
    fn test_echo() {
        let mut echo = EchoWorker::<u8>::default();
        assert_eq!(1, echo.apply(1, &Context::empty()).unwrap());
    }
}
