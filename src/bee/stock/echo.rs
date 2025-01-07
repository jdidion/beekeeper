use crate::bee::{Context, Worker, WorkerResult};
use std::fmt::Debug;
use std::marker::PhantomData;

/// A `Worker` that simply returns the input.
#[derive(Debug)]
pub struct Echo<T>(PhantomData<T>);

impl<T> Default for Echo<T> {
    fn default() -> Self {
        Echo(PhantomData)
    }
}

impl<T: Send + Debug + 'static> Worker for Echo<T> {
    type Input = T;
    type Output = T;
    type Error = ();

    #[inline]
    fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
        Ok(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bee::Context;

    #[test]
    fn test_echo() {
        let mut echo = Echo::<u8>::default();
        assert_eq!(1, echo.apply(1, &Context::empty()).unwrap());
    }
}
