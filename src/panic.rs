use super::boxed::BoxedFnOnce;
use std::any::Any;
use std::fmt::Debug;
use std::panic::AssertUnwindSafe;

pub type PanicPayload = Box<dyn Any + Send + 'static>;

/// Wraps a payload from a caught `panic` with an optional `detail`.
#[derive(Debug)]
pub struct Panic<T: Send + Debug + Eq> {
    payload: PanicPayload,
    detail: Option<T>,
}

impl<T: Send + Debug + Eq> Panic<T> {
    /// Attempts to call the provided function `f` and catches any panic. Returns either the return
    /// value of the function or a `Panic` created from the panic payload and the provided `detail`.
    pub fn try_call<O, F: FnOnce() -> O>(detail: Option<T>, f: F) -> Result<O, Self> {
        std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(|payload| Self { payload, detail })
    }

    pub(crate) fn try_call_boxed<O, F: BoxedFnOnce<Output = O> + ?Sized>(
        detail: Option<T>,
        f: Box<F>,
    ) -> Result<O, Self> {
        std::panic::catch_unwind(AssertUnwindSafe(|| f.call_box()))
            .map_err(|payload| Self { payload, detail })
    }

    /// Returns the payload of the panic.
    pub fn payload(&self) -> &PanicPayload {
        &self.payload
    }

    /// Returns the optional detail of the panic.
    pub fn detail(&self) -> Option<&T> {
        self.detail.as_ref()
    }

    /// Consumes this `Panic` and resumes unwinding the thread.
    pub fn resume(self) -> ! {
        std::panic::resume_unwind(self.payload)
    }
}

impl<T: Send + Debug + Eq> PartialEq for Panic<T> {
    fn eq(&self, other: &Self) -> bool {
        (*self.payload).type_id() == (*other.payload).type_id() && self.detail == other.detail
    }
}

impl<T: Send + Debug + Eq> Eq for Panic<T> {}

#[cfg(test)]
mod tests {
    use super::Panic;
    use std::fmt::Debug;

    impl<T: Send + Debug + Eq> Panic<T> {
        /// Panics with `msg` and immediately catches it to create a new `Panic` instance for testing.
        pub fn new(msg: &str, detail: Option<T>) -> Self {
            let payload = std::panic::catch_unwind(|| panic!("{}", msg))
                .err()
                .unwrap();
            Self { payload, detail }
        }
    }

    #[test]
    fn test_catch_panic() {
        let result = Panic::try_call("test".into(), || panic!("panic!"));
        let panic = result.unwrap_err();
        assert_eq!(*panic.detail().unwrap(), "test");
    }

    #[test]
    #[should_panic]
    fn test_resume_panic() {
        let result = Panic::try_call("test".into(), || panic!("panic!"));
        let panic = result.unwrap_err();
        panic.resume();
    }
}
