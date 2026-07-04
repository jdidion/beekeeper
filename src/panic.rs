//! Data type that wraps a `panic` payload.
use super::boxed::BoxedFnOnce;
use derive_more::Debug;
use std::any::Any;
use std::fmt;
use std::panic::AssertUnwindSafe;

pub type PanicPayload = Box<dyn Any + Send + 'static>;

/// Wraps a payload from a caught `panic` with an optional `detail`.
#[derive(Debug)]
pub struct Panic<T: Send + fmt::Debug + Eq> {
    #[debug("<panic! arg>")]
    payload: PanicPayload,
    detail: Option<T>,
}

impl<T: Send + fmt::Debug + Eq> Panic<T> {
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

impl<T: Send + fmt::Debug + Eq> PartialEq for Panic<T> {
    fn eq(&self, other: &Self) -> bool {
        (*self.payload).type_id() == (*other.payload).type_id() && self.detail == other.detail
    }
}

impl<T: Send + fmt::Debug + Eq> Eq for Panic<T> {}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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
    fn test_payload() {
        let result = Panic::try_call(Some("detail".to_string()), || panic!("boom"));
        let panic = result.unwrap_err();
        // the payload is the `&str` passed to `panic!`
        let payload = panic.payload();
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
    }

    #[test]
    fn test_eq() {
        // same payload type and same detail => equal
        let a = Panic::<String>::new("boom", Some("d1".to_string()));
        let b = Panic::<String>::new("kaboom", Some("d1".to_string()));
        assert_eq!(a, b);
        // same payload type but different detail => not equal
        let c = Panic::<String>::new("boom", Some("d2".to_string()));
        assert_ne!(a, c);
        // a `Panic` always equals itself (reflexive)
        assert!(a == a);
    }

    #[test]
    #[should_panic]
    fn test_resume_panic() {
        let result = Panic::try_call("test".into(), || panic!("panic!"));
        let panic = result.unwrap_err();
        panic.resume();
    }
}
