/// Trait implemented by a `Box`ed callable that can be executed once.
pub trait BoxedFnOnce {
    /// The type returned by the callable when called.
    type Output;

    /// Calls the boxed callable and returns the result.
    fn call_box(self: Box<Self>) -> Self::Output;
}

/// Blanket implementation of `BoxedFnOnce` for `FnOnce`.
impl<T, F: FnOnce() -> T> BoxedFnOnce for F {
    type Output = T;

    #[inline]
    fn call_box(self: Box<F>) -> Self::Output {
        (*self)()
    }
}
