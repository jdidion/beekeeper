pub(crate) trait BoxedFnOnce {
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
