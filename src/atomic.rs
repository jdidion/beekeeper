//! This module provides a common API for wrappers of `std::sync::atomic` types, enabling them to
//! be used in a generic context.
//!
//! TODO: The `Atomic` and `AtomicNumeric` traits and implementations could be replaced with the
//! equivalents from the `atomic`, `atomig`, or `radium` crates, but none of those seem to be
//! well-maintained at this point.

use paste::paste;
use std::fmt::Debug;
use std::ops::Add;
pub use std::sync::atomic::Ordering;

/// Trait for wrappers of [`atomic`](std::sync::atomic) types that provides a common API.
pub trait Atomic<T: Clone + Debug + Default>: Clone + Debug + Default + From<T> + Sync {
    /// Returns the current value of this `Atomic` using `Acquire` ordering.
    fn get(&self) -> T;

    /// Sets the value of this `Atomic` using `Release` ordering and returns the previous value.
    fn set(&self, value: T) -> T;

    /// Loads the current value of this `Atomic` using `AcqRel` ordering and calls `f`. If `f`
    /// returns `Some`, this atomic is updated with the new value using `Release` ordering and the
    /// previous value is returned. Otherwise the current value is returned.
    fn set_with<F: FnMut(T) -> Option<T>>(&self, f: F) -> T;

    /// Consumes this `Atomic` and returns the inner value.
    fn into_inner(self) -> T;
}

#[derive(Clone, Debug)]
pub struct Orderings {
    pub load: Ordering,
    pub swap: Ordering,
    pub fetch_update_set: Ordering,
    pub fetch_update_fetch: Ordering,
    pub fetch_add: Ordering,
    pub fetch_sub: Ordering,
}

impl Default for Orderings {
    fn default() -> Self {
        Orderings {
            load: Ordering::Acquire,
            swap: Ordering::Release,
            fetch_update_set: Ordering::AcqRel,
            fetch_update_fetch: Ordering::Acquire,
            fetch_add: Ordering::AcqRel,
            fetch_sub: Ordering::AcqRel,
        }
    }
}

/// Generates a wrapper for primitive type `T` that implement the `Atomic<T>`, `Clone`, `Debug`,
/// and `From<T>` traits.
macro_rules! atomic {
    ($type:ident) => {
        paste! {
            #[derive(Default)]
            pub struct [<Atomic $type:camel>] {
                inner: std::sync::atomic::[<Atomic $type:camel>],
                orderings: Orderings,
            }

            impl [<Atomic $type:camel>] {
                #[allow(dead_code)]
                pub fn new(value: $type, orderings: Orderings) -> Self {
                    Self {
                        inner: value.into(),
                        orderings,
                    }
                }
            }

            impl Atomic<$type> for [<Atomic $type:camel>] {
                fn get(&self) -> $type {
                    self.inner.load(self.orderings.load)
                }

                fn set(&self, value: $type) -> $type {
                    self.inner.swap(value, self.orderings.swap)
                }

                fn set_with<F: FnMut($type) -> Option<$type>>(&self, f: F) -> $type {
                    match self.inner.fetch_update(
                        self.orderings.fetch_update_set,
                        self.orderings.fetch_update_fetch,
                        f
                    ) {
                        Ok(prev) | Err(prev) => prev,
                    }
                }

                fn into_inner(self) -> $type {
                    self.inner.into_inner()
                }

            }

            impl Clone for [<Atomic $type:camel>] {
                fn clone(&self) -> Self {
                    Self { inner: self.get().into(), orderings: self.orderings.clone() }
                }
            }

            impl Debug for [<Atomic $type:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    self.inner.fmt(f)
                }
            }

            impl From<$type> for [<Atomic $type:camel>] {
                fn from(value: $type) -> Self {
                    [<Atomic $type:camel>] {
                        inner: std::sync::atomic::[<Atomic $type:camel>]::new(value),
                        orderings: Orderings::default(),
                    }
                }
            }
        }
    };
}

/// Trait for wrappers of [`atomic`](std::sync::atomic) numeric types that provides a common API.
pub trait AtomicNumber<T: Clone + Debug + Default>: Atomic<T> {
    /// Mutably adds `rhs` to the current value of this `Atomic` using `AcqRel` ordering and
    /// returns the previous value.
    fn add(&self, rhs: T) -> T;

    /// Mutably subtracts `rhs` from the current value of this `Atomic` using `AcqRel` ordering and
    /// returns the previous value.
    fn sub(&self, rhs: T) -> T;
}

/// Generates a wrapper for numeric type `T` that implements the `Atomic` and `AtomicNumber` traits.
macro_rules! atomic_number {
    ($type:ident) => {
        paste! {
            atomic!($type);

            impl AtomicNumber<$type> for [<Atomic $type:camel>] {
                fn add(&self, value: $type) -> $type {
                    self.inner.fetch_add(value, self.orderings.fetch_add)
                }

                fn sub(&self, value: $type) -> $type {
                    self.inner.fetch_sub(value, self.orderings.fetch_sub)
                }
            }
        }
    };
}

atomic!(bool);
atomic_number!(u32);
atomic_number!(u64);
atomic_number!(usize);

/// Wrapper for [`RwLock`](parking_lot::RwLock) that implements the `Atomic` trait. This enables
/// any type that is `Clone + Default` to be used in an `Atomic` context.
#[derive(Default)]
pub struct AtomicAny<T: Clone + Debug + Default + Sync + Send>(parking_lot::RwLock<T>);

impl<T: Clone + Debug + Default + Sync + Send + PartialEq> Atomic<T> for AtomicAny<T> {
    fn get(&self) -> T {
        self.0.read().clone()
    }

    fn set(&self, value: T) -> T {
        let mut val = self.0.write();
        let old_val = val.clone();
        *val = value;
        old_val
    }

    fn set_with<F: FnMut(T) -> Option<T>>(&self, mut f: F) -> T {
        let mut val = self.0.write();
        let cur_val = val.clone();
        if let Some(new_val) = f(cur_val.clone()) {
            *val = new_val;
        }
        cur_val
    }

    fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

impl<T: Clone + Debug + Default + Sync + Send + PartialEq> Debug for AtomicAny<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(f)
    }
}

impl<T: Clone + Debug + Default + Sync + Send> Clone for AtomicAny<T> {
    fn clone(&self) -> Self {
        Self(parking_lot::RwLock::new(self.0.read().clone()))
    }
}

impl<T: Clone + Debug + Default + Sync + Send> From<T> for AtomicAny<T> {
    fn from(value: T) -> Self {
        AtomicAny(parking_lot::RwLock::new(value))
    }
}

impl<T: Clone + Debug + Default + Sync + Send + PartialEq> PartialEq for AtomicAny<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

/// A wrapper around an `Option<P>` with different behavior in single- and multi-threaded contexts:
///
/// * The `Unsync` variant wraps `Option<P>`. It is intended to be used in a single-threaded
///   context, where the value can be set only via regular mutability (using the `set` method).
/// * The `Sync` variant wraps `Option<Atomic<P>>`. It is intended to be used in a multi-threaded
///   context, where the value can be set either by regular or interior mutability.
#[derive(Clone, PartialEq, Eq)]
pub enum AtomicOption<P, A>
where
    P: Clone + Debug + Default,
    A: Atomic<P>,
{
    Unsync(Option<P>),
    Sync(Option<A>),
}

impl<P, A> AtomicOption<P, A>
where
    P: Clone + Debug + Default,
    A: Atomic<P>,
{
    /// Returns the value if it is set.
    pub fn get(&self) -> Option<P> {
        match self {
            Self::Unsync(opt) => opt.clone(),
            Self::Sync(opt) => opt.as_ref().map(|atomic| atomic.get()),
        }
    }

    /// Returns the value if it is set, or the default value if it is not.
    pub fn get_or_default(&self) -> P {
        self.get().unwrap_or_default()
    }

    /// Sets the value to `value` using regular mutability. Returns the previous value.
    pub fn set(&mut self, value: Option<P>) -> Option<P> {
        match (self, value) {
            (Self::Sync(opt), Some(value)) => {
                opt.replace(value.into()).map(|atomic| atomic.into_inner())
            }
            (Self::Sync(opt), None) => opt.take().map(|atomic| atomic.into_inner()),
            (Self::Unsync(opt), Some(value)) => opt.replace(value),
            (Self::Unsync(opt), None) => opt.take(),
        }
    }

    /// If this is an `Unsync` variant, consumes `self` and returns the corresponding `Sync`
    /// variant. Otherwise returns `self`.
    pub fn into_sync(self) -> Self {
        if let Self::Unsync(opt) = self {
            Self::Sync(opt.map(A::from))
        } else {
            self
        }
    }

    /// Returns a `Sync` variant that is set from the default value of the wrapped `Atomic` if the
    /// value is unset, otherwise returns `self.into_sync()`.
    pub fn into_sync_default(self) -> Self {
        match self {
            Self::Unsync(None) | Self::Sync(None) => Self::Sync(Some(A::from(P::default()))),
            Self::Unsync(opt) => Self::Sync(opt.map(A::from)),
            _ => self,
        }
    }

    /// If this is a `Sync` variant, consumes `self` and returns the corresponding `Unsync` variant
    /// by consuming the wrapped `Atomic` and `take`ing its current value (which may panic if the
    /// inner value cannot be `take`n). Otherwise returns `self`.
    pub fn into_unsync(self) -> Self {
        if let Self::Sync(opt) = self {
            Self::Unsync(opt.map(|atomic| atomic.into_inner()))
        } else {
            self
        }
    }
}

/// Errors for invalid interior mutability operations.
#[derive(Debug, thiserror::Error)]
pub enum MutError {
    #[error("cannot use interior mutability to modify value in Unsync variant")]
    Unsync,
    #[error("cannot use interior mutability to modify value in an unset Sync variant")]
    Unset,
}

impl<P, A> AtomicOption<P, A>
where
    P: Copy + Debug + Default + Add<P, Output = P>,
    A: AtomicNumber<P>,
{
    /// If this is a `Sync` variant whose value is `Some`, updates the value to be the sum of
    /// the current value and `value` and returns the previous value. Otherwise returns a
    /// `MutError`.
    pub fn add(&self, rhs: P) -> Result<P, MutError> {
        match self {
            Self::Unsync(_) => Err(MutError::Unsync),
            Self::Sync(None) => Err(MutError::Unset),
            Self::Sync(Some(atomic)) => Ok(atomic.add(rhs)),
        }
    }
}

impl<P, A> AtomicOption<P, A>
where
    P: Copy + Debug + Default + PartialOrd<P>,
    A: AtomicNumber<P>,
{
    /// If this is a `Sync` variant whose value is `Some`, sets the value to the maximum of the
    /// current value and `rhs` and returns the previous value. Otherwise returns a `MutError`.
    pub fn set_max(&self, rhs: P) -> Result<P, MutError> {
        match self {
            Self::Unsync(_) => Err(MutError::Unsync),
            Self::Sync(None) => Err(MutError::Unset),
            Self::Sync(Some(atomic)) => {
                Ok(atomic.set_with(move |current| (current < rhs).then_some(rhs)))
            }
        }
    }
}

impl<P, A> Default for AtomicOption<P, A>
where
    P: Clone + Debug + Default,
    A: Atomic<P>,
{
    fn default() -> Self {
        Self::Unsync(None)
    }
}

impl<P, A> Debug for AtomicOption<P, A>
where
    P: Clone + Debug + Default,
    A: Atomic<P>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsync(None) | Self::Sync(None) => write!(f, "None"),
            Self::Unsync(Some(val)) => val.fmt(f),
            Self::Sync(Some(val)) => val.fmt(f),
        }
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use super::{Atomic, AtomicOption, MutError};
    use std::fmt::Debug;

    impl<P, A> AtomicOption<P, A>
    where
        P: Clone + Debug + Default,
        A: Atomic<P>,
    {
        /// Sets the value to the result of applying `f` to the current value using interior
        /// mutability. If `f` returns `Some(new_value)`, the value is updated and the previous value
        /// is returned, otherwise the value is not updated and an error is returned.
        pub fn try_update_with<F: FnMut(P) -> Option<P>>(&self, f: F) -> Result<P, MutError> {
            match self {
                Self::Unsync(_) => Err(MutError::Unsync),
                Self::Sync(None) => Err(MutError::Unset),
                Self::Sync(Some(atomic)) => Ok(atomic.set_with(f)),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::atomic::{AtomicAny, AtomicOption, MutError};

        #[test]
        fn test_try_update_with() {
            let mut a: AtomicOption<String, AtomicAny<String>> = AtomicOption::default();
            a.set(Some("hello".into()));
            let b = a.into_sync();
            assert_eq!(b.try_update_with(|_| None).unwrap(), "hello");
            assert_eq!(b.get(), Some("hello".into()));
            assert_eq!(
                b.try_update_with(|_| Some("world".into())).unwrap(),
                "hello"
            );
            assert_eq!(b.get(), Some("world".into()));
        }

        #[test]
        fn test_try_update_with_unset() {
            let a: AtomicOption<String, AtomicAny<String>> = AtomicOption::default();
            assert!(matches!(a.try_update_with(|_| None), Err(MutError::Unsync)));
            let b = a.into_sync();
            assert!(matches!(b.try_update_with(|_| None), Err(MutError::Unset)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paste::paste;

    macro_rules! test_numeric_type {
        ($type:ident) => {
            paste! {
                #[test]
                fn [<test_ $type:snake>]() {
                    let a = $type::from(42);
                    assert_eq!(a.get(), 42);
                    assert_eq!(a.set(44), 42);
                    assert_eq!(a.get(), 44);
                    assert_eq!(a.set_with(|val| Some(val + 1)), 44);
                    assert_eq!(a.get(), 45);
                    assert_eq!(a.set_with(|_| None), 45);
                    assert_eq!(a.get(), 45);
                    assert_eq!(a.add(1), 45);
                    assert_eq!(a.get(), 46);
                    assert_eq!(a.sub(1), 46);
                    assert_eq!(a.get(), 45);
                    let b = a.clone();
                    assert_eq!(b.into_inner(), 45);
                }
            }
        };
    }

    test_numeric_type!(AtomicU32);
    test_numeric_type!(AtomicU64);
    test_numeric_type!(AtomicUsize);

    #[test]
    fn test_atomic_any() {
        let a = AtomicAny::from("hello".to_string());
        assert_eq!(a.get(), "hello");
        assert_eq!(a.set("world".into()), "hello");
        assert_eq!(a.set_with(|val| Some(val.to_uppercase())), "world");
        assert_eq!(a.get(), "WORLD");
        assert_eq!(a.set_with(|_| None), "WORLD");
        assert_eq!(a.get(), "WORLD");
        let b = a.clone();
        assert_eq!(b.into_inner(), "WORLD");
    }

    #[test]
    fn test_atomic_option_default() {
        let mut a: AtomicOption<String, AtomicAny<String>> = AtomicOption::default();
        assert_eq!(a.get(), None);
        assert_eq!(a.set(Some("hello".into())), None);
        assert_eq!(a.get(), Some("hello".into()));
        assert_eq!(a.set(None), Some("hello".into()));
    }

    #[test]
    fn test_atomic_option_new() {
        let mut a: AtomicOption<String, AtomicAny<String>> =
            AtomicOption::Unsync(Some("hello".into()));
        assert_eq!(a.get(), Some("hello".into()));
        assert_eq!(a.set(Some("world".into())), Some("hello".into()));
        assert_eq!(a.get(), Some("world".into()));
        assert_eq!(a.set(None), Some("world".into()));
    }

    #[test]
    fn test_atomic_option_none_into_sync() {
        let a: AtomicOption<String, AtomicAny<String>> = AtomicOption::default();
        let mut b = a.into_sync();
        assert_eq!(b.get(), None);
        assert_eq!(b.get_or_default(), String::default());
        assert_eq!(b.set(Some("hello".into())), None);
        assert_eq!(b.get(), Some("hello".into()));
    }

    #[test]
    fn test_atomic_option_none_into_sync_default() {
        let a: AtomicOption<String, AtomicAny<String>> = AtomicOption::default();
        let mut b = a.into_sync_default();
        assert_eq!(b.get(), Some(String::default()));
        assert_eq!(b.set(Some("hello".into())), Some(String::default()));
        assert_eq!(b.get(), Some("hello".into()));
        let mut c = b.clone();
        let d = b.into_sync_default();
        assert_eq!(c, d);
        assert_eq!(c.set(None), Some("hello".into()));
        assert_eq!(c.get(), None);
    }

    #[test]
    fn test_atomic_option_sync_into_unsync() {
        let a: AtomicOption<String, AtomicAny<String>> = AtomicOption::Unsync(Some("hello".into()));
        assert_eq!(a.get(), Some("hello".into()));
        let b = a.into_sync();
        assert_eq!(b.get(), Some("hello".into()));
        let c = b.clone();
        let d = b.into_sync();
        assert_eq!(c, d);
        let e = d.into_unsync();
        assert_eq!(e.get(), Some("hello".into()));
        let f = e.clone();
        let g = e.into_unsync();
        assert_eq!(f, g);
    }

    #[test]
    fn test_atomic_option_numeric() {
        let mut a: AtomicOption<u32, AtomicU32> = AtomicOption::default();
        assert_eq!(a.get(), None);
        assert_eq!(a.set(Some(42)), None);
        assert_eq!(a.get(), Some(42));
        assert_eq!(a.set(None), Some(42));
        assert_eq!(a.get(), None);
    }

    #[test]
    fn test_atomic_option_numeric_ops() {
        let a: AtomicOption<u32, AtomicU32> = AtomicOption::Unsync(Some(42));
        let b = a.into_sync();
        assert!(matches!(b.add(1), Ok(42)));
        assert_eq!(b.get(), Some(43));
        assert!(matches!(b.set_max(44), Ok(43)));
        assert_eq!(b.get(), Some(44));
    }

    #[test]
    fn test_atomic_option_unsync() {
        let a: AtomicOption<u32, AtomicU32> = AtomicOption::default();
        assert!(matches!(a.add(1), Err(MutError::Unsync)));
        assert!(matches!(a.set_max(1), Err(MutError::Unsync)));
    }

    #[test]
    fn test_atomic_option_unset() {
        let a: AtomicOption<u32, AtomicU32> = AtomicOption::default();
        let b = a.into_sync();
        assert!(matches!(b.add(1), Err(MutError::Unset)));
        assert!(matches!(b.set_max(1), Err(MutError::Unset)));
    }
}
