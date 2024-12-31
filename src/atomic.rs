use paste::paste;
use std::fmt::Debug;
use std::ops::Add;
use std::sync::atomic::Ordering;

/// Trait for wrappers of `std::sync::atomic` types that provides a common API.
pub trait Atomic<T: Clone + Debug + Default>: Clone + Debug + Default + From<T> + Sync {
    /// Returns the current value of this `Atomic` using `Acquire` ordering.
    fn get(&self) -> T;

    /// Sets the value of this `Atomic` using `Release` ordering.
    fn set(&self, value: T) -> T;

    /// If the current value of this `Atomic` is `current`, sets it to `new` using `AcqRel`
    /// ordering and returns the previous value. Otherwise, returns the current value using
    /// `Acquire` ordering.
    fn set_when(&self, current: T, new: T) -> T;

    /// Loads the current value of this `Atomic` using `AcqRel` ordering and calls `f`. If `f`
    /// returns `Some`, this atomic is updated with the new value using `Release` ordering and the
    /// previous value is returned. Otherwise the current value is returned.
    fn set_with<F: FnMut(T) -> Option<T>>(&self, f: F) -> T;

    /// Consumes this `Atomic` and returns the inner value.
    fn into_inner(self) -> T;
}

macro_rules! atomic {
    ($type:ident) => {
        paste! {
            #[derive(Debug, Default)]
            pub struct [<Atomic $type:camel>](std::sync::atomic::[<Atomic $type:camel>]);

            impl Atomic<$type> for [<Atomic $type:camel>] {
                fn get(&self) -> $type {
                    self.0.load(Ordering::Acquire)
                }

                fn set(&self, value: $type) -> $type {
                    self.0.swap(value, Ordering::Release)
                }

                fn set_when(&self, current: $type, new: $type) -> $type {
                    match self.0.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(prev) | Err(prev) => prev,
                    }
                }

                fn set_with<F: FnMut($type) -> Option<$type>>(&self, f: F) -> $type {
                    match self.0.fetch_update(Ordering::AcqRel, Ordering::Acquire, f) {
                        Ok(prev) | Err(prev) => prev,
                    }
                }

                fn into_inner(self) -> $type {
                    self.0.into_inner()
                }

            }

            impl Clone for [<Atomic $type:camel>] {
                fn clone(&self) -> Self {
                    Self(self.get().into())
                }
            }

            impl From<$type> for [<Atomic $type:camel>] {
                fn from(value: $type) -> Self {
                    [<Atomic $type:camel>](std::sync::atomic::[<Atomic $type:camel>]::new(value))
                }
            }
        }
    };
}

/// Trait for wrappers of `std::sync::atomic` numeric types that provides a common API.
pub trait AtomicNumber<T: Clone + Debug + Default>: Atomic<T> {
    /// Mutably adds `rhs` to the current value of this `Atomic` using `AcqRel` ordering and
    /// returns the previous value.
    fn add(&self, rhs: T) -> T;

    /// Mutably subtracts `rhs` from the current value of this `Atomic` using `AcqRel` ordering and
    /// returns the previous value.
    fn sub(&self, rhs: T) -> T;
}

/// Generate atomic type wrappers that implement the `Atomic` and `AtomicNumber` traits.
macro_rules! atomic_number {
    ($type:ident) => {
        paste! {
            atomic!($type);

            impl AtomicNumber<$type> for [<Atomic $type:camel>] {
                fn add(&self, value: $type) -> $type {
                    self.0.fetch_add(value, Ordering::AcqRel)
                }

                fn sub(&self, value: $type) -> $type {
                    self.0.fetch_sub(value, Ordering::AcqRel)
                }
            }
        }
    };
}

atomic!(bool);
atomic_number!(u32);
atomic_number!(u64);
atomic_number!(usize);

/// Wrapper for `parking_lot::RwLock` that implements the `Atomic` trait. This enables any type
/// that is `Clone + Default` to be used in an `Atomic` context.
#[derive(Debug, Default)]
pub struct AtomicAny<T: Clone + Debug + Default + Sync + Send + PartialEq>(parking_lot::RwLock<T>);

impl<T: Clone + Debug + Default + Sync + Send + PartialEq> Clone for AtomicAny<T> {
    fn clone(&self) -> Self {
        Self(parking_lot::RwLock::new(self.0.read().clone()))
    }
}

impl<T: Clone + Debug + Default + Sync + Send + PartialEq> From<T> for AtomicAny<T> {
    fn from(value: T) -> Self {
        AtomicAny(parking_lot::RwLock::new(value))
    }
}

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

    fn set_when(&self, current: T, new: T) -> T {
        let mut val = self.0.write();
        if *val == current {
            *val = new;
            current
        } else {
            val.clone()
        }
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

/// A wrapper around an `Option<P>` with different behavior in single- and multi-threaded contexts:
///
/// * The `Unsync` variant wraps `Option<P>`. It is intended to be used in a single-threaded
///   context, where the value can be set only via regular mutability (using the `set` method).
///   Calling `update` on an `Unsync` will always panic, and calling `try_update` will return an
///   `Err`.
/// * The `Sync` variant wraps `Option<Atomic<P>>`. It is intended to be used in a multi-threaded
///   context, where the value can be set either by regular or interior mutability (using the
///   `update` or `try_update` method).
#[derive(Clone, Debug)]
pub enum AtomicOption<P, A>
where
    P: Clone + Debug + Default,
    A: Atomic<P>,
{
    Unsync(Option<P>),
    Sync(Option<A>),
}

#[derive(Debug, thiserror::Error)]
pub enum MutError {
    #[error("cannot use interior mutability to modify value in Unsync variant")]
    Unsync,
    #[error("cannot use interior mutability to modify value in an unset Sync variant")]
    Unset,
}

impl<P, A> AtomicOption<P, A>
where
    P: Clone + Debug + Default,
    A: Atomic<P>,
{
    // Returns `true` if this is a `Sync` variant.
    // pub fn is_sync(&self) -> bool {
    //     matches!(self, Self::Sync(_))
    // }

    // Returns `true` if the value is set.
    // pub fn is_set(&self) -> bool {
    //     match self {
    //         Self::Unsync(opt) => opt.is_some(),
    //         Self::Sync(opt) => opt.is_some(),
    //     }
    // }

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

    /// Sets the value to `value` using regular mutability.
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

    // Sets the value to `value` using interior mutability if possible, otherwise panics. This
    // method only succeeds when called on `Sync(Some(_))`.
    // pub fn update(&self, value: P) -> P {
    //     match self {
    //         Self::Unsync(_) => panic!("Cannot call `update` on an `Unsync` variant"),
    //         Self::Sync(None) => panic!("Cannot call `update` on an `Sync` variant with no value"),
    //         Self::Sync(Some(atomic)) => atomic.set(value),
    //     }
    // }

    // Sets the value to `value` using interior mutability if possible, otherwise returns a
    // `MutError`. This method only returns `Ok` when called on `Sync(Some(_))`.
    // pub fn try_update(&self, value: P) -> Result<P, MutError> {
    //     match self {
    //         Self::Unsync(_) => Err(MutError::Unsync),
    //         Self::Sync(None) => Err(MutError::Unset),
    //         Self::Sync(Some(atomic)) => Ok(atomic.set(value)),
    //     }
    // }

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

    /// If this is an `Unsync` variant, consumes `self` and returns the corresponding `Sync`
    /// variant. Otherwise returns `self`.
    pub fn into_sync(self) -> Self {
        if let Self::Unsync(opt) = self {
            Self::Sync(opt.map(A::from))
        } else {
            self
        }
    }

    // If this is a `Sync` variant, consumes `self` and returns the corresponding `Unsync` variant
    // by cloning the current value of the wrapped `Atomic`. Otherwise returns `self`.
    // pub fn clone_into_unsync(self) -> Self {
    //     if let Self::Sync(opt) = self {
    //         Self::Unsync(opt.map(|atomic| atomic.get()))
    //     } else {
    //         self
    //     }
    // }

    /// If this is a `Sync` variant, consumes `self` and returns the corresponding `Unsync` variant
    /// by consuming the wrapped `Atomic` and `take`ing its current value (which may panic if the
    /// inner value cannot be `take`n). Otherwise returns `self`.
    pub fn take_into_unsync(self) -> Self {
        if let Self::Sync(opt) = self {
            Self::Unsync(opt.map(|atomic| atomic.into_inner()))
        } else {
            self
        }
    }
}

impl<P, A> AtomicOption<P, A>
where
    P: Copy + Debug + Default + Add<Output = P> + PartialOrd<P>,
    A: AtomicNumber<P>,
{
    /// Updates the value to be the sum of `rhs` and either the current value if it is set or the
    /// default value if it is not set.
    // pub fn add(&mut self, rhs: P) -> P {
    //     match self {
    //         Self::Unsync(opt @ None) => {
    //             let cur = P::default();
    //             let lhs = opt.insert(cur.clone());
    //             *lhs = lhs.clone() + rhs;
    //             cur
    //         }
    //         Self::Sync(opt @ None) => {
    //             let cur = P::default();
    //             let _ = opt.insert(cur.clone().into()).add(rhs);
    //             cur
    //         }
    //         Self::Unsync(Some(lhs)) => {
    //             let cur = lhs.clone();
    //             *lhs = cur.clone() + rhs;
    //             cur
    //         }
    //         Self::Sync(Some(lhs)) => lhs.add(rhs),
    //     }
    // }

    /// If this is a `Sync` variant whose value is `Some`, updates the value to be the sum of
    /// the current value and `value` and returns the previous value. Otherwise panics.
    pub fn add_update(&self, rhs: P) -> P {
        match self {
            Self::Unsync(_) => panic!("Cannot call `add_update` on an `Unsync` variant"),
            Self::Sync(None) => {
                panic!("Cannot call `add_update` on an `Sync` variant with no value")
            }
            Self::Sync(Some(atomic)) => atomic.add(rhs),
        }
    }

    /// Adds the value to the current value using interior mutability if possible and returns the
    /// previous value, otherwise returns a `MutError` and . This method only returns `Ok` when
    /// called on `Sync(Some(_))`.
    // pub fn try_add_update(&self, rhs: P) -> Result<P, MutError> {
    //     match self {
    //         Self::Unsync(_) => Err(MutError::Unsync),
    //         Self::Sync(None) => Err(MutError::Unset),
    //         Self::Sync(Some(atomic)) => Ok(atomic.add(rhs)),
    //     }
    // }

    /// If this is a `Sync` variant whose value is `Some`, sets the value to the maximum of the
    /// current value and `rhs` if it is set and returns the previous value. Otherwise panics.
    pub fn set_max(&self, rhs: P) -> P {
        match self {
            Self::Unsync(_) => panic!("Cannot call `set_max` on an `Unsync` variant"),
            Self::Sync(None) => panic!("Cannot call `set_max` on an `Sync` variant with no value"),
            Self::Sync(Some(atomic)) => {
                atomic.set_with(move |current| (current < rhs).then_some(rhs))
            }
        }
    }

    // Sets the value to the maximum of the current value and `rhs` using interior mutability if
    // possible and returns the previous value, otherwise otherwise returns a `MutError`.
    // pub fn try_set_max(&self, rhs: P) -> Result<P, MutError> {
    //     match self {
    //         Self::Unsync(_) => Err(MutError::Unsync),
    //         Self::Sync(None) => Err(MutError::Unset),
    //         Self::Sync(Some(atomic)) => {
    //             Ok(atomic.set_with(move |current| (current < rhs).then_some(rhs)))
    //         }
    //     }
    // }
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
