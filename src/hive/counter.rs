use crate::atomic::{Atomic, AtomicInt, AtomicU64, Ordering, Orderings};

// TODO: it's not clear if SeqCst ordering is actually necessary - need to do some fuzz testing.
const SEQCST_ORDERING: Orderings = Orderings {
    load: Ordering::SeqCst,
    swap: Ordering::SeqCst,
    fetch_update_set: Ordering::SeqCst,
    fetch_update_fetch: Ordering::SeqCst,
    fetch_add: Ordering::SeqCst,
    fetch_sub: Ordering::SeqCst,
};

#[derive(thiserror::Error, Debug)]
pub enum CounterError {
    #[error("Left counter overflow")]
    LeftOverflow,
    #[error("Right counter overflow")]
    RightOverflow,
    #[error("Left counter underflow")]
    LeftUnderflow,
    #[error("Right counter underflow")]
    RightUnderflow,
}

/// A counter that can keep track of two related values (`L` and `R`) using a single atomic number.
/// The two values may be different sizes, but their total size in bits must equal the size of the
/// data type (for now fixed to `64`) used to store the value.
///
/// Three operations are supported:
/// * increment the left counter (`L`)
/// * decrement the right counter (`R`)
/// * transfer an amount `N` from `L` to `R` (i.e., a simultaneous decrement of `L` and
///   increment of `R` by the same amount)
pub struct DualCounter<const L: u32>(AtomicU64);

impl<const L: u32> DualCounter<L> {
    // validate that L is > 0
    const L_BITS: u32 = L.checked_sub(1).expect("L must be > 0") as u32 + 1;
    // validate that L is < 64
    const R_BITS: u32 = 63u32.checked_sub(Self::L_BITS).expect("L must be <= 63") + 1;
    // compute the maximum possible values for L and R
    const L_MAX: u64 = (1 << Self::L_BITS) - 1;
    const R_MAX: u64 = (1 << Self::R_BITS) - 1;

    /// Decomposes a 64-bit value into its left and right parts.
    #[inline]
    fn decompose(n: u64) -> (u64, u64) {
        (n & Self::L_MAX, n >> Self::L_BITS)
    }

    /// Returns a tuple with the (left, right) parts of the counter.
    pub fn get(&self) -> (u64, u64) {
        Self::decompose(self.0.get())
    }

    // pub fn reset(&self) -> (u64, u64) {
    //     Self::decompose(self.0.set(0))
    // }

    /// Increments the left counter by `n` and returns the previous value.
    ///
    /// Returns an error if `n` is greater than the maximum value (2^L - 1) or if the left counter
    /// overflows when incremented by `n`.
    pub fn increment_left(&self, n: u64) -> Result<u64, CounterError> {
        if n > Self::L_MAX {
            return Err(CounterError::LeftOverflow);
        }
        let prev_val = self.0.add(n) & Self::L_MAX;
        match prev_val.checked_add(n) {
            Some(new_val) if new_val <= Self::L_MAX => Ok(prev_val),
            Some(_) => Err(CounterError::LeftOverflow),
            None => unreachable!("counter overflow"),
        }
    }

    /// Decrements the right counter by `n` and returns the previous value.
    ///
    /// Returns an error  if `n` is greater than the maximum value (2^(64-L) - 1) or if the right
    /// counter underflows when decremented by `n`.
    pub fn decrement_right(&self, n: u64) -> Result<u64, CounterError> {
        if n > Self::R_MAX {
            return Err(CounterError::RightUnderflow);
        }
        let n_shifted = n.checked_shl(Self::L_BITS).unwrap();
        let prev_val = self.0.sub(n_shifted) >> Self::L_BITS;
        if prev_val >= n {
            Ok(prev_val)
        } else {
            Err(CounterError::RightUnderflow)
        }
    }

    /// Atomically decrements the left counter and increments the right counter by `n`, and returns
    /// the previous values of the counters.
    ///
    /// Returns an error if `n` is greater than the maximum value for either the right or left
    /// counter, if the left counter overflows when incremented, or if the right counter underflows
    /// when decremented.
    pub fn transfer(&self, n: u64) -> Result<(u64, u64), CounterError> {
        if n > Self::L_MAX {
            return Err(CounterError::LeftUnderflow);
        }
        if n > Self::R_MAX {
            return Err(CounterError::RightOverflow);
        }
        let delta = n.checked_shl(Self::L_BITS).unwrap().checked_sub(n).unwrap();
        let (prev_left, prev_right) = Self::decompose(self.0.add(delta));
        if prev_left < n {
            Err(CounterError::LeftUnderflow)
        } else {
            match prev_right.checked_add(n) {
                Some(new_val) if new_val <= Self::R_MAX => Ok((prev_left, prev_right)),
                Some(_) => Err(CounterError::RightOverflow),
                None => unreachable!("counter overflow"),
            }
        }
    }
}

impl<const L: u32> Default for DualCounter<L> {
    fn default() -> Self {
        DualCounter(AtomicU64::with_orderings(0, SEQCST_ORDERING))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_works() {
        let counter = DualCounter::<48>::default();

        assert_eq!(counter.increment_left(3).unwrap(), 0);
        assert_eq!(counter.increment_left(1).unwrap(), 3);
        assert_eq!(counter.get(), (4, 0));

        assert_eq!(counter.transfer(3).unwrap(), (4, 0));
        assert_eq!(counter.get(), (1, 3));

        assert_eq!(counter.decrement_right(2).unwrap(), 3);
        assert_eq!(counter.decrement_right(1).unwrap(), 1);
        assert_eq!(counter.get(), (1, 0));
    }

    #[test]
    fn test_increment_too_large() {
        let counter = DualCounter::<1>::default();
        assert!(matches!(
            counter.increment_left(2),
            Err(CounterError::LeftOverflow)
        ));
    }

    #[test]
    fn test_increment_overflow() {
        let counter = DualCounter::<48>::default();
        assert!(counter.increment_left(DualCounter::<48>::L_MAX).is_ok());
        assert!(matches!(
            counter.increment_left(DualCounter::<48>::L_MAX),
            Err(CounterError::LeftOverflow)
        ));
    }

    #[test]
    fn test_counter_overflow() {
        let counter = DualCounter::<63>::default();
        assert!(counter.increment_left(DualCounter::<63>::L_MAX).is_ok());
        assert!(matches!(
            counter.increment_left(DualCounter::<63>::L_MAX),
            Err(CounterError::LeftOverflow)
        ));
    }

    #[test]
    fn test_left_overflow() {
        let counter = DualCounter::<1>::default();
        assert!(counter.increment_left(1).is_ok());
        assert!(matches!(
            counter.increment_left(1),
            Err(CounterError::LeftOverflow)
        ));
    }

    #[test]
    fn test_transfer_overflow() {
        let counter = DualCounter::<63>::default();
        assert!(counter.increment_left(2).is_ok());
        assert!(matches!(
            counter.transfer(2),
            Err(CounterError::RightOverflow)
        ));
    }

    #[test]
    fn test_transfer_left_too_small() {
        let counter = DualCounter::<32>::default();
        assert!(counter.increment_left(2).is_ok());
        assert!(matches!(
            counter.transfer(3),
            Err(CounterError::LeftUnderflow)
        ));
    }

    #[test]
    fn test_transfer_right_too_large() {
        let counter = DualCounter::<32>::default();
        assert!(counter.increment_left(DualCounter::<32>::L_MAX).is_ok());
        assert!(counter.transfer(DualCounter::<32>::L_MAX).is_ok());
        assert!(counter.increment_left(1).is_ok());
        assert!(matches!(
            counter.transfer(1),
            Err(CounterError::RightOverflow)
        ));
    }

    #[test]
    fn test_decrement_too_large() {
        let counter = DualCounter::<63>::default();
        assert!(counter.increment_left(2).is_ok());
        assert!(counter.transfer(1).is_ok());
        assert!(matches!(
            counter.decrement_right(2),
            Err(CounterError::RightUnderflow)
        ));
    }

    #[test]
    fn test_right_underflow() {
        let counter = DualCounter::<63>::default();
        assert!(counter.increment_left(2).is_ok());
        assert!(counter.transfer(1).is_ok());
        assert!(counter.decrement_right(1).is_ok());
        assert!(matches!(
            counter.decrement_right(1),
            Err(CounterError::RightUnderflow)
        ));
    }
}
