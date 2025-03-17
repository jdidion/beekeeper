//! Weighted value used for task submission with the `local-batch` feature.
use num::ToPrimitive;
use std::ops::Deref;

/// Wraps a value of type `T` and an associated weight.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Weighted<T> {
    value: T,
    weight: u32,
}

impl<T> Weighted<T> {
    /// Creates a new `Weighted` instance with the given value and weight.
    pub fn new<P: ToPrimitive>(value: T, weight: P) -> Self {
        Self {
            value,
            weight: weight.to_u32().unwrap(),
        }
    }

    /// Creates a new `Weighted` instance with the given value and weight obtained from calling the
    /// given function on `value`.
    pub fn from_fn<F>(value: T, f: F) -> Self
    where
        F: FnOnce(&T) -> u32,
    {
        let weight = f(&value);
        Self::new(value, weight)
    }

    /// Creates a new `Weighted` instance with the given value and weight obtained by converting
    /// the value into a `u32`.
    pub fn from_identity(value: T) -> Self
    where
        T: ToPrimitive + Clone,
    {
        let weight = value.clone().to_u32().unwrap();
        Self::new(value, weight)
    }

    /// Returns the weight associated with this `Weighted` value.
    pub fn weight(&self) -> u32 {
        self.weight
    }

    /// Returns the value and weight as a tuple.
    pub fn into_parts(self) -> (T, u32) {
        (self.value, self.weight)
    }
}

impl<T> Deref for Weighted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> From<T> for Weighted<T> {
    fn from(value: T) -> Self {
        Self::new(value, 0)
    }
}

impl<T, P: ToPrimitive> From<(T, P)> for Weighted<T> {
    fn from((value, weight): (T, P)) -> Self {
        Self::new(value, weight)
    }
}

/// Extends `IntoIterator` to add methods to convert any iterator into an iterator over `Weighted`
/// items.
pub trait WeightedIteratorExt: IntoIterator + Sized {
    fn into_weighted<T>(self) -> impl Iterator<Item = Weighted<T>>
    where
        Self: IntoIterator<Item = (T, u32)>,
    {
        self.into_iter()
            .map(|(value, weight)| Weighted::new(value, weight))
    }

    fn into_default_weighted(self) -> impl Iterator<Item = Weighted<Self::Item>> {
        self.into_iter().map(Into::into)
    }

    fn into_const_weighted(self, weight: u32) -> impl Iterator<Item = Weighted<Self::Item>> {
        self.into_iter()
            .map(move |item| Weighted::new(item, weight))
    }

    fn into_identity_weighted(self) -> impl Iterator<Item = Weighted<Self::Item>>
    where
        Self::Item: ToPrimitive + Clone,
    {
        self.into_iter().map(Weighted::from_identity)
    }

    fn into_weighted_zip<W>(self, weights: W) -> impl Iterator<Item = Weighted<Self::Item>>
    where
        W: IntoIterator<Item = u32>,
        W::IntoIter: 'static,
    {
        self.into_iter()
            .zip(weights.into_iter().chain(std::iter::repeat(0)))
            .map(Into::into)
    }

    fn into_weighted_with<F>(self, f: F) -> impl Iterator<Item = Weighted<Self::Item>>
    where
        F: Fn(&Self::Item) -> u32,
    {
        self.into_iter().map(move |item| {
            let weight = f(&item);
            Weighted::new(item, weight)
        })
    }

    fn into_weighted_exact<T>(self) -> impl ExactSizeIterator<Item = Weighted<T>>
    where
        Self: IntoIterator<Item = (T, u32)>,
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter()
            .map(|(value, weight)| Weighted::new(value, weight))
    }

    fn into_default_weighted_exact(self) -> impl ExactSizeIterator<Item = Weighted<Self::Item>>
    where
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter().map(Into::into)
    }

    fn into_const_weighted_exact(
        self,
        weight: u32,
    ) -> impl ExactSizeIterator<Item = Weighted<Self::Item>>
    where
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter()
            .map(move |item| Weighted::new(item, weight))
    }

    fn into_identity_weighted_exact(self) -> impl ExactSizeIterator<Item = Weighted<Self::Item>>
    where
        Self::Item: ToPrimitive + Clone,
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter().map(Weighted::from_identity)
    }

    fn into_weighted_exact_with<F>(
        self,
        f: F,
    ) -> impl ExactSizeIterator<Item = Weighted<Self::Item>>
    where
        Self::IntoIter: ExactSizeIterator + 'static,
        F: Fn(&Self::Item) -> u32,
    {
        self.into_iter().map(move |item| {
            let weight = f(&item);
            Weighted::new(item, weight)
        })
    }
}

impl<T: IntoIterator> WeightedIteratorExt for T {}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let weighted = Weighted::new(42, 10);
        assert_eq!(*weighted, 42);
        assert_eq!(weighted.weight(), 10);
        assert_eq!(weighted.into_parts(), (42, 10));
    }

    #[test]
    fn test_from_fn() {
        let weighted = Weighted::from_fn(42, |x| x * 2);
        assert_eq!(*weighted, 42);
        assert_eq!(weighted.weight(), 84);
    }

    #[test]
    fn test_from_identity() {
        let weighted = Weighted::from_identity(42);
        assert_eq!(*weighted, 42);
        assert_eq!(weighted.weight(), 42);
    }

    #[test]
    fn test_from_unweighted() {
        let weighted = Weighted::from(42);
        assert_eq!(*weighted, 42);
        assert_eq!(weighted.weight(), 0);
    }

    #[test]
    fn test_from_tuple() {
        let weighted: Weighted<usize> = Weighted::from((42, 10));
        assert_eq!(*weighted, 42);
        assert_eq!(weighted.weight(), 10);
        assert_eq!(weighted.into_parts(), (42, 10));
    }

    #[test]
    fn test_into_weighted() {
        (0..10)
            .map(|i| (i, i))
            .into_weighted()
            .for_each(|weighted| assert_eq!(weighted.weight(), weighted.value));
    }

    #[test]
    fn test_into_default_weighted() {
        (0..10)
            .into_default_weighted()
            .for_each(|weighted| assert_eq!(weighted.weight(), 0));
    }

    #[test]
    fn test_into_identity_weighted() {
        (0..10)
            .into_identity_weighted()
            .for_each(|weighted| assert_eq!(weighted.weight(), weighted.value));
    }

    #[test]
    fn test_into_const_weighted() {
        (0..10)
            .into_const_weighted(5)
            .for_each(|weighted| assert_eq!(weighted.weight(), 5));
    }

    #[test]
    fn test_into_weighted_zip() {
        (0..10)
            .into_weighted_zip(10..20)
            .for_each(|weighted| assert_eq!(weighted.weight(), weighted.value + 10));
    }

    #[test]
    fn test_into_weighted_with() {
        (0..10)
            .into_weighted_with(|i| i * 2)
            .for_each(|weighted| assert_eq!(weighted.weight(), weighted.value * 2));
    }

    #[test]
    fn test_into_weighted_exact() {
        (0..10)
            .map(|i| (i, i))
            .into_weighted_exact()
            .for_each(|weighted| assert_eq!(weighted.weight(), weighted.value));
    }

    #[test]
    fn test_into_default_weighted_exact() {
        (0..10)
            .into_default_weighted_exact()
            .for_each(|weighted| assert_eq!(weighted.weight(), 0));
    }

    #[test]
    fn test_into_identity_weighted_exact() {
        (0..10)
            .into_identity_weighted_exact()
            .for_each(|weighted| assert_eq!(weighted.weight(), weighted.value));
    }

    #[test]
    fn test_into_const_weighted_exact() {
        (0..10)
            .into_const_weighted_exact(5)
            .for_each(|weighted| assert_eq!(weighted.weight(), 5));
    }

    #[test]
    fn test_into_weighted_exact_with() {
        (0..10)
            .into_weighted_exact_with(|i| i * 2)
            .for_each(|weighted| assert_eq!(weighted.weight(), weighted.value * 2));
    }
}
