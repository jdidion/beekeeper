//! Weighted value used for task submission with the `local-batch` feature.
use std::ops::Deref;

/// Wraps a value of type `T` and an associated weight.
pub struct Weighted<T> {
    value: T,
    weight: u32,
}

impl<T> Weighted<T> {
    /// Creates a new `Weighted` instance with the given value and weight.
    pub fn new(value: T, weight: u32) -> Self {
        Self { value, weight }
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
        T: Into<u32> + Clone,
    {
        let weight = value.clone().into();
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

impl<T> From<(T, u32)> for Weighted<T> {
    fn from((value, weight): (T, u32)) -> Self {
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
        Self::Item: Into<u32> + Clone,
    {
        self.into_iter()
            .map(move |item| Weighted::from_identity(item))
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
}

impl<T: IntoIterator> WeightedIteratorExt for T {}

/// Extends `IntoIterator` to add methods to convert any iterator into an iterator over `Weighted`
/// items.
pub trait WeightedExactSizeIteratorExt: IntoIterator + Sized {
    fn into_weighted<T>(self) -> impl ExactSizeIterator<Item = Weighted<T>>
    where
        Self: IntoIterator<Item = (T, u32)>,
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter()
            .map(|(value, weight)| Weighted::new(value, weight))
    }

    fn into_default_weighted(self) -> impl ExactSizeIterator<Item = Weighted<Self::Item>>
    where
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter().map(Into::into)
    }

    fn into_const_weighted(self, weight: u32) -> impl ExactSizeIterator<Item = Weighted<Self::Item>>
    where
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter()
            .map(move |item| Weighted::new(item, weight))
    }

    fn into_identity_weighted(self) -> impl ExactSizeIterator<Item = Weighted<Self::Item>>
    where
        Self::Item: Into<u32> + Clone,
        Self::IntoIter: ExactSizeIterator + 'static,
    {
        self.into_iter()
            .map(move |item| Weighted::from_identity(item))
    }
}

impl<T> WeightedExactSizeIteratorExt for T
where
    T: IntoIterator,
    T::IntoIter: ExactSizeIterator,
{
}
