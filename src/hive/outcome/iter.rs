use super::Outcome;
use crate::task::Worker;
use std::collections::HashMap;

pub type TaskResult<W> = Result<<W as Worker>::Output, <W as Worker>::Error>;

/// Consumes this `Outcome` and depending on the variant:
/// * Returns `Ok(W::Input)` if this is a `Success` outcome,
/// * Returns `Err(W::Error)` if this is a `Failure` or `MaxRetriesAttempted` outcome,
/// * Panics if this is an `Unprocessed` outcome
/// * Resumes unwinding if this is a `Panic` outcome
impl<W: Worker> From<Outcome<W>> for TaskResult<W> {
    fn from(value: Outcome<W>) -> TaskResult<W> {
        if let Outcome::Success { value, .. } = value {
            Ok(value)
        } else {
            Err(value.into_error())
        }
    }
}

/// An iterator that returns outcomes in `index` order.
pub struct OutcomeIterator<W: Worker> {
    inner: Box<dyn Iterator<Item = Outcome<W>>>,
    buf: HashMap<usize, Outcome<W>>,
    next: usize,
}

impl<W: Worker> OutcomeIterator<W> {
    /// Creates a new `OutcomeIteator` that will return ordered outcomes from the given iterator.
    /// Items are buffered until the next index is available. This iterator continues until the
    /// underlying iterator is exhausted and the next index is not in the buffer.
    pub fn new<T>(inner: T) -> Self
    where
        T: IntoIterator<Item = Outcome<W>>,
        T::IntoIter: 'static,
    {
        Self {
            inner: Box::new(inner.into_iter()),
            buf: HashMap::new(),
            next: 0,
        }
    }
}

impl<W: Worker> Iterator for OutcomeIterator<W> {
    type Item = Outcome<W>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self
                .buf
                .remove(&self.next)
                .or_else(|| self.inner.next())
                .map(|outcome| {
                    let index = outcome.index();
                    debug_assert!(index >= self.next, "duplicate result index");
                    if index == self.next {
                        Some(outcome)
                    } else {
                        self.buf.insert(index, outcome);
                        None
                    }
                }) {
                None => return None,
                Some(Some(outcome)) => {
                    self.next += 1;
                    return Some(outcome);
                }
                _ => (),
            }
        }
    }
}

pub trait OutcomeIteratorExt<W: Worker>: IntoIterator<Item = Outcome<W>> + Sized {
    fn into_ordered(self) -> impl Iterator<Item = Outcome<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self)
    }

    /// Consumes this iterator and returns an unordered iterator over `TaskResult`s.
    fn into_results(self) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::into)
    }

    /// Consumes this iterator and returns an ordered iterator over `TaskResult`s.
    fn into_ordered_results(self) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self).map(Outcome::into)
    }

    /// Consumes this iterator and returns an unordered iterator over `TaskResult`s.
    fn into_outputs(self) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::unwrap)
    }

    /// Consumes this iterator and returns an ordered iterator over output values.
    fn into_ordered_outputs(self) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self).map(Outcome::unwrap)
    }
}

impl<W: Worker, T: IntoIterator<Item = Outcome<W>>> OutcomeIteratorExt<W> for T {}
