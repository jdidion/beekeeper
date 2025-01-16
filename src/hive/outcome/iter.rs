use super::Outcome;
use crate::bee::Worker;
use std::collections::{HashMap, HashSet, VecDeque};

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
    indices: VecDeque<usize>,
    buf: HashMap<usize, Outcome<W>>,
}

impl<W: Worker> OutcomeIterator<W> {
    /// Creates a new `OutcomeIterator` that will return outcomes from the given iterator in the
    /// index order specified in `indices`. Items are buffered until the next index is available.
    /// This iterator continues until the limit is reached or the underlying iterator is exhausted
    /// and the next index is not in the buffer.
    pub fn new<T>(inner: T, indices: Vec<usize>) -> Self
    where
        T: IntoIterator<Item = Outcome<W>>,
        T::IntoIter: 'static,
    {
        Self {
            inner: Box::new(inner.into_iter().take(indices.len())),
            buf: HashMap::with_capacity(indices.len()),
            indices: indices.into(),
        }
    }
}

impl<W: Worker> Iterator for OutcomeIterator<W> {
    type Item = Result<Outcome<W>, usize>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(next) = self.indices.front() {
                if let Some(outcome) = self.buf.remove(next).or_else(|| self.inner.next()) {
                    let index = outcome.index();
                    if index == next {
                        self.indices.pop_front();
                        return Some(Ok(outcome));
                    } else {
                        if self.indices.contains(index) {
                            self.buf.insert(*index, outcome);
                        }
                        continue;
                    }
                } else {
                    return Some(Err(*next));
                }
            }
            return None;
        }
    }
}

pub trait OutcomeIteratorExt<W: Worker>: IntoIterator<Item = Outcome<W>> + Sized {
    /// Consumes this iterator and returns an ordered iterator over a maximum of `n` `TaskResult`s.
    /// Each item in the iterator is either an `Ok(Outcome)` or an `Err(index)` of a task that was
    /// not processed (e.g., because the hive was dropped or poisoned).
    fn take_ordered(self, indices: Vec<usize>) -> impl Iterator<Item = Result<Outcome<W>, usize>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self, indices)
    }

    /// Consumes this iterator and returns an unordered iterator over `TaskResult`s.
    ///
    /// This method panics if any of the outcomes represent unprocessed or panicked tasks.
    fn into_results(self) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::into)
    }

    /// Consumes this iterator and returns an unordered iterator over a maximum of `n`
    /// `TaskResult`s.
    ///
    /// This method panics if any of the outcomes represent unprocessed or panicked tasks.
    fn take_results(self, indices: Vec<usize>) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        let indices: HashSet<usize> = indices.into_iter().collect();
        let n = indices.len();
        self.into_iter()
            .filter_map(move |outcome| indices.contains(outcome.index()).then_some(outcome))
            .map(Outcome::into)
            .take(n)
    }

    /// Consumes this iterator and returns an ordered iterator over a maximum of `n` `TaskResult`s.
    ///
    /// This method panics if any of the outcomes represent unprocessed or panicked tasks.
    fn take_ordered_results(self, indices: Vec<usize>) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self, indices).map(|result| match result {
            Ok(outcome) => outcome.into(),
            Err(index) => panic!("Task was not processed: {}", index),
        })
    }

    /// Consumes this iterator and returns an unordered iterator over `TaskResult`s.
    ///
    /// This method panics if any of the outcomes represent failed, unprocessed, or panicked tasks.
    fn into_outputs(self) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::unwrap)
    }

    /// Consumes this iterator and returns an unordered iterator over a maximum of `n`
    /// output values.
    ///
    /// This method panics if any of the outcomes represent failed, unprocessed, or panicked tasks.
    fn take_outputs(self, indices: Vec<usize>) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        let indices: HashSet<usize> = indices.into_iter().collect();
        let n = indices.len();
        self.into_iter()
            .filter_map(move |outcome| indices.contains(outcome.index()).then_some(outcome))
            .map(Outcome::unwrap)
            .take(n)
    }

    /// Consumes this iterator and returns an ordered iterator over a maximum of `n` output values.
    ///
    /// This method panics if any of the outcomes represent failed, unprocessed, or panicked tasks.
    fn take_ordered_outputs(self, indices: Vec<usize>) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self, indices).map(|result| match result {
            Ok(outcome) => outcome.unwrap(),
            Err(index) => panic!("Task was not processed: {}", index),
        })
    }
}

impl<W: Worker, T: IntoIterator<Item = Outcome<W>>> OutcomeIteratorExt<W> for T {}
