use super::Outcome;
use crate::bee::{TaskId, Worker};
use std::collections::{BTreeSet, HashMap, VecDeque};

pub type TaskResult<W> = Result<<W as Worker>::Output, <W as Worker>::Error>;

/// Consumes this `Outcome` and depending on the variant:
/// * Returns `Ok(W::Input)` if this is a `Success` outcome,
/// * Returns `Err(W::Error)` if this is a `Failure` or `MaxRetriesAttempted` outcome,
/// * Resumes unwinding if this is a `Panic` outcome,
/// * Panics otherwise (e.g. `Unprocessed`, `Missing`).
impl<W: Worker> From<Outcome<W>> for TaskResult<W> {
    fn from(value: Outcome<W>) -> TaskResult<W> {
        if let Outcome::Success { value, .. } = value {
            Ok(value)
        } else {
            Err(value.try_into_error().expect("not an error outcome"))
        }
    }
}

/// Wraps an outcome iterator and yields the outcomes with specified task IDs in the order they are
/// yielded by the underlying iterator. When the underlying iterator is exhausted, if there are
/// remaining task IDs, they will be yielded as `Missing` outcomes.
pub struct UnorderedOutcomeIterator<W: Worker> {
    inner: Box<dyn Iterator<Item = Outcome<W>>>,
    task_ids: BTreeSet<TaskId>,
}

impl<W: Worker> UnorderedOutcomeIterator<W> {
    /// Creates a new `UnorderedOutcomeIterator` that will yield the outcomes from the given
    /// iterator with the specified `task_ids`.
    pub fn new<T, I: IntoIterator<Item = TaskId>>(inner: T, task_ids: I) -> Self
    where
        T: IntoIterator<Item = Outcome<W>>,
        T::IntoIter: 'static,
    {
        let task_ids: BTreeSet<_> = task_ids.into_iter().collect();
        Self {
            inner: Box::new(inner.into_iter().take(task_ids.len())),
            task_ids: task_ids,
        }
    }
}

impl<W: Worker> Iterator for UnorderedOutcomeIterator<W> {
    type Item = Outcome<W>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Some(outcome) if self.task_ids.remove(outcome.task_id()) => break Some(outcome),
                Some(_) => continue, // drop unrequested outcomes
                None if !self.task_ids.is_empty() => {
                    // convert extra task_ids to Missing outcomes
                    break Some(Outcome::Missing {
                        task_id: self.task_ids.pop_first().unwrap(),
                    });
                }
                None => break None,
            }
        }
    }
}

/// Wraps an outcome iterator and yields the outcomes with specified task IDs in order.
/// Items are buffered until the `Outcome` with the next ID is available. This iterator
/// continues until outcomes are yielded for all task IDs or the underlying iterator is exhausted
/// and the next ID is not in the buffer. If there are remaining task IDs, they will be yielded
/// as [`Outcome::Missing`].
pub struct OrderedOutcomeIterator<W: Worker> {
    inner: Box<dyn Iterator<Item = Outcome<W>>>,
    task_ids: VecDeque<TaskId>,
    buf: HashMap<TaskId, Outcome<W>>,
}

impl<W: Worker> OrderedOutcomeIterator<W> {
    /// Creates a new `OrderedOutcomeIterator` that will yield outcomes from the given iterator in
    /// the order specified in `task_ids`.
    pub fn new<T, I: IntoIterator<Item = TaskId>>(inner: T, task_ids: I) -> Self
    where
        T: IntoIterator<Item = Outcome<W>>,
        T::IntoIter: 'static,
    {
        let task_ids: VecDeque<TaskId> = task_ids.into_iter().collect();
        Self {
            inner: Box::new(inner.into_iter().take(task_ids.len())),
            buf: HashMap::with_capacity(task_ids.len()),
            task_ids,
        }
    }
}

impl<W: Worker> Iterator for OrderedOutcomeIterator<W> {
    type Item = Outcome<W>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(next) = self.task_ids.front() {
                // if the outcome with the next ID is buffered, remove it from the buffer,
                // otherwise take the next outcome from the underlying iterator
                if let Some(outcome) = self.buf.remove(next).or_else(|| self.inner.next()) {
                    let task_id = outcome.task_id();
                    if task_id == next {
                        // this is the next outcome expected
                        self.task_ids.pop_front();
                        return Some(outcome);
                    } else {
                        // this is an outcome for a future or unspecified ID
                        self.buf.insert(*task_id, outcome);
                        continue;
                    }
                } else {
                    // the underlying iterator is exhausted and we still have unsatisfied task_ids;
                    // convert them to `Missing` outcomes
                    return Some(Outcome::Missing { task_id: *next });
                }
            }
            // drop outcomes for task_ids that were not requested
            //if !self.buf.is_empty() { .. }
            return None;
        }
    }
}

pub trait OutcomeIteratorExt<W: Worker>: IntoIterator<Item = Outcome<W>> + Sized {
    /// Consumes this iterator and returns an unordered iterator over the `Outcome`s with the
    /// specified `task_ids`.
    ///
    /// `Outcome`s yielded by the iterator whose task IDs are not in `task_ids` are silently
    /// dropped. Any remaining task IDs after the iterator is exhausted are yielded as `Missing` outcomes.
    fn select_unordered<I>(self, task_ids: I) -> impl Iterator<Item = Outcome<W>>
    where
        I: IntoIterator<Item = TaskId>,
        <Self as IntoIterator>::IntoIter: 'static,
    {
        UnorderedOutcomeIterator::new(self, task_ids)
    }

    /// Consumes this iterator and returns an ordered iterator over the `Outcome`s with the
    /// specified `task_ids`.
    ///
    /// `Outcome`s yielded by the iterator whose task IDs are not in `task_ids` are silently
    /// dropped. Any remaining task IDs after the iterator is exhausted are yielded as `Missing`
    /// outcomes.
    fn select_ordered<I>(self, task_ids: I) -> impl Iterator<Item = Outcome<W>>
    where
        I: IntoIterator<Item = TaskId>,
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OrderedOutcomeIterator::new(self, task_ids)
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

    /// Consumes this iterator and returns an unordered iterator over the `Result`s with the
    /// specified `task_ids`.
    ///
    /// `Outcome`s yielded by the iterator whose task IDs are not in `task_ids` are silently
    /// dropped. This method panics if any of the outcomes represent unprocessed, missing, or
    /// panicked tasks.
    fn select_unordered_results<I>(self, task_ids: I) -> impl Iterator<Item = TaskResult<W>>
    where
        I: IntoIterator<Item = TaskId>,
        <Self as IntoIterator>::IntoIter: 'static,
    {
        UnorderedOutcomeIterator::new(self, task_ids).map(Outcome::into)
    }

    /// Consumes this iterator and returns an ordered iterator over the `Result`s with the
    /// specified `task_ids`.
    ///
    /// `Outcome`s yielded by the iterator whose task IDs are not in `task_id` are silently
    /// dropped. This method panics if any of the outcomes represent unprocessed or panicked tasks.
    fn select_ordered_results<I>(self, task_ids: I) -> impl Iterator<Item = TaskResult<W>>
    where
        I: IntoIterator<Item = TaskId>,
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OrderedOutcomeIterator::new(self, task_ids).map(Outcome::into)
    }

    /// Consumes this iterator and returns an unordered iterator over task outputs.
    ///
    /// This method panics if any of the outcomes represent failed, unprocessed, or panicked tasks.
    fn into_outputs(self) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::unwrap)
    }

    /// Consumes this iterator and returns an unordered iterator over the outputs with the
    /// specified `task_ids`.
    ///
    /// `Outcome`s yielded by the iterator whose task IDs are not in `task_ids` are silently
    /// dropped. This method panics if any of the outcomes represent failed, unprocessed, or
    /// panicked tasks.
    fn select_unordered_outputs<I>(self, task_ids: I) -> impl Iterator<Item = W::Output>
    where
        I: IntoIterator<Item = TaskId>,
        <Self as IntoIterator>::IntoIter: 'static,
    {
        UnorderedOutcomeIterator::new(self, task_ids).map(Outcome::unwrap)
    }

    /// Consumes this iterator and returns an ordered iterator over the outputs with the
    /// specified `task_ids`.
    ///
    /// `Outcome`s yielded by the iterator whose task IDs are not in `task_ids` are silently
    /// dropped. This method panics if any of the outcomes represent failed, unprocessed, or
    /// panicked tasks.
    fn select_ordered_outputs<I>(self, task_ids: I) -> impl Iterator<Item = W::Output>
    where
        I: IntoIterator<Item = TaskId>,
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OrderedOutcomeIterator::new(self, task_ids).map(Outcome::unwrap)
    }
}

impl<W: Worker, T: IntoIterator<Item = Outcome<W>>> OutcomeIteratorExt<W> for T {}
