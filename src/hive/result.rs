use crate::task::{ApplyError, Worker, WorkerResult};
use crate::Panic;
use std::{cmp::Ordering, collections::HashMap, fmt::Debug, panic};

pub type TaskResult<W> = Result<<W as Worker>::Output, <W as Worker>::Error>;

/// The possible outcomes of a task execution. Each outcome includes the index of the task that
/// produced it.
///
/// Note that `Outcome`s can only be compared or ordered with other `Outcome`s proced by the same
/// `Hive`, because comparison/ordering is completely based on the `index`.
#[derive(Debug)]
pub enum Outcome<W: Worker> {
    /// The task was executed successfully.
    Success { value: W::Output, index: usize },
    /// The task failed with an error that was not retryable.
    Failure {
        input: Option<W::Input>,
        error: W::Error,
        index: usize,
    },
    /// The task failed after retrying the maximum number of times.
    MaxRetriesAttempted {
        input: W::Input,
        error: W::Error,
        index: usize,
    },
    /// The task was not executed before the Hive was closed.
    Unprocessed { input: W::Input, index: usize },
    /// The task panicked. The input value that caused the panic is provided if possible.
    Panic {
        input: Option<W::Input>,
        payload: Panic<String>,
        index: usize,
    },
}

impl<W: Worker> Outcome<W> {
    pub(crate) fn from_worker_result(
        result: WorkerResult<W>,
        index: usize,
        retryable: bool,
    ) -> Self {
        match result {
            Ok(value) => Self::Success { index, value },
            Err(ApplyError::Retryable { input, error }) if retryable => Self::MaxRetriesAttempted {
                input,
                error,
                index,
            },
            Err(ApplyError::Retryable { input, error, .. }) => Self::Failure {
                input: Some(input),
                error,
                index,
            },
            Err(ApplyError::NotRetryable { input, error }) => Self::Failure {
                input,
                error,
                index,
            },
            Err(ApplyError::Cancelled { input }) => Self::Unprocessed { input, index },
            Err(ApplyError::Panic { input, payload }) => Self::Panic {
                input,
                payload,
                index,
            },
        }
    }

    /// Returns `true` if this is a `Success` outcome.
    pub fn is_success(&self) -> bool {
        matches!(self, Outcome::Success { .. })
    }

    /// Returns the index of the task that produced this outcome.
    pub fn index(&self) -> usize {
        match self {
            Outcome::Success { index, .. }
            | Outcome::Failure { index, .. }
            | Outcome::MaxRetriesAttempted { index, .. }
            | Outcome::Unprocessed { index, .. }
            | Outcome::Panic { index, .. } => *index,
        }
    }

    /// Consumes this `Outcome` and returns the value of this `Success` outcome. Panics if this is
    /// not a `Success` outcome.
    pub fn unwrap(self) -> W::Output {
        match self {
            Outcome::Success { value, .. } => value,
            _ => panic!("not a Success outcome"),
        }
    }

    /// Consumes this `Outcome` and returns the wrapped error. Panics if this `Outcome` is not an
    /// error (i.e., it's a `Success` or `Unprocessed` outcome).
    pub fn into_error(self) -> W::Error {
        match self {
            Outcome::Failure { error, .. } | Outcome::MaxRetriesAttempted { error, .. } => error,
            Outcome::Success { .. } => panic!("not an error outcome"),
            Outcome::Unprocessed { .. } => panic!("unprocessed input"),
            Outcome::Panic { payload, .. } => payload.resume(),
        }
    }
}

impl<W: Worker> PartialEq for Outcome<W> {
    fn eq(&self, other: &Self) -> bool {
        self.index() == other.index()
    }
}

impl<W: Worker> Eq for Outcome<W> {}

impl<W: Worker> PartialOrd for Outcome<W> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.index().partial_cmp(&other.index())
    }
}

impl<W: Worker> Ord for Outcome<W> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

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
    limit: Option<usize>,
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
            limit: None,
        }
    }

    /// Creates a new `OutcomeIteator` that will return up to `limit` ordered outcomes from the
    /// given iterator. Items are buffered until the next index is available. This iterator
    /// continues until the limit is reached or the underlying iterator is exhausted and the next
    /// index is not in the buffer.
    pub fn with_limit<T>(inner: T, limit: usize) -> Self
    where
        T: IntoIterator<Item = Outcome<W>>,
        T::IntoIter: 'static,
    {
        Self {
            inner: Box::new(inner.into_iter().take(limit)),
            buf: HashMap::with_capacity(limit),
            next: 0,
            limit: Some(limit),
        }
    }
}

impl<W: Worker> Iterator for OutcomeIterator<W> {
    type Item = Outcome<W>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.limit {
                Some(limit) if self.next >= limit => return None,
                _ => (),
            }
            match self
                .buf
                .remove(&self.next)
                .or_else(|| self.inner.next())
                .map(|outcome| {
                    let index = outcome.index();
                    if index < self.next {
                        panic!("duplicate result index");
                    } else if index == self.next {
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

    /// Consumes this iterator and returns an ordered iterator over a maximum of `n` `TaskResult`s.
    fn take_ordered(self, n: usize) -> impl Iterator<Item = Outcome<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::with_limit(self, n)
    }

    /// Consumes this iterator and returns an unordered iterator over `TaskResult`s.
    fn into_results(self) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::into)
    }

    /// Consumes this iterator and returns an unordered iterator over a maximum of `n`
    /// `TaskResult`s.
    fn take_results(self, n: usize) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::into).take(n)
    }

    /// Consumes this iterator and returns an ordered iterator over `TaskResult`s.
    fn into_ordered_results(self) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self).map(Outcome::into)
    }

    /// Consumes this iterator and returns an ordered iterator over a maximum of `n` `TaskResult`s.
    fn take_ordered_results(self, n: usize) -> impl Iterator<Item = TaskResult<W>>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::with_limit(self, n).map(Outcome::into)
    }

    /// Consumes this iterator and returns an unordered iterator over `TaskResult`s.
    fn into_outputs(self) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::unwrap)
    }

    /// Consumes this iterator and returns an unordered iterator over a maximum of `n`
    /// output values.
    fn take_outputs(self, n: usize) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        self.into_iter().map(Outcome::unwrap).take(n)
    }

    /// Consumes this iterator and returns an ordered iterator over output values.
    fn into_ordered_outputs(self) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::new(self).map(Outcome::unwrap)
    }

    /// Consumes this iterator and returns an ordered iterator over a maximum of `n` output values.
    fn take_ordered_outputs(self, n: usize) -> impl Iterator<Item = W::Output>
    where
        <Self as IntoIterator>::IntoIter: 'static,
    {
        OutcomeIterator::with_limit(self, n).map(Outcome::unwrap)
    }
}

impl<W: Worker, T: IntoIterator<Item = Outcome<W>>> OutcomeIteratorExt<W> for T {}
