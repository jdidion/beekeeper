use super::{DerefOutcomes, Outcome};
use crate::bee::{TaskId, Worker};

/// Traits with methods that should only be accessed internally by public traits.
pub mod sealed {
    use crate::bee::{TaskId, Worker};
    use crate::hive::Outcome;
    use std::{
        collections::HashMap,
        ops::{Deref, DerefMut},
    };

    pub trait DerefOutcomes<W: Worker> {
        /// Returns a read-only reference to a map of task task_id to `Outcome`.
        fn outcomes_deref(&self) -> impl Deref<Target = HashMap<TaskId, Outcome<W>>>;

        /// Returns a mutable reference to a map of task task_id to `Outcome`.
        fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>>;
    }

    pub trait OwnedOutcomes<W: Worker>: Sized {
        /// Returns an owned map of task task_id to `Outcome`.
        fn outcomes(self) -> HashMap<TaskId, Outcome<W>>;

        /// Returns a read-only reference to a map of task task_id to `Outcome`.
        fn outcomes_ref(&self) -> &HashMap<TaskId, Outcome<W>>;
    }
}

/// Trait implemented by structs that store `Outcome`s (`Hive`, `Husk`, and `OutcomeBatch`).
///
/// The first group of methods provided by this trait only require dereferencing the underlying map,
/// while the second group of methods require the ability to borrow or take ownership of the
/// underlying map (and thus, are not in scope for `Hive`).
pub trait OutcomeStore<W: Worker>: sealed::DerefOutcomes<W> {
    fn len(&self) -> usize {
        self.outcomes_deref().len()
    }

    /// Returns `true` if there are no stored outcomes.
    fn is_empty(&self) -> bool {
        self.outcomes_deref().is_empty()
    }

    /// Returns counts of the outcomes as a tuple `(unprocessed, successes, failures)`.
    fn count(&self) -> (usize, usize, usize) {
        self.outcomes_deref().values().fold(
            (0usize, 0usize, 0usize),
            |(unprocessed, successes, failures), result| match result {
                Outcome::Success { .. } => (unprocessed, successes + 1, failures),
                Outcome::Unprocessed { .. } => (unprocessed + 1, successes, failures),
                _ => (unprocessed, successes, failures + 1),
            },
        )
    }

    /// Panics if there are stored outcomes. If `allow_successes` is `true`, then
    /// `Outcome::Success` outcomes do not cause a panic.
    fn assert_empty(&self, allow_successes: bool) {
        let (unprocessed, successes, failures) = self.count();
        if !allow_successes && successes > 0 {
            panic!("{unprocessed} unprocessed inputs, {successes} successes, and {failures} failed tasks found");
        } else if unprocessed > 0 || failures > 0 {
            panic!("{unprocessed} unprocessed inputs and {failures} failed tasks found");
        }
    }

    /// Returns `true` if any of the outcomes are `Outcome::Unprocessed`.
    fn has_unprocessed(&self) -> bool {
        self.outcomes_deref()
            .values()
            .any(|outcome| outcome.is_unprocessed())
    }

    /// Returns the number of unprocessed outcomes in this store.
    fn num_unprocessed(&self) -> usize {
        self.outcomes_deref()
            .values()
            .filter(|outcome| outcome.is_unprocessed())
            .count()
    }

    /// Returns the task IDs of the unprocessed outcomes.
    fn unprocessed_task_ids(&self) -> Vec<TaskId> {
        self.outcomes_deref()
            .values()
            .filter(|outcome| outcome.is_unprocessed())
            .map(|outcome| *outcome.task_id())
            .collect()
    }

    /// Returns `true` if any of the outcomes are `Outcome::Success`.
    fn has_successes(&self) -> bool {
        self.outcomes_deref()
            .values()
            .any(|outcome| outcome.is_success())
    }

    /// Returns the number of success outcomes in this store.
    fn num_successes(&self) -> usize {
        self.outcomes_deref()
            .values()
            .filter(|outcome| outcome.is_success())
            .count()
    }

    /// Returns the task IDs of the success outcomes.
    fn success_task_ids(&self) -> Vec<TaskId> {
        self.outcomes_deref()
            .values()
            .filter(|outcome| outcome.is_success())
            .map(|outcome| *outcome.task_id())
            .collect()
    }

    /// Returns `true` if any of the outcomes are [`Outcome::Failure`], [`Outcome::Panic`], or
    /// [`Outcome::MaxRetriesAttempted`] (if the `retry` feature is enabled).
    fn has_failures(&self) -> bool {
        self.outcomes_deref()
            .values()
            .any(|outcome| outcome.is_failure())
    }

    /// Returns the number of failure outcomes in this store.
    fn num_failures(&self) -> usize {
        self.outcomes_deref()
            .values()
            .filter(|outcome| outcome.is_failure())
            .count()
    }

    /// Returns the task IDs of the success outcomes.
    fn failure_task_ids(&self) -> Vec<TaskId> {
        self.outcomes_deref()
            .values()
            .filter(|outcome| outcome.is_failure())
            .map(|outcome| *outcome.task_id())
            .collect()
    }

    /// Removes the outcome with the given `task_id``. Returns `None` if the task ID does not exist.
    fn remove(&mut self, task_id: TaskId) -> Option<Outcome<W>> {
        self.outcomes_deref_mut().remove(&task_id)
    }

    /// Removes all outcomes from this store. Returns a `Vec` containing the removed outcomes.
    fn remove_all(&mut self) -> Vec<Outcome<W>> {
        let mut outcomes: Vec<_> = self
            .outcomes_deref_mut()
            .drain()
            .map(|(_, outcome)| outcome)
            .collect();
        outcomes.sort();
        outcomes
    }

    /// Removes the outcome with the given `task_id` and returns its value. Returns `None` if the
    /// task ID does not exist. Panics if the outcome is not [`Outcome::Unprocessed`].
    fn remove_unprocessed(&mut self, task_id: TaskId) -> Option<W::Input> {
        self.outcomes_deref_mut()
            .remove(&task_id)
            .map(|outcome| match outcome {
                Outcome::Unprocessed { input: value, .. } => value,
                _ => panic!("not an Unprocessed outcome"),
            })
    }

    /// Removes and returns all unprocessed outcomes as a `Vec` of tuples `(task_id, value)`.
    fn remove_all_unprocessed(&mut self) -> Vec<(TaskId, W::Input)> {
        let task_ids = self.unprocessed_task_ids();
        task_ids
            .into_iter()
            .map(|task_id| (task_id, self.remove_unprocessed(task_id).unwrap()))
            .collect()
    }

    /// Removes the outcome with the given `task_id y and returns its value. Returns `None` if the
    /// task ID does not exist. Panics if the outcome is not [`Outcome::Success`].
    fn remove_success(&mut self, task_id: TaskId) -> Option<W::Output> {
        self.outcomes_deref_mut()
            .remove(&task_id)
            .map(|outcome| match outcome {
                Outcome::Success { value, .. } => value,
                _ => panic!("not a Success outcome"),
            })
    }

    /// Removes and returns all success outcomes as a `Vec` of tuples `(task_id, value)`.
    fn remove_all_successes(&mut self) -> Vec<(TaskId, W::Output)> {
        let task_ids = self.success_task_ids();
        task_ids
            .into_iter()
            .map(|task_id| (task_id, self.remove_success(task_id).unwrap()))
            .collect()
    }

    /// Removes the outcome with the given `task_id` and returns its value. Returns `None` if the
    /// task ID does not exist. Panics if the outcome is not a failure outcome.
    fn remove_failure(&mut self, task_id: TaskId) -> Option<Outcome<W>> {
        self.outcomes_deref_mut()
            .remove(&task_id)
            .inspect(|outcome| assert!(outcome.is_failure(), "not a failure outcome"))
    }

    /// Removes and returns all failure outcomes as a `Vec` of tuples `(task_id, value)`.
    fn remove_all_failures(&mut self) -> Vec<Outcome<W>> {
        let task_ids = self.failure_task_ids();
        task_ids
            .into_iter()
            .map(|task_id| self.remove_failure(task_id).unwrap())
            .collect()
    }

    // The following methods are available for structs that store *and* have ownership of
    // `Outcome`s (`Husk` and `OutcomeBatch`).

    /// Returns the stored `Outcome` associated with the given task_id, if any.
    fn get(&self, task_id: TaskId) -> Option<&Outcome<W>>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        self.outcomes_ref().get(&task_id)
    }

    /// Consumes this store and returns an iterator over the outcomes in task_id order.
    fn into_iter(self) -> impl Iterator<Item = Outcome<W>>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        self.outcomes().into_values()
    }

    /// Returns the successes as a `Vec` if there are no errors, otherwise panics.
    fn unwrap(self) -> Vec<W::Output>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        assert!(
            !(self.has_failures() || self.has_unprocessed()),
            "non-success outcomes found"
        );
        self.outcomes()
            .into_values()
            .filter(Outcome::is_success)
            .map(Outcome::unwrap)
            .collect()
    }

    /// Returns a `std::result::Result`: `Ok(Vec<W::Output>)` if there are no errors, otherwise
    /// `Err(Vec<W::Error>)`. If there are any `Outcome::Panic` variants, resumes unwinding the
    /// first panic. If `drop_unprocessed` is `true`, unprocessed inputs are discarded, otherwise
    /// they cause this method to panic.
    fn ok_or_unwrap_errors(self, drop_unprocessed: bool) -> Result<Vec<W::Output>, Vec<W::Error>>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        assert!(
            drop_unprocessed || !self.has_unprocessed(),
            "unprocessed inputs"
        );
        if self.has_failures() {
            let failures = self
                .into_iter()
                .filter(Outcome::is_failure)
                .filter_map(Outcome::try_into_error)
                .collect();
            Err(failures)
        } else {
            Ok(self.unwrap())
        }
    }

    /// Consumes this store and returns all the `Outcome::Unprocessed`. If `ordered` is `true`, the
    /// inputs are returned in task_id order, otherwise they are unordered.
    fn into_unprocessed(self, ordered: bool) -> Vec<W::Input>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        let values = self
            .outcomes()
            .into_values()
            .filter(Outcome::is_unprocessed);
        if ordered {
            let mut unordered: Vec<_> = values.collect();
            unordered.sort();
            unordered
                .into_iter()
                .map(Outcome::try_into_input)
                .map(Option::unwrap)
                .collect()
        } else {
            values
                .map(Outcome::try_into_input)
                .map(Option::unwrap)
                .collect()
        }
    }

    /// Returns an iterator over all the stored `Outcome::Unprocessed` outcomes. These are tasks
    /// that were queued but not yet processed when the `Hive` was dropped.
    fn iter_unprocessed(&self) -> impl Iterator<Item = (&TaskId, &W::Input)>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        self.outcomes_ref()
            .values()
            .filter_map(|result| match result {
                Outcome::Unprocessed { input, task_id } => Some((task_id, input)),
                _ => None,
            })
    }

    /// Returns an iterator over all the stored `Outcome::Success` outcomes. These are tasks
    /// that were successfully processed but not sent to any output channel.
    fn iter_successes(&self) -> impl Iterator<Item = (&TaskId, &W::Output)>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        self.outcomes_ref()
            .values()
            .filter_map(|result| match result {
                Outcome::Success { value, task_id } => Some((task_id, value)),
                _ => None,
            })
    }

    /// Returns an iterator over all the stored `Outcome::Success` outcomes. These are tasks
    /// that were successfully processed but not sent to any output channel.
    fn iter_failures(&self) -> impl Iterator<Item = &Outcome<W>>
    where
        Self: sealed::OwnedOutcomes<W>,
    {
        self.outcomes_ref()
            .values()
            .filter(|outcome| outcome.is_failure())
    }
}

/// Blanket implementation of `OutcomeStore` for types that implement `DerefOutcomes`.
impl<W: Worker, D: DerefOutcomes<W>> OutcomeStore<W> for D {}

#[cfg(test)]
mod tests {
    use super::OutcomeStore;
    use crate::bee::{Context, Worker, WorkerResult};
    use crate::hive::{Outcome, OutcomeBatch};
    use crate::panic::Panic;

    #[derive(Debug)]
    pub(super) struct TestWorker;

    impl Worker for TestWorker {
        type Input = u8;
        type Output = u8;
        type Error = ();

        fn apply(&mut self, i: Self::Input, _: &Context) -> WorkerResult<Self> {
            Ok(i)
        }
    }

    fn make_batch() -> OutcomeBatch<TestWorker> {
        let mut store = OutcomeBatch::empty();
        store.insert(Outcome::Success {
            value: 1,
            task_id: 0,
        });
        store.insert(Outcome::Unprocessed {
            input: 2,
            task_id: 1,
        });
        store.insert(Outcome::Failure {
            input: Some(3),
            error: (),
            task_id: 2,
        });
        store.insert(Outcome::Panic {
            input: Some(5),
            payload: Panic::new("oh no!", None),
            task_id: 3,
        });
        store
    }

    #[test]
    fn test_is_empty() {
        let mut store: OutcomeBatch<TestWorker> = OutcomeBatch::empty();
        assert!(store.is_empty());
        store.insert(Outcome::Success {
            value: 1,
            task_id: 0,
        });
        assert!(!store.is_empty());
    }

    #[test]
    fn test_count() {
        let store = make_batch();
        assert_eq!(store.count(), (1, 1, 2));
    }

    #[test]
    fn test_assert_empty() {
        let store: OutcomeBatch<TestWorker> = OutcomeBatch::empty();
        store.assert_empty(false);
    }

    #[test]
    fn test_assert_empty_with_success() {
        let mut store: OutcomeBatch<TestWorker> = OutcomeBatch::empty();
        store.insert(Outcome::Success {
            value: 1,
            task_id: 0,
        });
        store.assert_empty(true);
    }

    #[test]
    #[should_panic]
    fn test_assert_empty_fail() {
        let mut store: OutcomeBatch<TestWorker> = OutcomeBatch::empty();
        store.insert(Outcome::Success {
            value: 1,
            task_id: 0,
        });
        store.insert(Outcome::Unprocessed {
            input: 2,
            task_id: 1,
        });
        store.insert(Outcome::Failure {
            input: Some(3),
            error: (),
            task_id: 2,
        });
        store.assert_empty(false);
    }

    #[test]
    #[should_panic]
    fn test_assert_empty_fail_with_success() {
        let mut store: OutcomeBatch<TestWorker> = OutcomeBatch::empty();
        store.insert(Outcome::Success {
            value: 1,
            task_id: 0,
        });
        store.insert(Outcome::Unprocessed {
            input: 2,
            task_id: 1,
        });
        store.insert(Outcome::Failure {
            input: Some(3),
            error: (),
            task_id: 2,
        });
        store.assert_empty(true);
    }

    #[test]
    fn test_retrieve() {
        let store = make_batch();

        assert!(store.has_successes());
        assert!(store.get(0).unwrap().is_success());
        for task_id in 1..=3 {
            assert!(!store.get(task_id).unwrap().is_success());
        }
        assert_eq!(store.success_task_ids(), vec![0]);

        assert!(store.has_unprocessed());
        assert!(store.get(1).unwrap().is_unprocessed());
        for task_id in [0, 2, 3] {
            assert!(!store.get(task_id).unwrap().is_unprocessed());
        }
        assert_eq!(store.unprocessed_task_ids(), vec![1]);

        assert!(store.has_failures());
        for task_id in 2..=3 {
            assert!(store.get(task_id).unwrap().is_failure())
        }
        for task_id in [0, 1] {
            assert!(!store.get(task_id).unwrap().is_failure());
        }
        let mut failure_task_ids = store.failure_task_ids();
        failure_task_ids.sort();
        assert_eq!(failure_task_ids, vec![2, 3]);
    }

    #[test]
    fn test_remove() {
        let mut store = make_batch();
        for i in 0..4 {
            assert!(store.remove(i).is_some())
        }
        assert!(store.is_empty());
    }

    #[test]
    fn test_remove_kinds() {
        let mut store = make_batch();
        assert!(matches!(store.remove_success(0), Some(1)));
        assert!(matches!(store.remove_unprocessed(1), Some(2)));
        assert!(matches!(
            store.remove_failure(2),
            Some(Outcome::Failure {
                input: Some(3),
                task_id: 2,
                ..
            })
        ));
        assert!(matches!(
            store.remove_failure(3),
            Some(Outcome::Panic {
                input: Some(5),
                task_id: 3,
                ..
            })
        ));
        assert!(store.is_empty());
    }

    #[test]
    fn test_remove_all() {
        let mut store = make_batch();
        assert_eq!(vec![(0, 1)], store.remove_all_successes());
        assert_eq!(vec![(1, 2)], store.remove_all_unprocessed());
        assert_eq!(2, store.remove_all_failures().len());
    }
}

#[cfg(all(test, feature = "retry"))]
mod retry_tests {
    use super::tests::TestWorker;
    use super::OutcomeStore;
    use crate::hive::{Outcome, OutcomeBatch};
    use crate::panic::Panic;

    fn make_batch() -> OutcomeBatch<TestWorker> {
        let mut store = OutcomeBatch::empty();
        store.insert(Outcome::Success {
            value: 1,
            task_id: 0,
        });
        store.insert(Outcome::Unprocessed {
            input: 2,
            task_id: 1,
        });
        store.insert(Outcome::Failure {
            input: Some(3),
            error: (),
            task_id: 2,
        });
        store.insert(Outcome::MaxRetriesAttempted {
            input: 4,
            error: (),
            task_id: 3,
        });
        store.insert(Outcome::Panic {
            input: Some(5),
            payload: Panic::new("oh no!", None),
            task_id: 4,
        });
        store
    }

    #[test]
    fn test_count() {
        let store = make_batch();
        assert_eq!(store.count(), (1, 1, 3));
    }

    #[test]
    fn test_retrieve() {
        let store = make_batch();

        assert!(store.has_successes());
        assert!(store.get(0).unwrap().is_success());
        for task_id in 1..=4 {
            assert!(!store.get(task_id).unwrap().is_success());
        }
        assert_eq!(store.success_task_ids(), vec![0]);

        assert!(store.has_unprocessed());
        assert!(store.get(1).unwrap().is_unprocessed());
        for task_id in [0, 2, 3, 4] {
            assert!(!store.get(task_id).unwrap().is_unprocessed());
        }
        assert_eq!(store.unprocessed_task_ids(), [1]);

        assert!(store.has_failures());
        for task_id in 2..=4 {
            assert!(store.get(task_id).unwrap().is_failure())
        }
        for task_id in [0, 1] {
            assert!(!store.get(task_id).unwrap().is_failure());
        }
        let mut failure_task_ids = store.failure_task_ids();
        failure_task_ids.sort();
        assert_eq!(failure_task_ids, vec![2, 3, 4]);
    }

    #[test]
    fn test_remove() {
        let mut store = make_batch();
        for i in 0..5 {
            assert!(store.remove(i).is_some())
        }
        assert!(store.is_empty());
    }

    #[test]
    fn test_remove_kinds() {
        let mut store = make_batch();
        assert!(matches!(store.remove_success(0), Some(1)));
        assert!(matches!(store.remove_unprocessed(1), Some(2)));
        assert!(matches!(
            store.remove_failure(2),
            Some(Outcome::Failure {
                input: Some(3),
                task_id: 2,
                ..
            })
        ));
        assert!(matches!(
            store.remove_failure(3),
            Some(Outcome::MaxRetriesAttempted {
                input: 4,
                task_id: 3,
                ..
            })
        ));
        assert!(matches!(
            store.remove_failure(4),
            Some(Outcome::Panic {
                input: Some(5),
                task_id: 4,
                ..
            })
        ));
        assert!(store.is_empty());
    }

    #[test]
    fn test_remove_all() {
        let mut store = make_batch();
        assert_eq!(vec![(0, 1)], store.remove_all_successes());
        assert_eq!(vec![(1, 2)], store.remove_all_unprocessed());
        assert_eq!(3, store.remove_all_failures().len());
    }
}
