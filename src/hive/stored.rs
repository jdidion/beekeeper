use crate::hive::Outcome;
use crate::task::Worker;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    panic,
};

/// Trait implemented by both `Hive` and `Husk` with methods for retrieving stored `Outcome`s.
pub trait Stored<W: Worker> {
    /// Returns a read-only reference to a map of task index to `Outcome`.
    fn outcomes(&self) -> impl Deref<Target = HashMap<usize, Outcome<W>>>;

    /// Returns `true` if there are no stored outcomes.
    fn is_empty(&self) -> bool {
        self.outcomes().is_empty()
    }

    /// Returns counts of the unsuccessful outcomes as a tuple
    /// `(unprocessed, successes, failures)`.
    fn count(&self) -> (usize, usize, usize) {
        self.outcomes().values().into_iter().fold(
            (0usize, 0usize, 0usize),
            |(unprocessed, successes, failures), result| match result {
                Outcome::Success { .. } => (unprocessed, successes + 1, failures),
                Outcome::Unprocessed { .. } => (unprocessed + 1, successes, failures),
                Outcome::Failure { .. } => (unprocessed, successes, failures + 1),
                Outcome::MaxRetriesAttempted { .. } => (unprocessed, successes, failures + 1),
                Outcome::Panic { .. } => (unprocessed, successes, failures + 1),
            },
        )
    }

    /// Panics if there are stored outcomes. If `allow_successes` is `true`, then
    /// `Outcome::Success` outcomes do not cause a panic.
    fn assert_empty(&self, allow_successes: bool) {
        let (unprocessed, successes, failures) = self.count();
        if !allow_successes && successes > 0 {
            panic!("{unprocessed} unprocessed inputs, {successes} successes, and {failures} failed jobs found");
        } else if unprocessed > 0 || failures > 0 {
            panic!("{unprocessed} unprocessed inputs and {failures} failed jobs found");
        }
    }

    fn has_unprocessed(&self, index: usize) -> bool {
        self.outcomes()
            .get(&index)
            .into_iter()
            .any(|outcome| matches!(outcome, Outcome::Unprocessed { .. }))
    }

    /// Returns `true` if any of the outcomes are `Outcome::Unprocessed`.
    fn has_any_unprocessed(&self) -> bool {
        self.outcomes()
            .values()
            .into_iter()
            .any(|outcome| matches!(outcome, Outcome::Unprocessed { .. }))
    }

    /// Returns the task indicies of the unprocessed outcomes.
    fn unprocessed_indices(&self) -> Vec<usize> {
        self.outcomes()
            .values()
            .into_iter()
            .filter_map(|outcome| match outcome {
                Outcome::Unprocessed { index, .. } => Some(*index),
                _ => None,
            })
            .collect()
    }

    fn has_success(&self, index: usize) -> bool {
        self.outcomes()
            .get(&index)
            .into_iter()
            .any(|outcome| matches!(outcome, Outcome::Success { .. }))
    }

    /// Returns `true` if any of the outcomes are `Outcome::Success`.
    fn has_any_successes(&self) -> bool {
        self.outcomes()
            .values()
            .into_iter()
            .any(|outcome| matches!(outcome, Outcome::Success { .. }))
    }

    /// Returns the task indicies of the success outcomes.
    fn success_indices(&self) -> Vec<usize> {
        self.outcomes()
            .values()
            .into_iter()
            .filter_map(|outcome| match outcome {
                Outcome::Success { index, .. } => Some(*index),
                _ => None,
            })
            .collect()
    }

    fn has_failure(&self, index: usize) -> bool {
        self.outcomes()
            .get(&index)
            .into_iter()
            .any(|outcome| match outcome {
                Outcome::Failure { .. }
                | Outcome::MaxRetriesAttempted { .. }
                | Outcome::Panic { .. } => true,
                _ => false,
            })
    }

    /// Returns `true` if any of the outcomes are `Outcome::Success`.
    fn has_any_failures(&self) -> bool {
        self.outcomes()
            .values()
            .into_iter()
            .any(|outcome| match outcome {
                Outcome::Failure { .. }
                | Outcome::MaxRetriesAttempted { .. }
                | Outcome::Panic { .. } => true,
                _ => false,
            })
    }

    /// Returns the task indicies of the success outcomes.
    fn failure_indices(&self) -> Vec<usize> {
        self.outcomes()
            .values()
            .into_iter()
            .filter_map(|outcome| match outcome {
                Outcome::Failure { index, .. }
                | Outcome::MaxRetriesAttempted { index, .. }
                | Outcome::Panic { index, .. } => Some(*index),
                _ => None,
            })
            .collect()
    }

    /// Returns a mutable reference to a map of task index to `Outcome`.
    fn outcomes_mut(&mut self) -> impl DerefMut<Target = HashMap<usize, Outcome<W>>>;

    /// Removes the outcome with the given index. Returns `None` if the index does not exist.
    fn remove(&mut self, index: usize) -> Option<Outcome<W>> {
        self.outcomes_mut().remove(&index)
    }

    /// Removes the outcome with the given index and returns its value. Returns `None` if the index
    /// does not exist. Panics if the outcome is not `Outcome::Unprocessed`.
    fn remove_unprocessed(&mut self, index: usize) -> Option<W::Input> {
        self.outcomes_mut()
            .remove(&index)
            .map(|outcome| match outcome {
                Outcome::Unprocessed { input: value, .. } => value,
                _ => panic!("not an Unprocessed outcome"),
            })
    }

    /// Removes and returns all unprocessed outcomes as a `Vec` of tuples `(index, value)`.
    fn remove_all_unprocessed(&mut self) -> Vec<(usize, W::Input)> {
        let indices = self.unprocessed_indices();
        let mut outcomes = self.outcomes_mut();
        indices
            .into_iter()
            .map(|index| match outcomes.remove(&index) {
                Some(Outcome::Unprocessed {
                    input: value,
                    index,
                }) => (index, value),
                _ => panic!("unexpected outcome"),
            })
            .collect()
    }

    /// Removes the outcome with the given index and returns its value. Returns `None` if the index
    /// does not exist. Panics if the outcome is not `Outcome::Success`.
    fn remove_success(&mut self, index: usize) -> Option<W::Output> {
        self.outcomes_mut()
            .remove(&index)
            .map(|outcome| match outcome {
                Outcome::Success { value, .. } => value,
                _ => panic!("not a Success outcome"),
            })
    }

    /// Removes and returns all success outcomes as a `Vec` of tuples `(index, value)`.
    fn remove_all_successes(&mut self) -> Vec<(usize, W::Output)> {
        let indices = self.success_indices();
        let mut outcomes = self.outcomes_mut();
        indices
            .into_iter()
            .map(|index| match outcomes.remove(&index) {
                Some(Outcome::Success { value, index }) => (index, value),
                _ => panic!("unexpected outcome"),
            })
            .collect()
    }

    /// Removes the outcome with the given index and returns its value. Returns `None` if the index
    /// does not exist. Panics if the outcome is not `Outcome::Success`.
    fn remove_failure(&mut self, index: usize) -> Option<Outcome<W>> {
        self.outcomes_mut()
            .remove(&index)
            .map(|outcome| match outcome {
                outcome @ (Outcome::Failure { .. }
                | Outcome::MaxRetriesAttempted { .. }
                | Outcome::Panic { .. }) => outcome,
                _ => panic!("not a failure outcome"),
            })
    }

    /// Removes and returns all success outcomes as a `Vec` of tuples `(index, value)`.
    fn remove_all_failures(&mut self) -> Vec<Outcome<W>> {
        let indices = self.failure_indices();
        let mut outcomes = self.outcomes_mut();
        indices
            .into_iter()
            .map(|index| match outcomes.remove(&index) {
                Some(
                    outcome @ (Outcome::Failure { .. }
                    | Outcome::MaxRetriesAttempted { .. }
                    | Outcome::Panic { .. }),
                ) => outcome,
                _ => panic!("unexpected outcome"),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Stored;
    use crate::hive::Outcome;
    use crate::task::{Context, Worker, WorkerResult};
    use crate::Panic;
    use std::{
        collections::HashMap,
        ops::{Deref, DerefMut},
    };

    #[derive(Debug)]
    struct TestWorker;

    impl Worker for TestWorker {
        type Input = u8;
        type Output = u8;
        type Error = ();

        fn apply(&mut self, i: Self::Input, _: &Context) -> WorkerResult<Self> {
            Ok(i)
        }
    }

    #[derive(Default)]
    struct TestStored(HashMap<usize, Outcome<TestWorker>>);

    impl Stored<TestWorker> for TestStored {
        fn outcomes(&self) -> impl Deref<Target = HashMap<usize, Outcome<TestWorker>>> {
            &self.0
        }

        fn outcomes_mut(&mut self) -> impl DerefMut<Target = HashMap<usize, Outcome<TestWorker>>> {
            &mut self.0
        }
    }

    fn make_stored() -> TestStored {
        let mut store = TestStored::default();
        store.0.insert(0, Outcome::Success { value: 1, index: 0 });
        store
            .0
            .insert(1, Outcome::Unprocessed { input: 2, index: 1 });
        store.0.insert(
            2,
            Outcome::Failure {
                input: Some(3),
                error: (),
                index: 2,
            },
        );
        store.0.insert(
            3,
            Outcome::MaxRetriesAttempted {
                input: 4,
                error: (),
                index: 3,
            },
        );
        store.0.insert(
            4,
            Outcome::Panic {
                input: Some(5),
                payload: Panic::new("oh no!", None),
                index: 4,
            },
        );
        store
    }

    #[test]
    fn test_is_empty() {
        let mut store = TestStored::default();
        assert!(store.is_empty());
        store.0.insert(0, Outcome::Success { value: 1, index: 0 });
        assert!(!store.is_empty());
    }

    #[test]
    fn test_count() {
        let store = make_stored();
        assert_eq!(store.count(), (1, 1, 3));
    }

    #[test]
    fn test_assert_empty() {
        let store = TestStored::default();
        store.assert_empty(false);
    }

    #[test]
    fn test_assert_empty_with_success() {
        let mut store = TestStored::default();
        store.0.insert(0, Outcome::Success { value: 1, index: 0 });
        store.assert_empty(true);
    }

    #[test]
    #[should_panic]
    fn test_assert_empty_fail() {
        let mut store = TestStored::default();
        store.0.insert(0, Outcome::Success { value: 1, index: 0 });
        store
            .0
            .insert(1, Outcome::Unprocessed { input: 2, index: 1 });
        store.0.insert(
            2,
            Outcome::Failure {
                input: Some(3),
                error: (),
                index: 2,
            },
        );
        store.assert_empty(false);
    }

    #[test]
    #[should_panic]
    fn test_assert_empty_fail_with_success() {
        let mut store = TestStored::default();
        store.0.insert(0, Outcome::Success { value: 1, index: 0 });
        store
            .0
            .insert(1, Outcome::Unprocessed { input: 2, index: 1 });
        store.0.insert(
            2,
            Outcome::Failure {
                input: Some(3),
                error: (),
                index: 2,
            },
        );
        store.assert_empty(true);
    }

    #[test]
    fn test_retrieve() {
        let store = make_stored();

        assert!(store.has_any_successes());
        assert!(store.has_success(0));
        for index in 1..=4 {
            assert!(!store.has_success(index));
        }
        assert_eq!(store.success_indices(), vec![0]);

        assert!(store.has_any_unprocessed());
        assert!(store.has_unprocessed(1));
        for index in vec![0, 2, 3, 4] {
            assert!(!store.has_unprocessed(index));
        }
        assert_eq!(store.unprocessed_indices(), vec![1]);

        assert!(store.has_any_failures());
        assert!(store.has_failure(2));
        assert!(store.has_failure(3));
        assert!(store.has_failure(4));
        for index in vec![0, 1] {
            assert!(!store.has_failure(index));
        }
        let mut failure_indices = store.failure_indices();
        failure_indices.sort();
        assert_eq!(failure_indices, vec![2, 3, 4]);
    }

    #[test]
    fn test_remove() {
        let mut store = make_stored();
        for i in 0..5 {
            assert!(store.remove(i).is_some())
        }
        assert!(store.is_empty());
    }

    #[test]
    fn test_remove_kinds() {
        let mut store = make_stored();
        assert!(matches!(store.remove_success(0), Some(1)));
        assert!(matches!(store.remove_unprocessed(1), Some(2)));
        assert!(matches!(
            store.remove_failure(2),
            Some(Outcome::Failure {
                input: Some(3),
                index: 2,
                ..
            })
        ));
        assert!(matches!(
            store.remove_failure(3),
            Some(Outcome::MaxRetriesAttempted {
                input: 4,
                index: 3,
                ..
            })
        ));
        assert!(matches!(
            store.remove_failure(4),
            Some(Outcome::Panic {
                input: Some(5),
                index: 4,
                ..
            })
        ));
        assert!(store.is_empty());
    }

    #[test]
    fn test_remove_all() {
        let mut store = make_stored();
        assert_eq!(vec![(0, 1)], store.remove_all_successes());
        assert_eq!(vec![(1, 2)], store.remove_all_unprocessed());
        assert_eq!(3, store.remove_all_failures().len());
    }
}
