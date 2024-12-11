use crate::hive::Outcome;
use crate::task::Worker;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    panic,
};

/// Trait implemented by both `Hive` and `Husk` with methodS for retrieving stored `Outcome`s.
pub trait Stored<W: Worker> {
    /// Returns a read-only reference to a map of task index to `Outcome`.
    fn outcomes(&self) -> impl Deref<Target = HashMap<usize, Outcome<W>>>;

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
    fn remove_success(&mut self, index: usize) -> Option<(usize, W::Output)> {
        self.outcomes_mut()
            .remove(&index)
            .map(|outcome| match outcome {
                Outcome::Success { value, index } => (index, value),
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
}
