#[cfg(feature = "affinity")]
use crate::hive::Cores;
use crate::hive::{Builder, Hive, HiveResult, Outcome, OutcomeSender, Stored};
use crate::task::{Queen, Worker};
use std::{
    collections::HashMap,
    mem,
    ops::{Deref, DerefMut},
};

/// The remnants of a `Hive`.
pub struct Husk<W: Worker, Q: Queen<Kind = W>> {
    queen: Q,
    panic_count: usize,
    outcomes: HashMap<usize, Outcome<W>>,
    num_threads: usize,
    thread_name: Option<String>,
    thread_stack_size: Option<usize>,
    #[cfg(feature = "affinity")]
    affinity: Cores,
}

impl<W: Worker, Q: Queen<Kind = W>> Husk<W, Q> {
    pub(crate) fn new(
        queen: Q,
        panic_count: usize,
        outcomes: HashMap<usize, Outcome<W>>,
        num_threads: usize,
        thread_name: Option<String>,
        thread_stack_size: Option<usize>,
        #[cfg(feature = "affinity")] affinity: Cores,
    ) -> Self {
        Self {
            queen,
            panic_count,
            outcomes,
            num_threads,
            thread_name,
            thread_stack_size,
            #[cfg(feature = "affinity")]
            affinity,
        }
    }

    /// The `Queen` of the former `Hive`.
    pub fn queen(&self) -> &Q {
        &self.queen
    }

    /// The number of panicked threads in the former `Hive`.
    pub fn panic_count(&self) -> usize {
        self.panic_count
    }

    /// Returns the stored `Outcome` associated with the given index, if any.
    pub fn get(&self, index: usize) -> Option<&Outcome<W>> {
        self.outcomes.get(&index)
    }

    /// Returns an iterator over all the stored `Outcome::Unprocessed` outcomes. These are tasks
    /// that were queued but not yet processed when the `Hive` was dropped.
    pub fn iter_unprocessed(&self) -> impl Iterator<Item = (&usize, &W::Input)> {
        self.outcomes
            .values()
            .into_iter()
            .filter_map(|result| match result {
                Outcome::Unprocessed {
                    input: value,
                    index,
                } => Some((index, value)),
                _ => None,
            })
    }

    /// Returns an iterator over all the stored `Outcome::Success` outcomes. These are tasks
    /// that were successfully processed but not sent to any output channel.
    pub fn iter_successes(&self) -> impl Iterator<Item = (&usize, &W::Output)> {
        self.outcomes
            .values()
            .into_iter()
            .filter_map(|result| match result {
                Outcome::Success { value, index } => Some((index, value)),
                _ => None,
            })
    }

    /// Takes all `Outcome`s out of this `Husk`.
    pub fn take_all(&mut self) -> Vec<Outcome<W>> {
        mem::take(&mut self.outcomes)
            .into_values()
            .into_iter()
            .collect()
    }

    /// Converts all `Outcome`s into `Result`s and returns a `Vec` with the stored values, or
    /// an error if there were any stored failure `Outcome`s.
    pub fn into_result(self) -> HiveResult<Vec<W::Output>, W> {
        self.outcomes
            .into_values()
            .into_iter()
            .map(Outcome::into)
            .collect()
    }

    /// Consumes this `Husk` and returns the `Queen` and `Outcome`s.
    pub fn into_parts(self) -> (Q, Vec<Outcome<W>>) {
        (
            self.queen,
            self.outcomes.into_values().into_iter().collect(),
        )
    }

    /// Returns a new `Builder` that will create a `Hive` with the same configuration as the one
    /// that produced this `Husk`.
    pub fn as_builder(&self) -> Builder {
        let mut builder = Builder::new().num_threads(self.num_threads);
        if let Some(thread_name) = &self.thread_name {
            builder = builder.thread_name(thread_name.to_owned());
        }
        if let Some(thread_stack_size) = self.thread_stack_size {
            builder = builder.thread_stack_size(thread_stack_size);
        }
        #[cfg(feature = "affinity")]
        {
            builder = builder.thread_affinity(self.affinity.clone());
        }
        builder
    }

    /// Consumes this `Husk` and returns a new `Hive` with the same configuration as the one that
    /// produced this `Husk`.
    pub fn into_hive(self) -> Hive<W, Q> {
        self.as_builder().build(self.queen)
    }

    fn outcomes_to_unprocessed(outcomes: HashMap<usize, Outcome<W>>) -> Vec<W::Input> {
        outcomes
            .into_values()
            .into_iter()
            .filter_map(|outcome| match outcome {
                Outcome::Unprocessed { input: value, .. } => Some(value),
                _ => None,
            })
            .collect()
    }

    /// Consumes this `Husk` and returns all the `Outcome::Unprocessed` values.
    pub fn into_unprocessed(self) -> Vec<W::Input> {
        Self::outcomes_to_unprocessed(self.outcomes)
    }

    /// Consumes this `Husk` and creates a new `Hive` with the same configuration as the one that
    /// produced this `Husk`, and queues all the `Outcome::Unprocessed` values. The results will
    /// be sent to `tx`. Returns the new `Hive` and the indices of the tasks that were queued.
    pub fn into_hive_swarm_unprocessed_to(self, tx: OutcomeSender<W>) -> (Hive<W, Q>, Vec<usize>) {
        let hive = self.as_builder().build(self.queen);
        let unprocessed = Self::outcomes_to_unprocessed(self.outcomes);
        let indices = hive.swarm_send(unprocessed, tx);
        (hive, indices)
    }

    /// Consumes this `Husk` and creates a new `Hive` with the same configuration as the one that
    /// produced this `Husk`, and queues all the `Outcome::Unprocessed` values. The results will
    /// be retained in the new `Hive` for later retrieval. Returns the new `Hive` and the indices
    /// of the tasks that were queued.
    pub fn into_hive_swarm_unprocessed_keep(self) -> (Hive<W, Q>, Vec<usize>) {
        let hive = self.as_builder().build(self.queen);
        let unprocessed = Self::outcomes_to_unprocessed(self.outcomes);
        let indices = hive.swarm_store(unprocessed);
        (hive, indices)
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Stored<W> for Husk<W, Q> {
    fn outcomes(&self) -> impl Deref<Target = HashMap<usize, Outcome<W>>> {
        &self.outcomes
    }

    fn outcomes_mut(&mut self) -> impl DerefMut<Target = HashMap<usize, Outcome<W>>> {
        &mut self.outcomes
    }
}
