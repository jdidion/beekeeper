#[cfg(feature = "affinity")]
use crate::hive::Cores;
use crate::hive::{BatchResult, Builder, Hive, Outcome, OutcomeSender, Stored};
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
    pub fn into_result(self) -> BatchResult<W> {
        self.outcomes.into_values().map(Outcome::into).into()
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
    pub fn into_hive_swarm_unprocessed_store(self) -> (Hive<W, Q>, Vec<usize>) {
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

#[cfg(test)]
mod tests {
    use crate::hive::{outcome_channel, Builder, Outcome, OutcomeIteratorExt, Stored};
    use crate::util::{PunkWorker, Thunk, ThunkWorker};

    #[test]
    fn test_unprocessed() {
        // don't spin up any worker threads so that no tasks will be processed
        let hive = Builder::new()
            .num_threads(0)
            .build_with_default::<ThunkWorker<u8>>();
        let mut indices = hive.map_store((0..10).into_iter().map(|i| Thunk::of(move || i)));
        // cancel and smash the hive before the tasks can be processed
        hive.cancel();
        let husk = hive.into_husk();
        assert!(husk.has_any_unprocessed());
        for i in indices.iter() {
            assert!(husk.has_unprocessed(*i));
        }
        assert_eq!(husk.iter_unprocessed().count(), 10);
        let mut unprocessed_indices = husk
            .iter_unprocessed()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        indices.sort();
        unprocessed_indices.sort();
        assert_eq!(indices, unprocessed_indices);
        assert_eq!(husk.into_unprocessed().len(), 10);
    }

    #[test]
    fn test_reprocess_unprocessed() {
        // don't spin up any worker threads so that no tasks will be processed
        let hive1 = Builder::new()
            .num_threads(0)
            .build_with_default::<ThunkWorker<u8>>();
        let _ = hive1.map_store((0..10).into_iter().map(|i| Thunk::of(move || i)));
        // cancel and smash the hive before the tasks can be processed
        hive1.cancel();
        let husk1 = hive1.into_husk();
        let (hive2, _) = husk1.into_hive_swarm_unprocessed_store();
        // now spin up worker threads to process the tasks
        hive2.set_num_threads(8);
        hive2.join();
        let husk2 = hive2.into_husk();
        assert!(!husk2.has_any_unprocessed());
        assert!(husk2.has_any_successes());
        assert_eq!(husk2.iter_successes().count(), 10);
    }

    #[test]
    fn test_reprocess_unprocessed_to() {
        // don't spin up any worker threads so that no tasks will be processed
        let hive1 = Builder::new()
            .num_threads(0)
            .build_with_default::<ThunkWorker<u8>>();
        let _ = hive1.map_store((0..10).into_iter().map(|i| Thunk::of(move || i)));
        // cancel and smash the hive before the tasks can be processed
        hive1.cancel();
        let husk1 = hive1.into_husk();
        let (tx, rx) = outcome_channel();
        let (hive2, _) = husk1.into_hive_swarm_unprocessed_to(tx);
        // now spin up worker threads to process the tasks
        hive2.set_num_threads(8);
        hive2.join();
        let husk2 = hive2.into_husk();
        assert!(husk2.is_empty());
        let mut outputs = rx.into_ordered().map(Outcome::unwrap).collect::<Vec<_>>();
        outputs.sort();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn test_into_result() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with_default::<ThunkWorker<u8>>();
        hive.map_store((0..10).into_iter().map(|i| Thunk::of(move || i)));
        hive.join();
        let mut outputs = hive.into_husk().into_result().unwrap();
        outputs.sort();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>());
    }

    #[test]
    #[should_panic]
    fn test_into_result_panic() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with_default::<PunkWorker<u8>>();
        hive.map_store(
            (0..10)
                .into_iter()
                .map(|i| Thunk::of(move || if i == 5 { panic!("oh no!") } else { i })),
        );
        hive.join();
        let result = hive.into_husk().into_result();
        let _ = result.ok_or_unwrap_errors(true);
    }
}
