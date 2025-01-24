use super::{
    Builder, Config, DerefOutcomes, Hive, Outcome, OutcomeBatch, OutcomeSender, OwnedOutcomes,
    SpawnError,
};
use crate::bee::{Queen, TaskId, Worker};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

/// The remnants of a `Hive`.
///
/// Provides access to the `Queen` and to stored `Outcome`s. Can be used to create a new `Hive`
/// based on the previous `Hive`'s configuration.
pub struct Husk<W: Worker, Q: Queen<Kind = W>> {
    config: Config,
    queen: Q,
    num_panics: usize,
    outcomes: HashMap<TaskId, Outcome<W>>,
}

impl<W: Worker, Q: Queen<Kind = W>> Husk<W, Q> {
    /// Creates a new `Husk`. Should only be called from `Shared::try_into_husk`.
    pub(super) fn new(
        config: Config,
        queen: Q,
        num_panics: usize,
        outcomes: HashMap<TaskId, Outcome<W>>,
    ) -> Self {
        Self {
            config,
            queen,
            num_panics,
            outcomes,
        }
    }

    /// The `Queen` of the former `Hive`.
    pub fn queen(&self) -> &Q {
        &self.queen
    }

    /// The number of panicked threads in the former `Hive`.
    pub fn num_panics(&self) -> usize {
        self.num_panics
    }

    /// Consumes this `Husk` and returns the `Queen` and `Outcome`s.
    pub fn into_parts(self) -> (Q, OutcomeBatch<W>) {
        (self.queen, OutcomeBatch::new(self.outcomes))
    }

    /// Returns a new `Builder` that will create a `Hive` with the same configuration as the one
    /// that produced this `Husk`.
    pub fn as_builder(&self) -> Builder {
        self.config.clone().into()
    }

    /// Consumes this `Husk` and returns a new `Hive` with the same configuration and `Queen` as
    /// the one that produced this `Husk`.
    pub fn into_hive(self) -> Result<Hive<W, Q>, SpawnError> {
        self.as_builder().build(self.queen)
    }

    /// Extracts all the `Outcome::Unprocessed` values from the provided map and returns them as a
    /// `Vec` of input values.
    fn collect_unprocessed(outcomes: HashMap<TaskId, Outcome<W>>) -> Vec<W::Input> {
        outcomes
            .into_values()
            .filter_map(|outcome| match outcome {
                Outcome::Unprocessed { input: value, .. } => Some(value),
                _ => None,
            })
            .collect()
    }

    /// Consumes this `Husk` and creates a new `Hive` with the same configuration as the one that
    /// produced this `Husk`, and queues all the `Outcome::Unprocessed` values. The results will
    /// be sent to `tx`. Returns the new `Hive` and the IDs of the tasks that were queued.
    ///
    /// This method returns a `SpawnError` if there is an error creating the new `Hive`.
    pub fn into_hive_swarm_unprocessed_to(
        self,
        tx: OutcomeSender<W>,
    ) -> Result<(Hive<W, Q>, Vec<TaskId>), SpawnError> {
        let hive = self.as_builder().build(self.queen)?;
        let unprocessed = Self::collect_unprocessed(self.outcomes);
        let task_ids = hive.swarm_send(unprocessed, tx);
        Ok((hive, task_ids))
    }

    /// Consumes this `Husk` and creates a new `Hive` with the same configuration as the one that
    /// produced this `Husk`, and queues all the `Outcome::Unprocessed` values. The results will
    /// be retained in the new `Hive` for later retrieval. Returns the new `Hive` and the IDs
    /// of the tasks that were queued.
    ///
    /// This method returns a `SpawnError` if there is an error creating the new `Hive`.
    pub fn into_hive_swarm_unprocessed_store(
        self,
    ) -> Result<(Hive<W, Q>, Vec<TaskId>), SpawnError> {
        let hive = self.as_builder().build(self.queen)?;
        let unprocessed = Self::collect_unprocessed(self.outcomes);
        let task_ids = hive.swarm_store(unprocessed);
        Ok((hive, task_ids))
    }
}

impl<W: Worker, Q: Queen<Kind = W>> DerefOutcomes<W> for Husk<W, Q> {
    #[inline]
    fn outcomes_deref(&self) -> impl Deref<Target = HashMap<TaskId, Outcome<W>>> {
        &self.outcomes
    }

    #[inline]
    fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> {
        &mut self.outcomes
    }
}

impl<W: Worker, Q: Queen<Kind = W>> OwnedOutcomes<W> for Husk<W, Q> {
    #[inline]
    fn outcomes(self) -> HashMap<TaskId, Outcome<W>> {
        self.outcomes
    }

    #[inline]
    fn outcomes_ref(&self) -> &HashMap<TaskId, Outcome<W>> {
        &self.outcomes
    }
}

#[cfg(test)]
mod tests {
    use crate::bee::stock::{PunkWorker, Thunk, ThunkWorker};
    use crate::hive::{outcome_channel, Builder, Outcome, OutcomeIteratorExt, OutcomeStore};

    #[test]
    fn test_unprocessed() {
        // don't spin up any worker threads so that no tasks will be processed
        let hive = Builder::new()
            .num_threads(0)
            .build_with_default::<ThunkWorker<u8>>()
            .unwrap();
        let mut task_ids = hive.map_store((0..10).map(|i| Thunk::of(move || i)));
        // cancel and smash the hive before the tasks can be processed
        hive.suspend();
        let mut husk = hive.try_into_husk().unwrap();
        assert!(husk.has_unprocessed());
        for i in task_ids.iter() {
            assert!(husk.get(*i).unwrap().is_unprocessed());
        }
        assert_eq!(husk.iter_unprocessed().count(), 10);
        let mut unprocessed_task_ids = husk
            .iter_unprocessed()
            .map(|(task_id, _)| *task_id)
            .collect::<Vec<_>>();
        task_ids.sort();
        unprocessed_task_ids.sort();
        assert_eq!(task_ids, unprocessed_task_ids);
        assert_eq!(husk.remove_all_unprocessed().len(), 10);
    }

    #[test]
    fn test_reprocess_unprocessed() {
        // don't spin up any worker threads so that no tasks will be processed
        let hive1 = Builder::new()
            .num_threads(0)
            .build_with_default::<ThunkWorker<u8>>()
            .unwrap();
        let _ = hive1.map_store((0..10).map(|i| Thunk::of(move || i)));
        // cancel and smash the hive before the tasks can be processed
        hive1.suspend();
        let husk1 = hive1.try_into_husk().unwrap();
        let (hive2, _) = husk1.into_hive_swarm_unprocessed_store().unwrap();
        // now spin up worker threads to process the tasks
        hive2.grow(8);
        hive2.join();
        let husk2 = hive2.try_into_husk().unwrap();
        assert!(!husk2.has_unprocessed());
        assert!(husk2.has_successes());
        assert_eq!(husk2.iter_successes().count(), 10);
    }

    #[test]
    fn test_reprocess_unprocessed_to() {
        // don't spin up any worker threads so that no tasks will be processed
        let hive1 = Builder::new()
            .num_threads(0)
            .build_with_default::<ThunkWorker<u8>>()
            .unwrap();
        let _ = hive1.map_store((0..10).map(|i| Thunk::of(move || i)));
        // cancel and smash the hive before the tasks can be processed
        hive1.suspend();
        let husk1 = hive1.try_into_husk().unwrap();
        let (tx, rx) = outcome_channel();
        let (hive2, task_ids) = husk1.into_hive_swarm_unprocessed_to(tx).unwrap();
        // now spin up worker threads to process the tasks
        hive2.grow(8);
        hive2.join();
        let husk2 = hive2.try_into_husk().unwrap();
        assert!(husk2.is_empty());
        let mut outputs = rx
            .select_ordered(task_ids)
            .map(Outcome::unwrap)
            .collect::<Vec<_>>();
        outputs.sort();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn test_into_result() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with_default::<ThunkWorker<u8>>()
            .unwrap();
        hive.map_store((0..10).map(|i| Thunk::of(move || i)));
        hive.join();
        let mut outputs = hive.try_into_husk().unwrap().into_parts().1.unwrap();
        outputs.sort();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>());
    }

    #[test]
    #[should_panic]
    fn test_into_result_panic() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with_default::<PunkWorker<u8>>()
            .unwrap();
        hive.map_store(
            (0..10).map(|i| Thunk::of(move || if i == 5 { panic!("oh no!") } else { i })),
        );
        hive.join();
        let (_, result) = hive.try_into_husk().unwrap().into_parts();
        let _ = result.ok_or_unwrap_errors(true);
    }
}
