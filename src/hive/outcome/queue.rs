use super::{DerefOutcomes, Outcome};
use crate::bee::{TaskId, Worker};
use crossbeam_queue::SegQueue;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

/// Data structure that supports queuing `Outcomes` from multiple threads (without locking) and
/// fetching from a single thread (which requires draining the queue into a map that is behind a
/// mutex).
pub struct OutcomeQueue<W: Worker> {
    queue: SegQueue<Outcome<W>>,
    outcomes: Mutex<HashMap<TaskId, Outcome<W>>>,
}

impl<W: Worker> OutcomeQueue<W> {
    /// Adds an `outcome` to the queue.
    pub fn push(&self, outcome: Outcome<W>) {
        self.queue.push(outcome);
    }

    /// Flushes the queue into the map of outcomes and returns a mutable reference to the map.
    pub fn get_mut(&self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> {
        let mut outcomes = self.outcomes.lock();
        drain_into(&self.queue, &mut outcomes);
        outcomes
    }

    /// Flushes the queue into the map of outcomes, then takes all outcomes from the map and
    /// returns them.
    pub fn drain(&self) -> HashMap<TaskId, Outcome<W>> {
        let mut outcomes: HashMap<TaskId, Outcome<W>> = self.outcomes.lock().drain().collect();
        drain_into(&self.queue, &mut outcomes);
        outcomes
    }

    /// Consumes this `OutcomeQueue`, drains the queue, and returns the outcomes as a map.
    pub fn into_inner(self) -> HashMap<TaskId, Outcome<W>> {
        let mut outcomes = self.outcomes.into_inner();
        drain_into(&self.queue, &mut outcomes);
        outcomes
    }
}

#[inline]
fn drain_into<W: Worker>(queue: &SegQueue<Outcome<W>>, outcomes: &mut HashMap<TaskId, Outcome<W>>) {
    while let Some(outcome) = queue.pop() {
        outcomes.insert(*outcome.task_id(), outcome);
    }
}

impl<W: Worker> Default for OutcomeQueue<W> {
    fn default() -> Self {
        Self {
            queue: Default::default(),
            outcomes: Default::default(),
        }
    }
}

impl<W: Worker> DerefOutcomes<W> for OutcomeQueue<W> {
    fn outcomes_deref(&self) -> impl Deref<Target = HashMap<TaskId, Outcome<W>>> {
        self.get_mut()
    }

    fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> {
        self.get_mut()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::bee::stock::EchoWorker;
    use crate::hive::OutcomeStore;

    #[test]
    fn test_works() {
        let queue = OutcomeQueue::<EchoWorker<usize>>::default();
        queue.push(Outcome::Success {
            value: 42,
            task_id: 1,
        });
        queue.push(Outcome::Unprocessed {
            input: 43,
            task_id: 2,
        });
        queue.push(Outcome::Failure {
            input: Some(44),
            error: (),
            task_id: 3,
        });
        assert_eq!(queue.count(), (1, 1, 1));
        queue.push(Outcome::Missing { task_id: 4 });
        let outcomes = queue.drain();
        assert_eq!(outcomes.len(), 4);
        assert_eq!(
            outcomes[&1],
            Outcome::Success {
                value: 42,
                task_id: 1
            }
        );
        assert_eq!(
            outcomes[&2],
            Outcome::Unprocessed {
                input: 43,
                task_id: 2
            }
        );
        assert_eq!(
            outcomes[&3],
            Outcome::Failure {
                input: Some(44),
                error: (),
                task_id: 3
            }
        );
        assert_eq!(outcomes[&4], Outcome::Missing { task_id: 4 })
    }
}
