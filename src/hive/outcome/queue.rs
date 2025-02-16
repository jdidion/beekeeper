use super::Outcome;
use crate::bee::{TaskId, Worker};
use crossbeam_queue::SegQueue;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::DerefMut;

pub struct OutcomeQueue<W: Worker> {
    queue: SegQueue<Outcome<W>>,
    outcomes: Mutex<HashMap<TaskId, Outcome<W>>>,
}

impl<W: Worker> OutcomeQueue<W> {
    /// Adds an outcome to the queue.
    pub fn push(&self, outcome: Outcome<W>) {
        self.queue.push(outcome);
    }

    /// Flushes the queue into the map of outcomes and returns a mutable reference to the map.
    pub fn get_mut(&self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> + '_ {
        let mut outcomes = self.outcomes.lock();
        // add any queued outcomes to the map
        while let Some(outcome) = self.queue.pop() {
            outcomes.insert(*outcome.task_id(), outcome);
        }
        outcomes
    }

    /// Flushes the queue into the map of outcomes, then takes all outcomes from the map and
    /// returns them.
    pub fn drain(&self) -> HashMap<TaskId, Outcome<W>> {
        let mut outcomes: HashMap<TaskId, Outcome<W>> = self.outcomes.lock().drain().collect();
        // add any queued outcomes to the map
        while let Some(outcome) = self.queue.pop() {
            outcomes.insert(*outcome.task_id(), outcome);
        }
        outcomes
    }

    pub fn into_inner(self) -> HashMap<TaskId, Outcome<W>> {
        let mut outcomes = self.outcomes.into_inner();
        // add any queued outcomes to the map
        while let Some(outcome) = self.queue.pop() {
            outcomes.insert(*outcome.task_id(), outcome);
        }
        outcomes
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
