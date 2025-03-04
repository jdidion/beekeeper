use super::{DerefOutcomes, Outcome, OwnedOutcomes};
use crate::bee::{TaskId, Worker};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

/// A batch of `Outcome`s.
pub struct OutcomeBatch<W: Worker>(HashMap<TaskId, Outcome<W>>);

impl<W: Worker> OutcomeBatch<W> {
    pub(crate) fn new(outcomes: HashMap<TaskId, Outcome<W>>) -> Self {
        Self(outcomes)
    }
}

impl<W: Worker, I: IntoIterator<Item = Outcome<W>>> From<I> for OutcomeBatch<W> {
    fn from(value: I) -> Self {
        OutcomeBatch::new(
            value
                .into_iter()
                .map(|outcome| (*outcome.task_id(), outcome))
                .collect(),
        )
    }
}

impl<W: Worker> OwnedOutcomes<W> for OutcomeBatch<W> {
    #[inline]
    fn outcomes(self) -> HashMap<TaskId, Outcome<W>> {
        self.0
    }

    #[inline]
    fn outcomes_ref(&self) -> &HashMap<TaskId, Outcome<W>> {
        &self.0
    }
}

impl<W: Worker> DerefOutcomes<W> for OutcomeBatch<W> {
    #[inline]
    fn outcomes_deref(&self) -> impl Deref<Target = HashMap<TaskId, Outcome<W>>> {
        &self.0
    }

    #[inline]
    fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> {
        &mut self.0
    }
}

/// Functions only used in testing.
#[cfg(test)]
impl<W: Worker> OutcomeBatch<W> {
    pub(crate) fn empty() -> Self {
        OutcomeBatch::new(HashMap::new())
    }

    pub(crate) fn insert(&mut self, outcome: Outcome<W>) {
        self.0.insert(*outcome.task_id(), outcome);
    }
}
