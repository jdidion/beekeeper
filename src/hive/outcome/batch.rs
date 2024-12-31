use super::{Outcome, OutcomeDerefStore, OutcomeStore, Outcomes, OutcomesDeref};
use crate::task::Worker;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

/// A batch of `Outcome`s.
pub struct OutcomeBatch<W: Worker>(HashMap<usize, Outcome<W>>);

impl<W: Worker> OutcomeBatch<W> {
    pub(crate) fn new(outcomes: HashMap<usize, Outcome<W>>) -> Self {
        Self(outcomes)
    }
}

impl<W: Worker, I: IntoIterator<Item = Outcome<W>>> From<I> for OutcomeBatch<W> {
    fn from(value: I) -> Self {
        OutcomeBatch::new(
            value
                .into_iter()
                .map(|outcome| (outcome.index(), outcome))
                .collect(),
        )
    }
}

impl<W: Worker> Outcomes<W> for OutcomeBatch<W> {
    fn outcomes(self) -> HashMap<usize, Outcome<W>> {
        self.0
    }

    fn outcomes_ref(&self) -> &HashMap<usize, Outcome<W>> {
        &self.0
    }
}

impl<W: Worker> OutcomesDeref<W> for OutcomeBatch<W> {
    fn outcomes_deref(&self) -> impl Deref<Target = HashMap<usize, Outcome<W>>> {
        &self.0
    }

    fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<usize, Outcome<W>>> {
        &mut self.0
    }
}

impl<W: Worker> OutcomeStore<W> for OutcomeBatch<W> {}

impl<W: Worker> OutcomeDerefStore<W> for OutcomeBatch<W> {}

#[cfg(test)]
impl<W: Worker> OutcomeBatch<W> {
    pub(crate) fn empty() -> Self {
        OutcomeBatch::new(HashMap::new())
    }

    pub(crate) fn insert(&mut self, outcome: Outcome<W>) {
        self.0.insert(outcome.index(), outcome);
    }
}
