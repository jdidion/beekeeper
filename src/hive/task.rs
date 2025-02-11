use super::{Outcome, OutcomeSender, Task};
use crate::bee::{TaskId, Worker};

impl<W: Worker> Task<W> {
    /// Returns the ID of this task.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Consumes this `Task` and returns a `Outcome::Unprocessed` outcome with the input and ID,
    /// and the outcome sender.
    pub fn into_unprocessed(self) -> (Outcome<W>, Option<OutcomeSender<W>>) {
        let outcome = Outcome::Unprocessed {
            input: self.input,
            task_id: self.id,
        };
        (outcome, self.outcome_tx)
    }
}

#[cfg(not(feature = "retry"))]
impl<W: Worker> Task<W> {
    /// Creates a new `Task`.
    pub fn new(id: TaskId, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> Self {
        Task {
            id,
            input,
            outcome_tx,
        }
    }

    pub fn into_parts(self) -> (TaskId, W::Input, Option<OutcomeSender<W>>) {
        (self.id, self.input, self.outcome_tx)
    }
}

#[cfg(feature = "retry")]
impl<W: Worker> Task<W> {
    /// Creates a new `Task`.
    pub fn new(id: TaskId, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> Self {
        Task {
            id,
            input,
            outcome_tx,
            attempt: 0,
        }
    }

    /// Creates a new `Task`.
    pub fn with_attempt(
        id: TaskId,
        input: W::Input,
        outcome_tx: Option<OutcomeSender<W>>,
        attempt: u32,
    ) -> Self {
        Task {
            id,
            input,
            outcome_tx,
            attempt,
        }
    }

    pub fn into_parts(self) -> (TaskId, W::Input, u32, Option<OutcomeSender<W>>) {
        (self.id, self.input, self.attempt, self.outcome_tx)
    }
}
