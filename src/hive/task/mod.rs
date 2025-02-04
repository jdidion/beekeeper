#[cfg(feature = "retry")]
mod delay;
mod iter;
mod local;

pub use local::LocalQueuesImpl;

use super::{Outcome, OutcomeSender, Task};
use crate::bee::{Context, TaskId, Worker};

impl<W: Worker> Task<W> {
    /// Creates a new `Task`.
    pub fn new(input: W::Input, ctx: Context, outcome_tx: Option<OutcomeSender<W>>) -> Self {
        Task {
            input,
            ctx,
            outcome_tx,
        }
    }

    /// Returns the ID of this task.
    pub fn id(&self) -> TaskId {
        self.ctx.task_id()
    }

    /// Consumes this `Task` and returns a tuple `(input, context, outcome_tx)`.
    pub fn into_parts(self) -> (W::Input, Context, Option<OutcomeSender<W>>) {
        (self.input, self.ctx, self.outcome_tx)
    }

    /// Consumes this `Task` and returns a `Outcome::Unprocessed` outcome with the input and ID,
    /// and the outcome sender.
    pub fn into_unprocessed(self) -> (Outcome<W>, Option<OutcomeSender<W>>) {
        let (input, ctx, outcome_tx) = self.into_parts();
        let outcome = Outcome::Unprocessed {
            input,
            task_id: ctx.task_id(),
        };
        (outcome, outcome_tx)
    }
}
