use super::{Outcome, OutcomeSender, Task};
use crate::bee::{Context, TaskId, Worker};
use crate::channel::SenderExt;

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

    /// Consumes this `Task`, converts it into a `Outcome::Unprocessed`, and attempts to send it to
    /// the `OutcomeSender` if there is one. Returns `None` if the send succeeds, or the `Outcome`
    /// if there is no sender or the send fails.
    pub fn into_unprocessed_try_send(self) -> Option<Outcome<W>> {
        let (outcome, outcome_tx) = self.into_unprocessed();
        if let Some(tx) = outcome_tx {
            tx.try_send_msg(outcome)
        } else {
            Some(outcome)
        }
    }
}
