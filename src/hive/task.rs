use super::{OutcomeSender, Task};
use crate::task::{Context, Worker};

impl<W: Worker> Task<W> {
    pub fn new(input: W::Input, ctx: Context, outcome_tx: Option<OutcomeSender<W>>) -> Self {
        Task {
            input,
            ctx,
            outcome_tx,
        }
    }

    /// Returns the index of this task.
    pub fn index(&self) -> usize {
        self.ctx.index()
    }

    /// Consumes this `Task` and returns a tuple `(input, context, outcome_tx)`.
    pub fn into_parts(self) -> (W::Input, Context, Option<OutcomeSender<W>>) {
        (self.input, self.ctx, self.outcome_tx)
    }
}
