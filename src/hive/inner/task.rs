use super::Task;
use crate::bee::{TaskId, TaskMeta, Worker};
use crate::hive::{Outcome, OutcomeSender};

pub use task_impl::TaskInput;

impl<W: Worker> Task<W> {
    /// Creates a new `Task` with the given metadata.
    pub fn with_meta(
        input: W::Input,
        meta: TaskMeta,
        outcome_tx: Option<OutcomeSender<W>>,
    ) -> Self {
        Task {
            input,
            meta,
            outcome_tx,
        }
    }

    #[inline]
    pub fn id(&self) -> TaskId {
        self.meta.id()
    }

    /// Returns a reference to the task metadata.
    #[inline]
    pub fn meta(&self) -> &TaskMeta {
        &self.meta
    }

    /// Consumes this `Task` and returns its input, metadata, and outcome sender.
    pub fn into_parts(self) -> (W::Input, TaskMeta, Option<OutcomeSender<W>>) {
        (self.input, self.meta, self.outcome_tx)
    }

    /// Consumes this `Task` and returns a `Outcome::Unprocessed` outcome with the input and ID,
    /// and the outcome sender.
    pub fn into_unprocessed(self) -> (Outcome<W>, Option<OutcomeSender<W>>) {
        let outcome = Outcome::Unprocessed {
            input: self.input,
            task_id: self.meta.id(),
        };
        (outcome, self.outcome_tx)
    }
}

#[cfg(not(feature = "local-batch"))]
mod task_impl {
    use super::Task;
    use crate::bee::{TaskId, TaskMeta, Worker};
    use crate::hive::OutcomeSender;

    pub type TaskInput<W> = <W as Worker>::Input;

    impl<W: Worker> Task<W> {
        /// Creates a new `Task` with the given `task_id`.
        pub fn new(
            task_id: TaskId,
            input: TaskInput<W>,
            outcome_tx: Option<OutcomeSender<W>>,
        ) -> Self {
            Task {
                input,
                meta: TaskMeta::new(task_id),
                outcome_tx,
            }
        }
    }
}

#[cfg(feature = "local-batch")]
mod task_impl {
    use super::Task;
    use crate::bee::{TaskId, TaskMeta, Worker};
    use crate::hive::{Outcome, OutcomeSender, Weighted};

    pub type TaskInput<W> = Weighted<<W as Worker>::Input>;

    impl<W: Worker> Task<W> {
        /// Creates a new `Task` with the given `task_id`.
        pub fn new(
            task_id: TaskId,
            input: TaskInput<W>,
            outcome_tx: Option<OutcomeSender<W>>,
        ) -> Self {
            let (input, weight) = input.into_parts();
            Task {
                input,
                meta: TaskMeta::with_weight(task_id, weight),
                outcome_tx,
            }
        }

        /// Consumes this `Task` and returns a `Outcome::WeightLimitExceeded` outcome with the input,
        /// weight, and ID, and the outcome sender.
        pub fn into_overweight(self) -> (Outcome<W>, Option<OutcomeSender<W>>) {
            let outcome = Outcome::WeightLimitExceeded {
                input: self.input,
                weight: self.meta.weight(),
                task_id: self.meta.id(),
            };
            (outcome, self.outcome_tx)
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use super::Task;
    use crate::bee::{TaskMeta, Worker};
    use crate::hive::OutcomeSender;

    impl<W: Worker> Task<W> {
        /// Creates a new `Task`.
        pub fn with_meta_inc_attempt(
            input: W::Input,
            mut meta: TaskMeta,
            outcome_tx: Option<OutcomeSender<W>>,
        ) -> Self {
            meta.inc_attempt();
            Self {
                input,
                meta,
                outcome_tx,
            }
        }
    }
}

impl<I: Clone, W: Worker<Input = I>> Clone for Task<W> {
    fn clone(&self) -> Self {
        Self {
            input: self.input.clone(),
            meta: self.meta.clone(),
            outcome_tx: self.outcome_tx.clone(),
        }
    }
}

impl<W: Worker> PartialEq for Task<W> {
    fn eq(&self, other: &Self) -> bool {
        self.meta.id() == other.meta.id()
    }
}

impl<W: Worker> Eq for Task<W> {}

impl<W: Worker> PartialOrd for Task<W> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<W: Worker> Ord for Task<W> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.meta.id().cmp(&other.meta.id())
    }
}
