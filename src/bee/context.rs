//! The context for a task processed by a `Worker`.
use std::cell::RefCell;
use std::fmt::Debug;

pub type TaskId = usize;

/// Trait that provides a `Context` with limited access to a worker thread's state during
/// task execution.
pub trait TaskContext<I>: Debug {
    /// Returns `true` if tasks in progress should be cancelled.
    fn should_cancel_tasks(&self) -> bool;

    /// Submits a new task to the `Hive` that is executing the current task.
    fn submit_task(&self, input: I) -> TaskId;
}

#[derive(Debug)]
pub struct Context<'a, I> {
    task_id: TaskId,
    task_ctx: Option<&'a dyn TaskContext<I>>,
    subtask_ids: RefCell<Option<Vec<TaskId>>>,
    #[cfg(feature = "retry")]
    attempt: u32,
}

impl<I> Context<'_, I> {
    /// The unique ID of this task within the `Hive`.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns `true` if the current task should be cancelled.
    ///
    /// A long-running `Worker` should check this periodically and, if it returns `true`, exit
    /// early with an `ApplyError::Cancelled` result.
    pub fn is_cancelled(&self) -> bool {
        self.task_ctx
            .as_ref()
            .map(|worker| worker.should_cancel_tasks())
            .unwrap_or(false)
    }

    /// Submits a new task to the `Hive` that is executing the current task.
    ///
    /// If a thread-local queue is available and has capacity, the task will be added to it,
    /// otherwise it is added to the global queue. The ID of the submitted task is stored in this
    /// `Context` and ultimately returned in the `subtask_ids` of the `Outcome` of the submitting
    /// task.
    ///
    /// The task will be submitted with the same outcome sender as the current task, or stored in
    /// the `Hive` if there is no sender.
    ///
    /// Returns an `Err` containing `input` if the new task was not successfully submitted.
    pub fn submit(&self, input: I) -> Result<(), I> {
        if let Some(worker) = self.task_ctx.as_ref() {
            let task_id = worker.submit_task(input);
            self.subtask_ids
                .borrow_mut()
                .get_or_insert_default()
                .push(task_id);
            Ok(())
        } else {
            Err(input)
        }
    }

    /// Consumes this `Context` and returns the IDs of the subtasks spawned during the execution
    /// of the task, if any.
    pub(crate) fn into_subtask_ids(self) -> Option<Vec<TaskId>> {
        self.subtask_ids.into_inner()
    }
}

#[cfg(not(feature = "retry"))]
impl<'a, I> Context<'a, I> {
    /// Returns a new empty context. This is primarily useful for testing.
    pub fn empty() -> Self {
        Self {
            task_id: 0,
            task_ctx: None,
            subtask_ids: None,
        }
    }

    /// Creates a new `Context` with the given task_id and shared cancellation status.
    pub fn new(task_id: TaskId, task_ctx: Option<&'a dyn TaskContext<I>>) -> Self {
        Self {
            task_id,
            task_ctx,
            subtask_ids: None,
        }
    }

    /// The number of previous failed attempts to execute the current task.
    ///
    /// Always returns `0`.
    pub fn attempt(&self) -> u32 {
        0
    }
}

#[cfg(feature = "retry")]
impl<'a, I> Context<'a, I> {
    /// Returns a new empty context. This is primarily useful for testing.
    pub fn empty() -> Self {
        Self {
            task_id: 0,
            attempt: 0,
            task_ctx: None,
            subtask_ids: RefCell::new(None),
        }
    }

    /// Creates a new `Context` with the given task_id and shared cancellation status.
    pub fn new(task_id: TaskId, attempt: u32, task_ctx: Option<&'a dyn TaskContext<I>>) -> Self {
        Self {
            task_id,
            attempt,
            task_ctx,
            subtask_ids: RefCell::new(None),
        }
    }

    /// The number of previous attempts to execute the current task.
    ///
    /// Returns `0` for the first attempt and increments by `1` for each retry attempt (if any).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

#[cfg(test)]
pub mod mock {
    use super::{TaskContext, TaskId};
    use std::cell::RefCell;

    #[derive(Debug, Default)]
    pub struct MockTaskContext(RefCell<TaskId>);

    impl<I> TaskContext<I> for MockTaskContext {
        fn should_cancel_tasks(&self) -> bool {
            false
        }

        fn submit_task(&self, _: I) -> super::TaskId {
            let mut task_id = self.0.borrow_mut();
            let cur_id = *task_id;
            *task_id += 1;
            cur_id
        }
    }
}
