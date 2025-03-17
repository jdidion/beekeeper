//! The context for a task processed by a `Worker`.
use std::cell::RefCell;
use std::fmt::Debug;

/// Type of unique ID for a task within the `Hive`.
pub type TaskId = usize;

/// Trait that provides a `Context` with limited access to a worker thread's state during
/// task execution.
pub trait LocalContext<I>: Debug {
    /// Returns `true` if tasks in progress should be cancelled.
    fn should_cancel_tasks(&self) -> bool;

    /// Submits a new task to the `Hive` that is executing the current task.
    fn submit_task(&self, input: I) -> TaskId;

    #[cfg(test)]
    fn thread_index(&self) -> usize;
}

/// The context visible to a task when processing an input.
#[derive(Debug)]
pub struct Context<'a, I> {
    meta: TaskMeta,
    local: Option<&'a dyn LocalContext<I>>,
    subtask_ids: RefCell<Option<Vec<TaskId>>>,
}

impl<'a, I> Context<'a, I> {
    /// Returns a new empty context. This is primarily useful for testing.
    pub fn empty() -> Self {
        Self {
            meta: TaskMeta::default(),
            local: None,
            subtask_ids: RefCell::new(None),
        }
    }

    /// Creates a new `Context` with the given task metadata and shared state.
    pub fn new(meta: TaskMeta, local: Option<&'a dyn LocalContext<I>>) -> Self {
        Self {
            meta,
            local,
            subtask_ids: RefCell::new(None),
        }
    }

    /// The unique ID of this task within the `Hive`.
    pub fn task_id(&self) -> TaskId {
        self.meta.id()
    }

    /// Returns the number of previous failed attempts to execute the current task.
    pub fn attempt(&self) -> u8 {
        self.meta.attempt()
    }

    /// Returns `true` if the current task should be cancelled.
    ///
    /// A long-running `Worker` should check this periodically and, if it returns `true`, exit
    /// early with an `ApplyError::Cancelled` result.
    pub fn is_cancelled(&self) -> bool {
        self.local
            .as_ref()
            .map(|local| local.should_cancel_tasks())
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
        if let Some(local) = self.local.as_ref() {
            let task_id = local.submit_task(input);
            self.subtask_ids
                .borrow_mut()
                .get_or_insert_default()
                .push(task_id);
            Ok(())
        } else {
            Err(input)
        }
    }

    /// Returns the unique index of the worker thread executing this task.
    #[cfg(test)]
    pub fn thread_index(&self) -> Option<usize> {
        self.local.map(|local| local.thread_index())
    }

    /// Consumes this `Context` and returns the IDs of the subtasks spawned during the execution
    /// of the task, if any.
    pub(crate) fn into_parts(self) -> (TaskMeta, Option<Vec<TaskId>>) {
        (self.meta, self.subtask_ids.into_inner())
    }
}

/// The metadata of a task.
#[derive(Clone, Debug, Default)]
pub struct TaskMeta {
    id: TaskId,
    #[cfg(feature = "local-batch")]
    weight: u32,
    #[cfg(feature = "retry")]
    attempt: u8,
}

impl TaskMeta {
    /// Creates a new `TaskMeta` with the given task ID.
    pub fn new(id: TaskId) -> Self {
        TaskMeta {
            id,
            ..Default::default()
        }
    }

    /// Creates a new `TaskMeta` with the given task ID and weight.
    #[cfg(feature = "local-batch")]
    pub fn with_weight(task_id: TaskId, weight: u32) -> Self {
        TaskMeta {
            id: task_id,
            weight,
            ..Default::default()
        }
    }

    /// Returns the unique ID of this task within the `Hive`.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Returns the number of previous failed attempts to execute the current task.
    ///
    /// Always returns `0` if the `retry` feature is not enabled.
    pub fn attempt(&self) -> u8 {
        #[cfg(feature = "retry")]
        return self.attempt;
        #[cfg(not(feature = "retry"))]
        return 0;
    }

    /// Increments the number of previous failed attempts to execute the current task.
    #[cfg(feature = "retry")]
    pub(crate) fn inc_attempt(&mut self) {
        self.attempt += 1;
    }

    /// Returns the task weight.
    ///
    /// Always returns `0` if the `local-batch` feature is not enabled.
    pub fn weight(&self) -> u32 {
        #[cfg(feature = "local-batch")]
        return self.weight;
        #[cfg(not(feature = "local-batch"))]
        return 0;
    }
}

impl From<TaskId> for TaskMeta {
    fn from(value: TaskId) -> Self {
        TaskMeta::new(value)
    }
}

#[cfg(all(test, feature = "retry"))]
impl TaskMeta {
    pub fn with_attempt(task_id: TaskId, attempt: u8) -> Self {
        Self {
            id: task_id,
            attempt,
            ..Default::default()
        }
    }
}
