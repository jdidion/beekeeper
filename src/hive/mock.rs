use super::{Task, TaskInput};
use crate::bee::{Context, LocalContext, TaskId, TaskMeta, Worker, WorkerResult};
use std::cell::RefCell;

#[derive(Debug)]
pub struct MockTaskRunner(RefCell<TaskId>);

impl MockTaskRunner {
    pub fn new() -> Self {
        Self(RefCell::new(0))
    }

    /// Applies the given `worker` to the given `input`.
    ///
    /// Returns a tuple of the apply result, the task metadata, and the IDs of any
    /// subtasks that were submitted.
    pub fn apply<W: Worker>(
        &self,
        input: TaskInput<W>,
        worker: &mut W,
    ) -> (WorkerResult<W>, TaskMeta, Option<Vec<TaskId>>) {
        let task_id = self.next_task_id();
        let local = MockLocalContext(&self);
        let task: Task<W> = Task::new(task_id, input, None);
        let (input, task_meta, _) = task.into_parts();
        let ctx = Context::new(task_meta, Some(&local));
        let result = worker.apply(input, &ctx);
        let (task_meta, subtask_ids) = ctx.into_parts();
        (result, task_meta, subtask_ids)
    }

    fn next_task_id(&self) -> TaskId {
        let mut task_id_counter = self.0.borrow_mut();
        let task_id = *task_id_counter;
        *task_id_counter += 1;
        task_id
    }
}

#[derive(Debug)]
struct MockLocalContext<'a>(&'a MockTaskRunner);

impl<'a, I> LocalContext<I> for MockLocalContext<'a> {
    fn should_cancel_tasks(&self) -> bool {
        false
    }

    fn submit_task(&self, _: I) -> TaskId {
        self.0.next_task_id()
    }
}
