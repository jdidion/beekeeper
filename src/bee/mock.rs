use super::{Context, LocalContext, TaskId, TaskMeta, Worker, WorkerResult};
use std::cell::RefCell;

/// Applies the given `worker` to the given `input` using the given `task_meta`.
///
/// Returns a tuple of the apply result, the (possibly modified) task metadata, and the IDs of any
/// subtasks that were submitted.
pub fn apply<W: Worker>(
    input: W::Input,
    task_meta: TaskMeta,
    worker: &mut W,
) -> (WorkerResult<W>, TaskMeta, Option<Vec<TaskId>>) {
    let local = MockLocalContext::new(task_meta.id());
    let ctx = Context::new(task_meta, Some(&local));
    let result = worker.apply(input, &ctx);
    let (task_meta, subtask_ids) = ctx.into_parts();
    (result, task_meta, subtask_ids)
}

#[derive(Debug, Default)]
pub struct MockLocalContext(RefCell<TaskId>);

impl MockLocalContext {
    pub fn new(task_id: TaskId) -> Self {
        Self(RefCell::new(task_id))
    }

    pub fn into_task_count(self) -> usize {
        self.0.into_inner()
    }
}

impl<I> LocalContext<I> for MockLocalContext {
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
