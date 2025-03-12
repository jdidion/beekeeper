//! Utilities for testing `Worker`s.
use super::{Outcome, Task, TaskInput};
use crate::bee::{Context, LocalContext, TaskId, Worker};
use std::cell::RefCell;

/// A struct used for testing `Worker`s in a mock environment without needing to create a `Hive`.
#[derive(Debug)]
pub struct MockTaskRunner<W: Worker> {
    worker: RefCell<W>,
    task_id: RefCell<TaskId>,
}

impl<W: Worker> MockTaskRunner<W> {
    /// Creates a new `MockTaskRunner` with a starting task ID of 0.
    pub fn new(worker: W, first_task_id: TaskId) -> Self {
        Self {
            worker: RefCell::new(worker),
            task_id: RefCell::new(first_task_id),
        }
    }

    /// Applies the given `worker` to the given `input`.
    ///
    /// The task ID is automatically incremented and used to create the `Context`.
    ///
    /// Returns the `Outcome` from executing the task.
    pub fn apply<I: Into<TaskInput<W>>>(&self, input: I) -> Outcome<W> {
        let task_id = self.next_task_id();
        let local = MockLocalContext(self);
        let task: Task<W> = Task::new(task_id, input.into(), None);
        let (input, task_meta, _) = task.into_parts();
        let ctx = Context::new(task_meta, Some(&local));
        let result = self.worker.borrow_mut().apply(input, &ctx);
        let (task_meta, subtask_ids) = ctx.into_parts();
        Outcome::from_worker_result(result, task_meta, subtask_ids)
    }

    fn next_task_id(&self) -> TaskId {
        let mut task_id_counter = self.task_id.borrow_mut();
        let task_id = *task_id_counter;
        *task_id_counter += 1;
        task_id
    }
}

impl<W: Worker> From<W> for MockTaskRunner<W> {
    fn from(value: W) -> Self {
        Self::new(value, 0)
    }
}

impl<W: Worker + Default> Default for MockTaskRunner<W> {
    fn default() -> Self {
        Self::from(W::default())
    }
}

#[derive(Debug)]
struct MockLocalContext<'a, W: Worker>(&'a MockTaskRunner<W>);

impl<W, I> LocalContext<I> for MockLocalContext<'_, W>
where
    W: Worker,
    I: Into<TaskInput<W>>,
{
    fn should_cancel_tasks(&self) -> bool {
        false
    }

    fn submit_task(&self, _: I) -> TaskId {
        self.0.next_task_id()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::vec;

    use super::MockTaskRunner;
    use crate::bee::{Context, Worker, WorkerResult};
    use crate::hive::Outcome;

    #[derive(Debug, Default)]
    struct TestWorker;

    impl Worker for TestWorker {
        type Input = usize;
        type Output = usize;
        type Error = ();

        fn apply(&mut self, input: Self::Input, ctx: &Context<Self::Input>) -> WorkerResult<Self> {
            if !ctx.is_cancelled() {
                for i in 1..=3 {
                    ctx.submit(input + i).unwrap();
                }
            }
            Ok(input)
        }
    }

    #[test]
    fn test_works() {
        let runner = MockTaskRunner::<TestWorker>::default();
        let outcome = runner.apply(42);
        assert!(matches!(
            outcome,
            Outcome::SuccessWithSubtasks {
                value: 42,
                task_id: 0,
                ..
            }
        ));
        assert_eq!(outcome.subtask_ids(), Some(&vec![1, 2, 3]))
    }
}
