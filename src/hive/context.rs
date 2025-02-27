use crate::bee::{LocalContext, Queen, TaskId, Worker};
use crate::hive::{OutcomeSender, Shared, TaskQueues, WorkerQueues};
use std::fmt;
use std::sync::Arc;

pub struct HiveLocalContext<'a, W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    worker_queues: &'a T::WorkerQueues,
    shared: &'a Arc<Shared<Q, T>>,
    outcome_tx: Option<&'a OutcomeSender<W>>,
}

impl<'a, W, Q, T> HiveLocalContext<'a, W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    pub fn new(
        worker_queues: &'a T::WorkerQueues,
        shared: &'a Arc<Shared<Q, T>>,
        outcome_tx: Option<&'a OutcomeSender<W>>,
    ) -> Self {
        Self {
            worker_queues,
            shared,
            outcome_tx,
        }
    }
}

impl<W, Q, T> LocalContext<W::Input> for HiveLocalContext<'_, W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn should_cancel_tasks(&self) -> bool {
        self.shared.is_suspended()
    }

    fn submit_task(&self, input: W::Input) -> TaskId {
        let task = self.shared.prepare_task(input, self.outcome_tx);
        let task_id = task.id();
        self.worker_queues.push(task);
        task_id
    }
}

impl<W, Q, T> fmt::Debug for HiveLocalContext<'_, W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveLocalContext").finish()
    }
}
