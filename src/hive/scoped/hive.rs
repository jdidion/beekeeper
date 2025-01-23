use crate::{ApplyError, Panic};
use parking_lot::Mutex;
use std::{
    fmt::Debug,
    sync::{mpsc, Arc},
    thread,
};

pub type WorkerError<W> = ApplyError<<W as Worker>::Input, <W as Worker>::Error>;
pub type WorkerResult<W> = Result<<W as Worker>::Output, WorkerError<W>>;

pub trait Worker: Debug + Sized {
    type Input: Send;
    type Output: Send;
    type Error: Send + Debug;

    fn apply(&mut self, _: Self::Input, _: &Context) -> WorkerResult<Self>;
}

pub trait Queen: Send + Sync {
    type Kind: Worker;

    fn create(&mut self) -> Self::Kind;
}

pub struct Hive<W: Worker, Q: Queen<Kind = W>> {
    queen: Mutex<Q>,
    num_threads: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum HiveError<W: Worker> {
    #[error("Task failed")]
    Failed(W::Error),
    #[error("Task retried the maximum number of times")]
    MaxRetriesAttempted(W::Error),
    #[error("Task input was not processed")]
    Unprocessed(W::Input),
    #[error("Task panicked")]
    Panic(Panic<String>),
}

pub type HiveResult<T, W> = Result<T, HiveError<W>>;
pub type TaskResult<W> = HiveResult<<W as Worker>::Output, W>;

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome<W: Worker> {
    /// The task was executed successfully.
    Success { value: W::Output, task_id: TaskId },
    /// The task failed with an error that was not retryable.
    Failure { error: W::Error, task_id: TaskId },
    /// The task failed after retrying the maximum number of times.
    MaxRetriesAttempted { error: W::Error, task_id: TaskId },
    /// The task was not executed before the Hive was closed.
    Unprocessed { value: W::Input, task_id: TaskId },
    /// The task panicked.
    Panic {
        payload: Panic<String>,
        task_id: TaskId,
    },
}

impl<W: Worker> Outcome<W> {
    /// Returns the ID of the task that produced this outcome.
    pub fn task_id(&self) -> TaskId {
        match self {
            Outcome::Success { task_id, .. }
            | Outcome::Failure { task_id, .. }
            | Outcome::MaxRetriesAttempted { task_id, .. }
            | Outcome::Unprocessed { task_id, .. }
            | Outcome::Panic { task_id, .. } => *task_id,
        }
    }

    /// Creates a new `Outcome` from a `Panic`.
    pub fn from_panic(payload: Panic<String>, task_id: TaskId) -> Outcome<W> {
        Outcome::Panic { payload, task_id }
    }

    pub(crate) fn from_panic_result(
        result: Result<WorkerResult<W>, Panic<String>>,
        task_id: TaskId,
    ) -> Outcome<W> {
        match result {
            Ok(result) => Outcome::from_worker_result(result, task_id),
            Err(panic) => Outcome::from_panic(panic, task_id),
        }
    }

    pub(crate) fn from_worker_result(result: WorkerResult<W>, task_id: TaskId) -> Outcome<W> {
        match result {
            Ok(value) => Self::Success { task_id, value },
            Err(ApplyError::Cancelled { input } | ApplyError::Retryable { input, .. }) => {
                Self::Unprocessed {
                    value: input,
                    task_id,
                }
            }
            Err(ApplyError::Fatal(error)) => Self::Failure { error, task_id },
        }
    }
}

/// Context for a task.
#[derive(Debug, Default)]
pub struct Context {
    task_id: TaskId,
    attempt: u32,
}

impl Context {
    fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            attempt: 0,
        }
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
    pub fn map(&self, inputs: impl IntoIterator<Item = W::Input>) {
        //-> impl Iterator<Item = TaskResult<W>> {
        let (task_tx, task_rx) = mpsc::channel();
        let task_rx = Arc::new(Mutex::new(task_rx));
        let (outcome_tx, outcome_rx) = crate::outcome_channel();
        thread::scope(|scope| {
            let join_handles = (0..self.num_threads)
                .map(|task_id| {
                    let task_rx = task_rx.clone();
                    let outcome_tx = outcome_tx.clone();
                    scope.spawn(move || loop {
                        let mut worker = self.queen.lock().create();
                        if let Ok(input) = task_rx.lock().recv() {
                            let ctx = Context::new(task_id);
                            let result: Result<WorkerResult<W>, Panic<String>> =
                                Panic::try_call(None, || worker.apply(input, &ctx));
                            let outcome: Outcome<W> = Outcome::from_panic_result(result, task_id);
                            outcome_tx.send(outcome);
                        } else {
                            break;
                        }
                    })
                })
                .collect::<Vec<_>>();
        });
        // let num_tasks = inputs
        //     .into_iter()
        //     .map(|input| self.apply_send(job, tx.clone()))
        //     .count();
        // rx.into_iter().take(num_tasks).map(Outcome::into_result)
    }
}
