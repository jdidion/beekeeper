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
    Success { value: W::Output, index: usize },
    /// The task failed with an error that was not retryable.
    Failure { error: W::Error, index: usize },
    /// The task failed after retrying the maximum number of times.
    MaxRetriesAttempted { error: W::Error, index: usize },
    /// The task was not executed before the Hive was closed.
    Unprocessed { value: W::Input, index: usize },
    /// The task panicked.
    Panic {
        payload: Panic<String>,
        index: usize,
    },
}

impl<W: Worker> Outcome<W> {
    /// Returns the index of the task that produced this outcome.
    pub fn index(&self) -> usize {
        match self {
            Outcome::Success { index, .. }
            | Outcome::Failure { index, .. }
            | Outcome::MaxRetriesAttempted { index, .. }
            | Outcome::Unprocessed { index, .. }
            | Outcome::Panic { index, .. } => *index,
        }
    }

    /// Creates a new `Outcome` from a `Panic`.
    pub fn from_panic(payload: Panic<String>, index: usize) -> Outcome<W> {
        Outcome::Panic { payload, index }
    }

    pub(crate) fn from_panic_result(
        result: Result<WorkerResult<W>, Panic<String>>,
        index: usize,
    ) -> Outcome<W> {
        match result {
            Ok(result) => Outcome::from_worker_result(result, index),
            Err(panic) => Outcome::from_panic(panic, index),
        }
    }

    pub(crate) fn from_worker_result(result: WorkerResult<W>, index: usize) -> Outcome<W> {
        match result {
            Ok(value) => Self::Success { index, value },
            Err(ApplyError::Cancelled { input } | ApplyError::Retryable { input, .. }) => {
                Self::Unprocessed {
                    value: input,
                    index,
                }
            }
            Err(ApplyError::NotRetryable(error)) => Self::Failure { error, index },
        }
    }
}

/// Context for a task.
#[derive(Debug, Default)]
pub struct Context {
    index: usize,
    attempt: u32,
}

impl Context {
    fn new(index: usize) -> Self {
        Self { index, attempt: 0 }
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
                .map(|index| {
                    let task_rx = task_rx.clone();
                    let outcome_tx = outcome_tx.clone();
                    scope.spawn(move || loop {
                        let mut worker = self.queen.lock().create();
                        if let Ok(input) = task_rx.lock().recv() {
                            let ctx = Context::new(index);
                            let result: Result<WorkerResult<W>, Panic<String>> =
                                Panic::try_call(None, || worker.apply(input, &ctx));
                            let outcome: Outcome<W> = Outcome::from_panic_result(result, index);
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
