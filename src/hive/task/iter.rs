use crate::bee::{Queen, Worker};
use crate::hive::counter::CounterError;
use crate::hive::{LocalQueues, Shared, Task};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum NextTaskError {
    #[error("Task receiver disconnected")]
    Disconnected,
    #[error("The hive has been poisoned")]
    Poisoned,
    #[error("Task counter has invalid state")]
    InvalidCounter(CounterError),
}

pub struct TaskIterator<W: Worker, Q: Queen<Kind = W>, L: LocalQueues<W>> {
    thread_index: usize,
    shared: Arc<Shared<W, Q, L>>,
}

impl<W: Worker, Q: Queen<Kind = W>, L: LocalQueues<W>> Iterator for TaskIterator<W, Q, L> {
    type Item = Task<W>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
