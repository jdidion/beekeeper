use super::{HiveInner, LocalQueue, Shared, Task};
use crate::bee::{Context, Queen, Worker};
use crossbeam_deque::Injector as GlobalQueue;
use std::sync::Arc;

type WorkerQueue<W> = crossbeam_deque::Worker<Task<W>>;

impl<W: Worker> LocalQueue<W> for WorkerQueue<W> {
    fn new<Q: Queen<Kind = W>>(shared: &Arc<Shared<W, Q, Self>>) -> Self
    where
        Self: Sized,
    {
        Self::new_fifo()
    }
}

pub struct WorkstealingHive<W: Worker, Q: Queen<Kind = W>>(Option<HiveInner<W, Q, WorkerQueue<W>>>);
