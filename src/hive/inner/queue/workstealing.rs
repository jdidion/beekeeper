use super::{GlobalTaskQueue, LocalTaskQueues, Task};
use crate::bee::{Queen, Worker};
use crate::hive::Shared;
use crossbeam_deque::{Injector, Stealer, Worker as LocalQueue};
use std::marker::PhantomData;

struct GlobalQueue<W: Worker> {
    queue: Injector<Task<W>>,
    _worker: PhantomData<W>,
}

impl<W: Worker> GlobalTaskQueue<W> for GlobalQueue<W> {
    fn try_push(&self, task: Task<W>) -> Result<(), Task<W>> {
        self.queue.push(task);
    }

    fn try_pop(&self) -> Result<Task<W>, super::PopTaskError> {
        todo!()
    }

    fn try_iter(&self) -> impl Iterator<Item = Task<W>> + '_ {
        todo!()
    }

    fn drain(&self) -> Vec<Task<W>> {
        todo!()
    }

    fn close(&self) {
        todo!()
    }
}

struct WorkerQueue {}

impl<W: Worker> LocalTaskQueues<W, GlobalQueue<W>> for WorkerQueue<W> {}
