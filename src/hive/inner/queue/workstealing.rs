//! Implementation of `TaskQueues` that uses workstealing to distribute tasks among worker threads.
//! Tasks are sent from the `Hive` via a global `Injector` queue. Each worker thread has a local
//! `Worker` queue where tasks can be pushed. If the local queue is empty, the worker thread first
//! tries to steal a task from the global queue and falls back to stealing from another worker
//! thread. If the `batching` feature is enabled, a worker thread will try to fill its local queue
//! up to the limit when stealing from the global queue.
use super::{Config, PopTaskError, Status, Task, TaskQueues, Token, WorkerQueues};
#[cfg(feature = "batching")]
use crate::atomic::Atomic;
use crate::bee::Worker;
use crossbeam_deque::{Injector, Stealer};
use crossbeam_queue::SegQueue;
use parking_lot::RwLock;
use rand::prelude::*;
use std::ops::Deref;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Time to wait after trying to pop and finding all queues empty.
const EMPTY_DELAY: Duration = Duration::from_millis(100);

pub struct WorkstealingTaskQueues<W: Worker> {
    global: Arc<GlobalQueue<W>>,
    local: RwLock<Vec<Arc<LocalQueueShared<W>>>>,
}

impl<W: Worker> TaskQueues<W> for WorkstealingTaskQueues<W> {
    type WorkerQueues = WorkstealingWorkerQueues<W>;

    fn new(_: Token) -> Self {
        Self {
            global: Arc::new(GlobalQueue::new()),
            local: Default::default(),
        }
    }

    fn init_for_threads(&self, start_index: usize, end_index: usize, config: &Config) {
        let mut local_queues = self.local.write();
        assert_eq!(local_queues.len(), start_index);
        (start_index..end_index).for_each(|thread_index| {
            local_queues.push(Arc::new(LocalQueueShared::new(thread_index, config)));
        });
    }

    fn update_for_threads(&self, start_index: usize, end_index: usize, config: &Config) {
        let local_queues = self.local.read();
        assert!(local_queues.len() >= end_index);
        (start_index..end_index).for_each(|thread_index| local_queues[thread_index].update(config));
    }

    fn worker_queues(&self, thread_index: usize) -> Self::WorkerQueues {
        let local_queue = crossbeam_deque::Worker::new_fifo();
        self.global.add_stealer(local_queue.stealer());
        WorkstealingWorkerQueues::new(local_queue, &self.global, &self.local.read()[thread_index])
    }

    fn try_push_global(&self, task: Task<W>) -> Result<(), Task<W>> {
        self.global.try_push(task)
    }

    fn close(&self, urgent: bool, _: Token) {
        self.global.close(urgent);
    }

    fn drain(self) -> Vec<Task<W>> {
        if !self.global.is_closed() {
            panic!("close must be called before drain");
        }
        let mut tasks = Vec::new();
        let global = crate::hive::util::unwrap_arc(self.global)
            .unwrap_or_else(|_| panic!("timeout waiting to take ownership of global queue"));
        global.drain_into(&mut tasks);
        for local in self.local.into_inner().into_iter() {
            let local = crate::hive::util::unwrap_arc(local)
                .unwrap_or_else(|_| panic!("timeout waiting to take ownership of local queue"));
            local.drain_into(&mut tasks);
        }
        tasks
    }
}

pub struct GlobalQueue<W: Worker> {
    queue: Injector<Task<W>>,
    stealers: RwLock<Vec<Stealer<Task<W>>>>,
    status: Status,
}

impl<W: Worker> GlobalQueue<W> {
    fn new() -> Self {
        Self {
            queue: Injector::new(),
            stealers: Default::default(),
            status: Default::default(),
        }
    }

    fn add_stealer(&self, stealer: Stealer<Task<W>>) {
        self.stealers.write().push(stealer);
    }

    fn try_push(&self, task: Task<W>) -> Result<(), Task<W>> {
        if !self.status.can_push() {
            return Err(task);
        }
        self.queue.push(task);
        Ok(())
    }

    /// Tries to steal a task from a random worker using its `Stealer`.
    fn try_steal_from_worker(&self) -> Result<Task<W>, PopTaskError> {
        let stealers = self.stealers.read();
        let n = stealers.len();
        // randomize the stealing order, to prevent always stealing from the same thread
        std::iter::from_fn(|| Some(rand::rng().random_range(0..n)))
            .take(n)
            .filter_map(|i| stealers[i].steal().success())
            .next()
            .ok_or_else(|| {
                if self.is_closed() && self.queue.is_empty() {
                    PopTaskError::Closed
                } else {
                    thread::park_timeout(EMPTY_DELAY);
                    PopTaskError::Empty
                }
            })
    }

    /// Tries to steal a task from the global queue, otherwise tries to steal a task from another
    /// worker thread.
    fn try_pop_unchecked(&self) -> Result<Task<W>, PopTaskError> {
        if let Some(task) = self.queue.steal().success() {
            Ok(task)
        } else {
            self.try_steal_from_worker()
        }
    }

    /// Tries to steal up to `limit + 1` tasks from the global queue. If at least one task was
    /// stolen, it is popped and returned. Otherwise tries to steal a task from another worker
    /// thread.
    #[cfg(feature = "batching")]
    fn try_refill_and_pop(
        &self,
        local_batch: &crossbeam_deque::Worker<Task<W>>,
        limit: usize,
    ) -> Result<Task<W>, PopTaskError> {
        if let Some(task) = self
            .queue
            .steal_batch_with_limit_and_pop(local_batch, limit + 1)
            .success()
        {
            Ok(task)
        } else {
            self.try_steal_from_worker()
        }
    }

    fn is_closed(&self) -> bool {
        self.status.is_closed()
    }

    fn close(&self, urgent: bool) {
        self.status.set(urgent);
    }

    fn drain_into(self, tasks: &mut Vec<Task<W>>) {
        while let Some(task) = self.queue.steal().success() {
            tasks.push(task);
        }
        // since the `TaskQueues` instance does not retain a reference to the workers' queues
        // (it can't, because they're not Send/Sync), the only way we have to drain them is via
        // their stealers
        self.stealers.into_inner().into_iter().for_each(|stealer| {
            while let Some(task) = stealer.steal().success() {
                tasks.push(task);
            }
        })
    }
}

pub struct WorkstealingWorkerQueues<W: Worker> {
    local: crossbeam_deque::Worker<Task<W>>,
    global: Arc<GlobalQueue<W>>,
    shared: Arc<LocalQueueShared<W>>,
}

impl<W: Worker> WorkstealingWorkerQueues<W> {
    fn new(
        local: crossbeam_deque::Worker<Task<W>>,
        global: &Arc<GlobalQueue<W>>,
        shared: &Arc<LocalQueueShared<W>>,
    ) -> Self {
        Self {
            global: Arc::clone(global),
            local,
            shared: Arc::clone(shared),
        }
    }
}

impl<W: Worker> WorkerQueues<W> for WorkstealingWorkerQueues<W> {
    fn push(&self, task: Task<W>) {
        self.local.push(task);
    }

    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
        self.shared.try_pop(&self.global, &self.local)
    }

    #[cfg(feature = "retry")]
    fn try_push_retry(&self, task: Task<W>) -> Result<std::time::Instant, Task<W>> {
        self.shared.try_push_retry(task)
    }
}

impl<W: Worker> Deref for WorkstealingWorkerQueues<W> {
    type Target = Self;

    fn deref(&self) -> &Self::Target {
        self
    }
}

struct LocalQueueShared<W: Worker> {
    _thread_index: usize,
    /// queue of abandon tasks
    local_abandoned: SegQueue<Task<W>>,
    #[cfg(feature = "batching")]
    batch_limit: crate::atomic::AtomicUsize,
    /// thread-local queues used for tasks that are waiting to be retried after a failure
    #[cfg(feature = "retry")]
    local_retry: super::RetryQueue<W>,
}

impl<W: Worker> LocalQueueShared<W> {
    fn new(thread_index: usize, _config: &Config) -> Self {
        Self {
            _thread_index: thread_index,
            local_abandoned: Default::default(),
            #[cfg(feature = "batching")]
            batch_limit: crate::atomic::AtomicUsize::new(_config.batch_limit.get_or_default()),
            #[cfg(feature = "retry")]
            local_retry: super::RetryQueue::new(_config.retry_factor.get_or_default()),
        }
    }

    fn update(&self, _config: &Config) {
        #[cfg(feature = "batching")]
        self.batch_limit.set(_config.batch_limit.get_or_default());
        #[cfg(feature = "retry")]
        self.local_retry
            .set_delay_factor(_config.retry_factor.get_or_default());
    }

    fn try_pop(
        &self,
        global: &GlobalQueue<W>,
        local_batch: &crossbeam_deque::Worker<Task<W>>,
    ) -> Result<Task<W>, PopTaskError> {
        if !global.status.can_pop() {
            return Err(PopTaskError::Closed);
        }
        // first try to get a previously abandoned task
        if let Some(task) = self.local_abandoned.pop() {
            return Ok(task);
        }
        // if retry is enabled, try to get a task from the retry queue
        #[cfg(feature = "retry")]
        if let Some(task) = self.local_retry.try_pop() {
            return Ok(task);
        }
        // next try the local queue
        if let Some(task) = local_batch.pop() {
            return Ok(task);
        }
        // fall back to requesting a task from the global queue - if batching is enabled, this will
        // also try to refill the local queue
        #[cfg(feature = "batching")]
        {
            let limit = self.batch_limit.get();
            if limit > 0 {
                return global.try_refill_and_pop(local_batch, limit);
            }
        }
        global.try_pop_unchecked()
    }

    fn drain_into(self, tasks: &mut Vec<Task<W>>) {
        while let Some(task) = self.local_abandoned.pop() {
            tasks.push(task);
        }
        #[cfg(feature = "retry")]
        self.local_retry.drain_into(tasks);
    }

    #[cfg(feature = "retry")]
    fn try_push_retry(&self, task: Task<W>) -> Result<std::time::Instant, Task<W>> {
        self.local_retry.try_push(task)
    }
}
