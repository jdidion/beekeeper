use super::{Config, PopTaskError, Task, TaskQueues, Token, WorkerQueues};
use crate::atomic::{Atomic, AtomicBool, AtomicUsize};
use crate::bee::Worker;
use crossbeam_deque::{Injector, Stealer};
use crossbeam_queue::SegQueue;
use parking_lot::RwLock;
use rand::prelude::*;
use std::ops::Deref;
use std::sync::Arc;

struct WorkstealingTaskQueues<W: Worker> {
    global: Arc<GlobalQueue<W>>,
    local: RwLock<Vec<Arc<LocalQueueShared<W>>>>,
}

impl<W: Worker> TaskQueues<W> for WorkstealingTaskQueues<W> {
    type WorkerQueues = WorkstealingWorkerQueues<W>;
    type WorkerQueuesTarget = Self::WorkerQueues;

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
            local_queues.push(Arc::new(LocalQueueShared::new(
                thread_index,
                &self.global,
                config,
            )));
        });
    }

    fn update_for_threads(&self, start_index: usize, end_index: usize, config: &Config) {
        let local_queues = self.local.read();
        assert!(local_queues.len() > end_index);
        (start_index..end_index).for_each(|thread_index| local_queues[thread_index].update(config));
    }

    fn worker_queues(&self, thread_index: usize) -> Self::WorkerQueuesTarget {
        let local_queue = crossbeam_deque::Worker::new_fifo();
        self.global.add_stealer(local_queue.stealer());
        let shared = &self.local.read()[thread_index];
        WorkstealingWorkerQueues::new(local_queue, Arc::clone(&shared))
    }

    fn try_push_global(&self, task: Task<W>) -> Result<(), Task<W>> {
        self.global.try_push(task)
    }

    fn close(&self, _: Token) {
        self.global.close();
    }

    fn drain(self) -> Vec<Task<W>> {
        self.close(Token);
        let mut tasks = Vec::new();
        let global = crate::hive::unwrap_arc(self.global);
        global.drain_into(&mut tasks);
        for local in self.local.into_inner().into_iter() {
            let local = crate::hive::unwrap_arc(local);
            local.drain_into(&mut tasks);
        }
        tasks
    }
}

pub struct GlobalQueue<W: Worker> {
    queue: Injector<Task<W>>,
    stealers: RwLock<Vec<Stealer<Task<W>>>>,
    closed: AtomicBool,
}

impl<W: Worker> GlobalQueue<W> {
    fn new() -> Self {
        Self {
            queue: Injector::new(),
            stealers: Default::default(),
            closed: AtomicBool::default(),
        }
    }

    fn add_stealer(&self, stealer: Stealer<Task<W>>) {
        self.stealers.write().push(stealer);
    }

    fn try_push(&self, task: Task<W>) -> Result<(), Task<W>> {
        if self.closed.get() {
            return Err(task);
        }
        self.queue.push(task);
        Ok(())
    }

    fn try_steal(&self) -> Option<Task<W>> {
        let stealers = self.stealers.read();
        let n = stealers.len();
        // randomize the stealing order, to prevent always stealing from the same thread
        // TODO: put this into a shared global to prevent creating a new instance on every call
        std::iter::from_fn(|| Some(rand::rng().random_range(0..n)))
            .take(n)
            .filter_map(|i| stealers[i].steal().success())
            .next()
    }

    /// Tries to steal a task from the global queue, otherwise tries to steal a task from another
    /// worker thread.
    #[cfg(not(feature = "batching"))]
    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
        if let Some(task) = self.queue.steal().success() {
            Ok(task)
        } else {
            self.try_steal().ok_or(PopTaskError::Empty)
        }
    }

    /// Tries to steal up to `limit` tasks from the global queue. If at least one task was stolen,
    /// it is popped and returned. Otherwise tries to steal a task from another worker thread.
    #[cfg(feature = "batching")]
    fn try_refill_and_pop(
        &self,
        local_batch: &crossbeam_deque::Worker<Task<W>>,
        limit: usize,
    ) -> Result<Task<W>, PopTaskError> {
        if let Some(task) = self
            .queue
            .steal_batch_with_limit_and_pop(local_batch, limit)
            .success()
        {
            return Ok(task);
        }
        self.try_steal().ok_or(PopTaskError::Empty)
    }

    fn close(&self) {
        self.closed.set(true);
    }

    fn drain_into(self, tasks: &mut Vec<Task<W>>) {
        while let Some(task) = self.queue.steal().success() {
            tasks.push(task);
        }
        self.stealers.into_inner().into_iter().for_each(|stealer| {
            while let Some(task) = stealer.steal().success() {
                tasks.push(task);
            }
        })
    }
}

pub struct WorkstealingWorkerQueues<W: Worker> {
    queue: crossbeam_deque::Worker<Task<W>>,
    shared: Arc<LocalQueueShared<W>>,
}

impl<W: Worker> WorkstealingWorkerQueues<W> {
    fn new(queue: crossbeam_deque::Worker<Task<W>>, shared: Arc<LocalQueueShared<W>>) -> Self {
        Self { queue, shared }
    }
}

impl<W: Worker> WorkerQueues<W> for WorkstealingWorkerQueues<W> {
    fn push(&self, task: Task<W>) {
        self.queue.push(task);
    }

    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
        self.shared.try_pop(&self.queue)
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
    global: Arc<GlobalQueue<W>>,
    /// queue of abandon tasks
    local_abandoned: SegQueue<Task<W>>,
    #[cfg(feature = "batching")]
    batch_limit: AtomicUsize,
    /// thread-local queues used for tasks that are waiting to be retried after a failure
    #[cfg(feature = "retry")]
    local_retry: super::retry::RetryQueue<W>,
}

impl<W: Worker> LocalQueueShared<W> {
    fn new(thread_index: usize, global: &Arc<GlobalQueue<W>>, config: &Config) -> Self {
        Self {
            _thread_index: thread_index,
            global: Arc::clone(global),
            local_abandoned: Default::default(),
            #[cfg(feature = "batching")]
            batch_limit: AtomicUsize::new(config.batch_limit.get_or_default()),
            #[cfg(feature = "retry")]
            local_retry: super::retry::RetryQueue::new(config.retry_factor.get_or_default()),
        }
    }

    fn update(&self, config: &Config) {
        #[cfg(feature = "batching")]
        self.batch_limit.set(config.batch_limit.get_or_default());
        #[cfg(feature = "retry")]
        self.local_retry
            .set_delay_factor(config.retry_factor.get_or_default());
    }

    fn try_pop(
        &self,
        local_batch: &crossbeam_deque::Worker<Task<W>>,
    ) -> Result<Task<W>, PopTaskError> {
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
        // fall back to requesting a task from the global queue - this will also refill the local
        // batch queue if the batching feature is enabled
        if let Some(task) = local_batch.pop() {
            return Ok(task);
        }
        #[cfg(feature = "batching")]
        {
            self.global
                .try_refill_and_pop(local_batch, self.batch_limit.get())
        }
        #[cfg(not(feature = "batching"))]
        {
            self.global.try_pop()
        }
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
