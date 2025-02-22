//! Implementation of `TaskQueues` that uses `crossbeam` channels for the global queue (i.e., for
//! sending tasks from the `Hive` to the worker threads) and a default implementation of local
//! queues that depends on which combination of the `retry` and `batching` features are enabled.
use super::{Config, PopTaskError, Status, Task, TaskQueues, Token, WorkerQueues};
use crate::bee::Worker;
use crossbeam_channel::RecvTimeoutError;
use crossbeam_queue::SegQueue;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

// time to wait when polling the global queue
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Type alias for the input task channel sender
type TaskSender<W> = crossbeam_channel::Sender<Task<W>>;
/// Type alias for the input task channel receiver
type TaskReceiver<W> = crossbeam_channel::Receiver<Task<W>>;

pub struct ChannelTaskQueues<W: Worker> {
    global: Arc<GlobalQueue<W>>,
    local: RwLock<Vec<Arc<LocalQueueShared<W>>>>,
}

impl<W: Worker> TaskQueues<W> for ChannelTaskQueues<W> {
    type WorkerQueues = ChannelWorkerQueues<W>;

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
            local_queues.push(Arc::new(LocalQueueShared::new(thread_index, config)))
        });
    }

    fn update_for_threads(&self, start_index: usize, end_index: usize, config: &Config) {
        let local_queues = self.local.write();
        assert!(local_queues.len() >= end_index);
        local_queues[start_index..end_index]
            .iter()
            .for_each(|queue| queue.update(&self.global, config));
    }

    fn try_push_global(&self, task: Task<W>) -> Result<(), Task<W>> {
        self.global.try_push(task)
    }

    fn worker_queues(&self, thread_index: usize) -> Self::WorkerQueues {
        ChannelWorkerQueues::new(&self.global, &self.local.read()[thread_index])
    }

    fn close(&self, urgent: bool, _: Token) {
        self.global.close(urgent)
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
    global_tx: TaskSender<W>,
    global_rx: TaskReceiver<W>,
    status: Status,
}

impl<W: Worker> GlobalQueue<W> {
    fn new() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self {
            global_tx: tx,
            global_rx: rx,
            status: Default::default(),
        }
    }

    #[inline]
    fn try_push(&self, task: Task<W>) -> Result<(), Task<W>> {
        if !self.status.can_push() {
            return Err(task);
        }
        self.global_tx.send(task).map_err(|err| err.into_inner())
    }

    #[inline]
    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
        match self.global_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(task) => Ok(task),
            Err(RecvTimeoutError::Disconnected) => Err(PopTaskError::Closed),
            Err(RecvTimeoutError::Timeout) if self.is_closed() && self.global_rx.is_empty() => {
                Err(PopTaskError::Closed)
            }
            Err(RecvTimeoutError::Timeout) => Err(PopTaskError::Empty),
        }
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.status.is_closed()
    }

    fn close(&self, urgent: bool) {
        self.status.set(urgent);
    }

    fn drain_into(self, tasks: &mut Vec<Task<W>>) {
        tasks.reserve(self.global_rx.len());
        tasks.extend(self.global_rx.try_iter());
    }

    #[cfg(feature = "batching")]
    fn try_iter(&self) -> impl Iterator<Item = Task<W>> {
        self.global_rx.try_iter()
    }
}

pub struct ChannelWorkerQueues<W: Worker> {
    global: Arc<GlobalQueue<W>>,
    shared: Arc<LocalQueueShared<W>>,
}

impl<W: Worker> ChannelWorkerQueues<W> {
    fn new(global_queue: &Arc<GlobalQueue<W>>, shared: &Arc<LocalQueueShared<W>>) -> Self {
        Self {
            global: Arc::clone(global_queue),
            shared: Arc::clone(shared),
        }
    }
}

impl<W: Worker> WorkerQueues<W> for ChannelWorkerQueues<W> {
    fn push(&self, task: Task<W>) {
        self.shared.push(task, &self.global);
    }

    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
        self.shared.try_pop(&self.global)
    }

    #[cfg(feature = "retry")]
    fn try_push_retry(&self, task: Task<W>) -> Result<std::time::Instant, Task<W>> {
        self.shared.try_push_retry(task)
    }
}

struct LocalQueueShared<W: Worker> {
    _thread_index: usize,
    /// queue of abandon tasks
    local_abandoned: SegQueue<Task<W>>,
    /// thread-local queue of tasks used when the `batching` feature is enabled
    #[cfg(feature = "batching")]
    local_batch: batching::WorkerBatchQueue<W>,
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
            local_batch: batching::WorkerBatchQueue::new(_config.batch_limit.get_or_default()),
            #[cfg(feature = "retry")]
            local_retry: super::RetryQueue::new(_config.retry_factor.get_or_default()),
        }
    }

    /// Updates the local queues based on the provided `config`:
    /// If `batching` is enabled, resizes the batch queue if necessary.
    /// If `retry` is enabled, updates the retry factor.
    fn update(&self, _global: &GlobalQueue<W>, _config: &Config) {
        #[cfg(feature = "batching")]
        self.local_batch
            .set_limit(_config.batch_limit.get_or_default(), _global, self);
        #[cfg(feature = "retry")]
        self.local_retry
            .set_delay_factor(_config.retry_factor.get_or_default());
    }

    #[inline]
    fn push(&self, task: Task<W>, global: &GlobalQueue<W>) {
        #[cfg(feature = "batching")]
        let task = match self.local_batch.try_push(task) {
            Ok(_) => return,
            Err(task) => task,
        };
        self.push_global(task, global);
    }

    #[inline]
    fn push_global(&self, task: Task<W>, global: &GlobalQueue<W>) {
        let task = match global.try_push(task) {
            Ok(_) => return,
            Err(task) => task,
        };
        self.local_abandoned.push(task);
    }

    #[inline]
    fn try_pop(&self, global: &GlobalQueue<W>) -> Result<Task<W>, PopTaskError> {
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
        // if batching is enabled, try to get a task from the batch queue
        // and try to refill it from the global queue if it's empty
        #[cfg(feature = "batching")]
        {
            self.local_batch.try_pop_or_refill(global, self)
        }
        // fall back to requesting a task from the global queue
        #[cfg(not(feature = "batching"))]
        {
            global.try_pop()
        }
    }

    #[cfg(feature = "retry")]
    fn try_push_retry(&self, task: Task<W>) -> Result<std::time::Instant, Task<W>> {
        self.local_retry.try_push(task)
    }

    /// Consumes this `ChannelWorkerQueues` and drains the tasks currently in the queues into
    /// `tasks`.
    fn drain_into(self, tasks: &mut Vec<Task<W>>) {
        while let Some(task) = self.local_abandoned.pop() {
            tasks.push(task);
        }
        #[cfg(feature = "batching")]
        self.local_batch.drain_into(tasks);
        #[cfg(feature = "retry")]
        self.local_retry.drain_into(tasks);
    }
}

#[cfg(feature = "batching")]
mod batching {
    use super::{GlobalQueue, LocalQueueShared, Task};
    use crate::atomic::{Atomic, AtomicUsize};
    use crate::bee::Worker;
    use crate::hive::inner::queue::PopTaskError;
    use crossbeam_queue::ArrayQueue;
    use parking_lot::RwLock;

    pub struct WorkerBatchQueue<W: Worker> {
        inner: RwLock<Option<ArrayQueue<Task<W>>>>,
        limit: AtomicUsize,
    }

    impl<W: Worker> WorkerBatchQueue<W> {
        pub fn new(batch_limit: usize) -> Self {
            if batch_limit == 0 {
                Self {
                    inner: RwLock::new(None),
                    limit: Default::default(),
                }
            } else {
                Self {
                    inner: RwLock::new(Some(ArrayQueue::new(batch_limit))),
                    limit: AtomicUsize::new(batch_limit),
                }
            }
        }

        pub fn set_limit(
            &self,
            limit: usize,
            global: &GlobalQueue<W>,
            parent: &LocalQueueShared<W>,
        ) {
            // acquire the exclusive lock first to prevent simultaneous updates
            let mut queue = self.inner.write();
            let old_limit = self.limit.set(limit);
            if old_limit == limit {
                return;
            }
            let old_queue = if limit == 0 {
                queue.take()
            } else {
                queue.replace(ArrayQueue::new(limit))
            };
            if let Some(old_queue) = old_queue {
                // try to push tasks from the old queue to the new one and fall back to pushing
                // them to the global queue
                old_queue
                    .into_iter()
                    .filter_map(|task| {
                        if let Some(new_queue) = queue.as_ref() {
                            new_queue.push(task).err()
                        } else {
                            Some(task)
                        }
                    })
                    .for_each(|task| parent.push_global(task, global));
            }
        }

        pub fn try_push(&self, task: Task<W>) -> Result<(), Task<W>> {
            if let Some(queue) = self.inner.read().as_ref() {
                queue.push(task)
            } else {
                Err(task)
            }
        }

        pub fn try_pop_or_refill(
            &self,
            global: &GlobalQueue<W>,
            parent: &LocalQueueShared<W>,
        ) -> Result<Task<W>, PopTaskError> {
            // pop from the local queue if it has any tasks
            if let Some(local) = self.inner.read().as_ref() {
                if !local.is_empty() {
                    if let Some(task) = local.pop() {
                        return Ok(task);
                    }
                }
                // otherwise pull at least 1 and up to `batch_limit + 1` tasks from the input channel
                // wait for the next task from the receiver
                let first = global.try_pop()?;
                // if we succeed in getting the first task, try to refill the local queue
                let limit = self.limit.get();
                // batch size 0 means batching is disabled
                if limit > 0 {
                    // otherwise try to take up to `batch_limit` tasks from the input channel
                    // and add them to the local queue, but don't block if the input channel
                    // is empty
                    for task in global.try_iter().take(limit) {
                        if let Err(task) = local.push(task) {
                            parent.local_abandoned.push(task);
                            break;
                        }
                    }
                }
                Ok(first)
            } else {
                global.try_pop()
            }
        }

        pub fn drain_into(self, tasks: &mut Vec<Task<W>>) {
            if let Some(queue) = self.inner.into_inner() {
                tasks.reserve(queue.len());
                while let Some(task) = queue.pop() {
                    tasks.push(task);
                }
            }
        }
    }
}
