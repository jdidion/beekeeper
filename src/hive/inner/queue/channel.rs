//! Implementation of `TaskQueues` that uses `crossbeam` channels for the global queue (i.e., for
//! sending tasks from the `Hive` to the worker threads) and a default implementation of local
//! queues that depends on which combination of the `retry` and `batching` features are enabled.
use super::{Config, PopTaskError, Task, TaskQueues, Token, WorkerQueues};
use crate::atomic::{Atomic, AtomicBool};
use crate::bee::Worker;
use crossbeam_channel::RecvTimeoutError;
use crossbeam_queue::SegQueue;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

// time to wait in between polling the retry queue and then the task receiver
const RECV_TIMEOUT: Duration = Duration::from_secs(1);

/// Type alias for the input task channel sender
type TaskSender<W> = crossbeam_channel::Sender<Task<W>>;
/// Type alias for the input task channel receiver
type TaskReceiver<W> = crossbeam_channel::Receiver<Task<W>>;

pub struct ChannelTaskQueues<W: Worker> {
    global: Arc<GlobalQueue<W>>,
    local: RwLock<Vec<Arc<ChannelWorkerQueues<W>>>>,
}

impl<W: Worker> TaskQueues<W> for ChannelTaskQueues<W> {
    type WorkerQueues = ChannelWorkerQueues<W>;
    type WorkerQueuesTarget = Arc<Self::WorkerQueues>;

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
            local_queues.push(Arc::new(ChannelWorkerQueues::new(
                thread_index,
                &self.global,
                config,
            )))
        });
    }

    fn update_for_threads(&self, start_index: usize, end_index: usize, config: &Config) {
        let local_queues = self.local.write();
        assert!(local_queues.len() > end_index);
        local_queues[start_index..end_index]
            .iter()
            .for_each(|queue| queue.update(config));
    }

    fn worker_queues(&self, thread_index: usize) -> Arc<Self::WorkerQueues> {
        Arc::clone(&self.local.read()[thread_index])
    }

    fn try_push_global(&self, task: Task<W>) -> Result<(), Task<W>> {
        self.global.try_push(task)
    }

    fn close(&self, _: Token) {
        self.global.close()
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
    global_tx: TaskSender<W>,
    global_rx: TaskReceiver<W>,
    closed: AtomicBool,
}

impl<W: Worker> GlobalQueue<W> {
    fn new() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self {
            global_tx: tx,
            global_rx: rx,
            closed: Default::default(),
        }
    }

    #[inline]
    fn try_push(&self, task: Task<W>) -> Result<(), Task<W>> {
        if self.closed.get() {
            return Err(task);
        }
        self.global_tx.send(task).map_err(|err| err.into_inner())
    }

    #[inline]
    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
        match self.global_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(task) => Ok(task),
            Err(RecvTimeoutError::Disconnected) => Err(PopTaskError::Closed),
            Err(RecvTimeoutError::Timeout) if self.closed.get() && self.global_rx.is_empty() => {
                Err(PopTaskError::Closed)
            }
            Err(RecvTimeoutError::Timeout) => Err(PopTaskError::Empty),
        }
    }

    fn try_iter(&self) -> impl Iterator<Item = Task<W>> + '_ {
        self.global_rx.try_iter()
    }

    fn close(&self) {
        self.closed.set(true);
    }

    fn drain_into(self, tasks: &mut Vec<Task<W>>) {
        tasks.extend(self.global_rx.try_iter());
    }
}

pub struct ChannelWorkerQueues<W: Worker> {
    _thread_index: usize,
    global: Arc<GlobalQueue<W>>,
    /// queue of abandon tasks
    local_abandoned: SegQueue<Task<W>>,
    /// thread-local queue of tasks used when the `batching` feature is enabled
    #[cfg(feature = "batching")]
    local_batch: RwLock<crossbeam_queue::ArrayQueue<Task<W>>>,
    /// thread-local queues used for tasks that are waiting to be retried after a failure
    #[cfg(feature = "retry")]
    local_retry: super::retry::RetryQueue<W>,
}

impl<W: Worker> ChannelWorkerQueues<W> {
    fn new(thread_index: usize, global_queue: &Arc<GlobalQueue<W>>, config: &Config) -> Self {
        Self {
            _thread_index: thread_index,
            global: Arc::clone(global_queue),
            local_abandoned: Default::default(),
            #[cfg(feature = "batching")]
            local_batch: RwLock::new(crossbeam_queue::ArrayQueue::new(
                config.batch_limit.get_or_default().max(1),
            )),
            #[cfg(feature = "retry")]
            local_retry: super::retry::RetryQueue::new(config.retry_factor.get_or_default()),
        }
    }

    /// Updates the local queues based on the provided `config`:
    /// If `batching` is enabled, resizes the batch queue if necessary.
    /// If `retry` is enabled, updates the retry factor.
    fn update(&self, config: &Config) {
        #[cfg(feature = "batching")]
        self.update_batch(config);
        #[cfg(feature = "retry")]
        self.local_retry.set_delay_factor(config.retry_factor.get_or_default());
    }

    /// Consumes this `ChannelWorkerQueues` and drains the tasks currently in the queues into
    /// `tasks`.
    fn drain_into(self, tasks: &mut Vec<Task<W>>) {
        while let Some(task) = self.local_abandoned.pop() {
            tasks.push(task);
        }
        #[cfg(feature = "batching")]
        {
            let batch = self.local_batch.into_inner();
            tasks.reserve(batch.len());
            while let Some(task) = batch.pop() {
                tasks.push(task);
            }
        }
        #[cfg(feature = "retry")]
        self.local_retry.drain_into(tasks);
    }
}

impl<W: Worker> WorkerQueues<W> for ChannelWorkerQueues<W> {
    fn push(&self, task: Task<W>) {
        #[cfg(feature = "batching")]
        let task = match self.local_batch.read().push(task) {
            Ok(_) => return,
            Err(task) => task,
        };
        let task = match self.global.try_push(task) {
            Ok(_) => return,
            Err(task) => task,
        };
        self.local_abandoned.push(task);
    }

    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
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
            self.try_pop_batch_or_refill().ok_or(PopTaskError::Empty)
        }
        // fall back to requesting a task from the global queue
        #[cfg(not(feature = "batching"))]
        self.global.try_pop()
    }

    #[cfg(feature = "retry")]
    fn try_push_retry(&self, task: Task<W>) -> Result<std::time::Instant, Task<W>> {
        self.local_retry.try_push(task)
    }
}

#[cfg(feature = "batching")]
mod batching {
    use super::{ChannelWorkerQueues, Config, Task};
    use crate::bee::Worker;
    use crossbeam_queue::ArrayQueue;
    use std::time::Duration;

    impl<W: Worker> ChannelWorkerQueues<W> {
        pub fn update_batch(&self, config: &Config) {
            let batch_limit = config.batch_limit.get_or_default().max(1);
            let mut queue = self.local_batch.write();
            // block until the current queue is small enough that it can fit into the new queue
            while queue.len() > batch_limit {
                std::thread::sleep(Duration::from_millis(10));
            }
            let new_queue = ArrayQueue::new(batch_limit);
            while let Some(task) = queue.pop() {
                if let Err(task) = new_queue
                    .push(task)
                    .or_else(|task| self.global.try_push(task))
                {
                    self.local_abandoned.push(task);
                    break;
                }
            }
            assert!(queue.is_empty());
            *queue = new_queue;
        }

        pub(super) fn try_pop_batch_or_refill(&self) -> Option<Task<W>> {
            // pop from the local queue if it has any tasks
            let local_queue = self.local_batch.read();
            if !local_queue.is_empty() {
                return local_queue.pop();
            }
            // otherwise pull at least 1 and up to `batch_limit + 1` tasks from the input channel
            // wait for the next task from the receiver
            let first = self.global.try_pop().ok();
            // if we fail after trying to get one, don't keep trying to fill the queue
            if first.is_some() {
                let batch_limit = local_queue.capacity();
                // batch size 0 means batching is disabled
                if batch_limit > 0 {
                    // otherwise try to take up to `batch_limit` tasks from the input channel
                    // and add them to the local queue, but don't block if the input channel
                    // is empty
                    for task in self.global.try_iter().take(batch_limit) {
                        if let Err(task) = local_queue.push(task) {
                            self.local_abandoned.push(task);
                            break;
                        }
                    }
                }
            }
            first
        }
    }
}
