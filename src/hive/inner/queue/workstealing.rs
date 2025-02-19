use super::{Config, PopTaskError, Task, TaskQueues, Token, WorkerQueues};
use crate::atomic::{Atomic, AtomicBool, AtomicUsize};
use crate::bee::Worker;
use crossbeam_deque::{Injector as GlobalQueue, Stealer, Worker as LocalQueue};
use parking_lot::{Mutex, RwLock};

pub struct WorkstealingWorkerQueues<W: Worker> {
    local_batch: LocalQueue<W>,
    batch_limit: AtomicUsize,
    /// thread-local queues used for tasks that are waiting to be retried after a failure
    #[cfg(feature = "retry")]
    local_retry: super::delay::DelayQueue<Task<W>>,
    #[cfg(feature = "retry")]
    retry_factor: crate::atomic::AtomicU64,
}

impl<W: Worker> WorkerQueues<W> for WorkstealingWorkerQueues<W> {
    fn push(&self, task: Task<W>) {
        todo!()
    }

    fn try_pop(&self) -> Result<Task<W>, PopTaskError> {
        todo!()
    }

    fn try_push_retry(&self, task: Task<W>) -> Result<std::time::Instant, Task<W>> {
        todo!()
    }
}

struct WorkstealingQueues<W: Worker> {
    global_queue: GlobalQueue<Task<W>>,
    local_queues: RwLock<Vec<WorkstealingWorkerQueues<W>>>,
    local_stealers: RwLock<Vec<Stealer<Task<W>>>>,
    closed: AtomicBool,
}

impl<W: Worker> TaskQueues<W> for WorkstealingQueues<W> {
    type WorkerQueues = WorkstealingWorkerQueues<W>;

    fn new(_: Token) -> Self {
        Self {
            global_queue: Default::default(),
            local_worker_queues: Default::default(),
            local_stealers: Default::default(),
            batch_limit: AtomicUsize::new(config.batch_limit.get_or_default()),
            closed: Default::default(),
            #[cfg(feature = "retry")]
            local_retry_queues: Default::default(),
            #[cfg(feature = "retry")]
            retry_factor: crate::atomic::AtomicU64::new(config.retry_factor.get_or_default()),
        }
    }

    fn init_for_threads(&self, start_index: usize, end_index: usize, config: &Config) {
        let mut local_queues = self.local_worker_queues.write();
        assert_eq!(local_queues.len(), start_index);
        let mut stealers = self.local_stealers.write();
        (start_index..end_index).for_each(|_| {
            let local_queue = LocalQueue::new_fifo();
            let stealer = local_queue.stealer();
            local_queues.push(Mutex::new(local_queue));
            stealers.push(stealer);
        });
        //#[cfg(feature = "retry")]
        //self.init_retry_queues_for_threads(start_index, end_index);
    }

    fn update_for_threads(&self, start_index: usize, end_index: usize, config: &Config) {
        todo!()
    }

    fn try_push_global(&self, task: Task<W>) -> Result<(), Task<W>> {
        if !self.closed.get() {
            self.global_queue.push(task);
            Ok(())
        } else {
            Err(task)
        }
    }

    fn try_push_local(&self, task: Task<W>, thread_index: usize) -> Result<(), Task<W>> {
        self.local_worker_queues.read()[thread_index]
            .lock()
            .push(task);
        Ok(())
    }

    fn try_pop(&self, thread_index: usize) -> Result<Task<W>, PopTaskError> {
        // first try popping from the local queue
        {
            let worker_queue_mutex = &self.local_worker_queues.read()[thread_index];
            let worker_queue = worker_queue_mutex.lock();
            worker_queue.pop().or_else(|| {
                self.global_queue
                    .steal_batch_with_limit_and_pop(&worker_queue, self.batch_limit.get())
                    .success()
            })
        }
        .or_else(|| {
            // TODO: randomize the order
            self.local_stealers
                .read()
                .iter()
                .filter_map(|stealer| stealer.steal().success())
                .next()
        })
        .ok_or(PopTaskError::Empty)
    }

    fn drain(&self) -> Vec<Task<W>> {
        let mut tasks = Vec::new();
        while let Some(task) = self.global_queue.steal().success() {
            tasks.push(task);
        }
        let local_queues = self.local_worker_queues.read();
        local_queues
            .iter()
            .fold(tasks, |mut tasks, local_queue_mutex| {
                let local_queue = local_queue_mutex.lock();
                while let Some(task) = local_queue.pop() {
                    tasks.push(task);
                }
                tasks
            })
    }

    fn retry(&self, task: Task<W>, thread_index: usize) -> Result<std::time::Instant, Task<W>> {
        todo!()
    }

    fn close(&self, _: Token) {
        self.closed.set(true);
    }
}
