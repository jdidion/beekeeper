use std::marker::PhantomData;

use crate::bee::{Queen, Worker};
use crate::hive::{GlobalQueue, LocalQueues, QueuePair, Shared, Task};
use parking_lot::RwLock;

pub struct LocalQueuesImpl<W: Worker, G: GlobalQueue<W>> {
    /// thread-local queues of tasks used when the `batching` feature is enabled
    #[cfg(feature = "batching")]
    batch_queues: RwLock<Vec<crossbeam_queue::ArrayQueue<Task<W>>>>,
    /// thread-local queues used for tasks that are waiting to be retried after a failure
    #[cfg(feature = "retry")]
    retry_queues: RwLock<Vec<crate::hive::queue::delay::DelayQueue<Task<W>>>>,
    /// marker for the global queue type
    _global: PhantomData<G>,
}

#[cfg(feature = "retry")]
impl<W: Worker, G: GlobalQueue<W>> LocalQueuesImpl<W, G> {
    #[inline]
    fn try_pop_retry(&self, thread_index: usize) -> Option<Task<W>> {
        self.retry_queues
            .read()
            .get(thread_index)
            .and_then(|queue| queue.try_pop())
    }
}

#[cfg(feature = "batching")]
impl<W: Worker, G: GlobalQueue<W>> LocalQueuesImpl<W, G> {
    #[inline]
    fn try_push_local(&self, task: Task<W>, thread_index: usize) -> Result<(), Task<W>> {
        self.batch_queues.read()[thread_index].push(task)
    }

    #[inline]
    fn try_pop_local_or_refill<Q: Queen<Kind = W>, P: QueuePair<W, Global = G, Local = Self>>(
        &self,
        thread_index: usize,
        shared: &Shared<W, Q, P>,
    ) -> Option<Task<W>> {
        let local_queue = &self.batch_queues.read()[thread_index];
        // pop from the local queue if it has any tasks
        if !local_queue.is_empty() {
            return local_queue.pop();
        }
        // otherwise pull at least 1 and up to `batch_size + 1` tasks from the input channel
        // wait for the next task from the receiver
        let first = shared.global_queue.try_pop().and_then(Result::ok);
        // if we fail after trying to get one, don't keep trying to fill the queue
        if first.is_some() {
            let batch_size = shared.batch_size();
            // batch size 0 means batching is disabled
            if batch_size > 0 {
                // otherwise try to take up to `batch_size` tasks from the input channel
                // and add them to the local queue, but don't block if the input channel
                // is empty
                for result in shared
                    .global_queue
                    .try_iter()
                    .take(batch_size)
                    .map(|task| local_queue.push(task))
                {
                    if let Err(task) = result {
                        // for some reason we can't push the task to the local queue;
                        // this should never happen, but just in case we turn it into an
                        // unprocessed outcome and stop iterating
                        shared.abandon_task(task);
                        break;
                    }
                }
            }
        }
        first
    }
}

impl<W: Worker, G: GlobalQueue<W>> LocalQueues<W, G> for LocalQueuesImpl<W, G> {
    fn init_for_threads<Q: Queen<Kind = W>, P: QueuePair<W, Global = G, Local = Self>>(
        &self,
        start_index: usize,
        end_index: usize,
        #[allow(unused_variables)] shared: &Shared<W, Q, P>,
    ) {
        #[cfg(feature = "batching")]
        self.init_batch_queues_for_threads(start_index, end_index, shared);
        #[cfg(feature = "retry")]
        self.init_retry_queues_for_threads(start_index, end_index);
    }

    #[cfg(feature = "batching")]
    fn resize<Q: Queen<Kind = W>, P: QueuePair<W, Global = G, Local = Self>>(
        &self,
        start_index: usize,
        end_index: usize,
        new_size: usize,
        shared: &Shared<W, Q, P>,
    ) {
        self.resize_batch_queues(start_index, end_index, new_size, shared);
    }

    /// Creates a task from `input` and pushes it to the local queue if there is space,
    /// otherwise attempts to add it to the global queue. Returns the task ID if the push
    /// succeeds, otherwise returns an error with the input.
    fn push<Q: Queen<Kind = W>, P: QueuePair<W, Global = G, Local = Self>>(
        &self,
        task: Task<W>,
        #[allow(unused_variables)] thread_index: usize,
        shared: &Shared<W, Q, P>,
    ) {
        #[cfg(feature = "batching")]
        let task = match self.try_push_local(task, thread_index) {
            Ok(_) => return,
            Err(task) => task,
        };
        shared.push_global(task);
    }

    /// Returns the next task from the local queue if there are any, otherwise attempts to
    /// fetch at least 1 and up to `batch_size + 1` tasks from the input channel and puts all
    /// but the first one into the local queue.
    fn try_pop<Q: Queen<Kind = W>, P: QueuePair<W, Global = G, Local = Self>>(
        &self,
        thread_index: usize,
        #[allow(unused_variables)] shared: &Shared<W, Q, P>,
    ) -> Option<Task<W>> {
        #[cfg(feature = "retry")]
        if let Some(task) = self.try_pop_retry(thread_index) {
            return Some(task);
        }
        #[cfg(feature = "batching")]
        if let Some(task) = self.try_pop_local_or_refill(thread_index, shared) {
            return Some(task);
        }
        None
    }

    fn drain(&self) -> Vec<Task<W>> {
        let mut tasks = Vec::new();
        #[cfg(feature = "batching")]
        {
            self.drain_batch_queues_into(&mut tasks);
        }
        #[cfg(feature = "retry")]
        {
            self.drain_retry_queues_into(&mut tasks);
        }
        tasks
    }

    #[cfg(feature = "retry")]
    fn retry<Q: Queen<Kind = W>, P: QueuePair<W, Global = G, Local = Self>>(
        &self,
        task: Task<W>,
        thread_index: usize,
        shared: &Shared<W, Q, P>,
    ) -> Option<std::time::Instant> {
        self.try_push_retry(task, thread_index, shared)
    }
}

impl<W: Worker, G: GlobalQueue<W>> Default for LocalQueuesImpl<W, G> {
    fn default() -> Self {
        Self {
            #[cfg(feature = "batching")]
            batch_queues: Default::default(),
            #[cfg(feature = "retry")]
            retry_queues: Default::default(),
            _global: PhantomData,
        }
    }
}

#[cfg(feature = "batching")]
mod batching {
    use super::LocalQueuesImpl;
    use crate::bee::{Queen, Worker};
    use crate::hive::{GlobalQueue, QueuePair, Shared, Task};
    use crossbeam_queue::ArrayQueue;
    use std::collections::HashSet;
    use std::time::Duration;

    impl<W: Worker, G: GlobalQueue<W>> LocalQueuesImpl<W, G> {
        pub(super) fn init_batch_queues_for_threads<
            Q: Queen<Kind = W>,
            P: QueuePair<W, Global = G, Local = Self>,
        >(
            &self,
            start_index: usize,
            end_index: usize,
            shared: &Shared<W, Q, P>,
        ) {
            let mut batch_queues = self.batch_queues.write();
            assert_eq!(batch_queues.len(), start_index);
            let queue_size = shared.batch_size().max(1);
            (start_index..end_index).for_each(|_| batch_queues.push(ArrayQueue::new(queue_size)));
        }

        pub(super) fn resize_batch_queues<
            Q: Queen<Kind = W>,
            P: QueuePair<W, Global = G, Local = Self>,
        >(
            &self,
            start_index: usize,
            end_index: usize,
            batch_size: usize,
            shared: &Shared<W, Q, P>,
        ) {
            // keep track of which queues need to be resized
            // TODO: this method could cause a hang if one of the worker threads is stuck - we
            // might want to keep track of each queue's size and if we don't see it shrink
            // within a certain amount of time, we give up on that thread and leave it with a
            // wrong-sized queue (which should never cause a panic)
            let mut to_resize: HashSet<usize> = (start_index..end_index).collect();
            // iterate until we've resized them all
            loop {
                // scope the mutable access to local_queues
                {
                    let mut batch_queues = self.batch_queues.write();
                    to_resize.retain(|thread_index| {
                        let queue = if let Some(queue) = batch_queues.get_mut(*thread_index) {
                            queue
                        } else {
                            return false;
                        };
                        if queue.len() > batch_size {
                            return true;
                        }
                        let new_queue = ArrayQueue::new(batch_size);
                        while let Some(task) = queue.pop() {
                            if let Err(task) = new_queue.push(task) {
                                // for some reason we can't push the task to the new queue
                                // this should never happen, but just in case we turn it into
                                // an unprocessed outcome
                                shared.abandon_task(task);
                            }
                        }
                        // this is safe because the worker threads can't get readable access to the
                        // queue while this thread holds the lock
                        let old_queue = std::mem::replace(queue, new_queue);
                        assert!(old_queue.is_empty());
                        false
                    });
                }
                if !to_resize.is_empty() {
                    // short sleep to give worker threads the chance to pull from their queues
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        pub(super) fn drain_batch_queues_into(&self, tasks: &mut Vec<Task<W>>) {
            let _ = self
                .batch_queues
                .write()
                .iter_mut()
                .fold(tasks, |tasks, queue| {
                    tasks.reserve(queue.len());
                    while let Some(task) = queue.pop() {
                        tasks.push(task);
                    }
                    tasks
                });
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use super::LocalQueuesImpl;
    use crate::bee::{Queen, Worker};
    use crate::hive::queue::delay::DelayQueue;
    use crate::hive::{GlobalQueue, QueuePair, Shared, Task};
    use std::time::{Duration, Instant};

    impl<W: Worker, G: GlobalQueue<W>> LocalQueuesImpl<W, G> {
        /// Initializes the retry queues worker threads in the specified range.
        pub(super) fn init_retry_queues_for_threads(&self, start_index: usize, end_index: usize) {
            let mut retry_queues = self.retry_queues.write();
            assert_eq!(retry_queues.len(), start_index);
            (start_index..end_index).for_each(|_| retry_queues.push(DelayQueue::default()))
        }

        /// Adds a task to the retry queue with a delay based on `attempt`.
        pub(super) fn try_push_retry<
            Q: Queen<Kind = W>,
            P: QueuePair<W, Global = G, Local = Self>,
        >(
            &self,
            task: Task<W>,
            thread_index: usize,
            shared: &Shared<W, Q, P>,
        ) -> Option<Instant> {
            // compute the delay
            let delay = shared
                .config
                .retry_factor
                .get()
                .map(|retry_factor| {
                    2u64.checked_pow(task.attempt - 1)
                        .and_then(|multiplier| {
                            retry_factor
                                .checked_mul(multiplier)
                                .or(Some(u64::MAX))
                                .map(Duration::from_nanos)
                        })
                        .unwrap()
                })
                .unwrap_or_default();
            if let Some(queue) = self.retry_queues.read().get(thread_index) {
                queue.push(task, delay)
            } else {
                Err(task)
            }
            // if unable to queue the task, abandon it
            .map_err(|task| shared.abandon_task(task))
            .ok()
        }

        pub(super) fn drain_retry_queues_into(&self, tasks: &mut Vec<Task<W>>) {
            let _ = self
                .retry_queues
                .write()
                .iter_mut()
                .fold(tasks, |tasks, queue| {
                    tasks.reserve(queue.len());
                    tasks.extend(queue.drain());
                    tasks
                });
        }
    }
}
