use crate::atomic::{Atomic, AtomicU64};
use crate::bee::Worker;
use crate::hive::Task;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

/// A task queue where each task has an associated `Instant` at which it will be available.
///
/// This is implemented internally as `UnsafeCell<BinaryHeap>`.
///
/// SAFETY: This data structure is designed to enable the queue to be modified (using `push` and
/// `try_pop`) by a *single thread* using interior mutability. The `drain` method is called by a
/// different thread, but it first takes ownership of the queue and so will never be called
/// concurrently with `push/pop`.
///
/// `UnsafeCell` is used for performance - this is safe so long as the queue is only accessed from
/// a single thread at a time. This data structure is *not* thread-safe.
#[derive(Debug)]
pub struct RetryQueue<W: Worker> {
    inner: UnsafeCell<BinaryHeap<DelayedTask<W>>>,
    delay_factor: AtomicU64,
}

impl<W: Worker> RetryQueue<W> {
    /// Creates a new `RetryQueue` with the given `delay_factor` (in nanoseconds).
    pub fn new(delay_factor: u64) -> Self {
        Self {
            inner: UnsafeCell::new(BinaryHeap::new()),
            delay_factor: AtomicU64::new(delay_factor),
        }
    }

    /// Changes the delay factor for the queue.
    pub fn set_delay_factor(&self, delay_factor: u64) {
        self.delay_factor.set(delay_factor);
    }

    /// Pushes an item onto the queue. Returns the `Instant` at which the task will be available,
    /// or an error with `task` if there was an error pushing it.
    ///
    /// SAFETY: this method is only ever called within a single thread.
    pub fn try_push(&self, task: Task<W>) -> Result<Instant, Task<W>> {
        unsafe {
            match self.inner.get().as_mut() {
                Some(queue) => {
                    // compute the delay
                    let delay = 2u64
                        .checked_pow(task.meta.attempt() as u32 - 1)
                        .and_then(|multiplier| {
                            self.delay_factor
                                .get()
                                .checked_mul(multiplier)
                                .or(Some(u64::MAX))
                                .map(Duration::from_nanos)
                        })
                        .unwrap_or_default();
                    let delayed = DelayedTask::new(task, delay);
                    let until = delayed.until;
                    queue.push(delayed);
                    Ok(until)
                }
                None => Err(task),
            }
        }
    }

    /// Returns the task at the head of the queue, if one exists and is available (i.e., its delay
    /// has been exceeded), and removes it.
    ///
    /// SAFETY: this method is only ever called within a single thread.
    pub fn try_pop(&self) -> Option<Task<W>> {
        unsafe {
            let queue_ptr = self.inner.get();
            if queue_ptr
                .as_ref()
                .and_then(|queue| queue.peek())
                .map(|head| head.until <= Instant::now())
                .unwrap_or(false)
            {
                queue_ptr
                    .as_mut()
                    .and_then(|queue| queue.pop())
                    .map(|delayed| delayed.value)
            } else {
                None
            }
        }
    }

    /// Consumes this `RetryQueue` and drains all tasks from the queue into `sink`.
    pub fn drain_into(self, sink: &mut Vec<Task<W>>) {
        let mut queue = self.inner.into_inner();
        sink.reserve(queue.len());
        sink.extend(queue.drain().map(|delayed| delayed.value))
    }
}

unsafe impl<W: Worker> Sync for RetryQueue<W> {}

/// Wrapper for a Task with an associated `Instant` at which it will be available.
struct DelayedTask<W: Worker> {
    value: Task<W>,
    until: Instant,
}

impl<W: Worker> DelayedTask<W> {
    pub fn new(value: Task<W>, delay: Duration) -> Self {
        Self {
            value,
            until: Instant::now() + delay,
        }
    }
}

/// Implements ordering for `Delayed`, so it can be used to correctly order elements in the
/// `BinaryHeap` of the `RetryQueue`.
///
/// Earlier entries have higher priority (should be popped first), so they are Greater that later
/// entries.
impl<W: Worker> Ord for DelayedTask<W> {
    fn cmp(&self, other: &DelayedTask<W>) -> Ordering {
        other.until.cmp(&self.until)
    }
}

impl<W: Worker> PartialOrd for DelayedTask<W> {
    fn partial_cmp(&self, other: &DelayedTask<W>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<W: Worker> PartialEq for DelayedTask<W> {
    fn eq(&self, other: &DelayedTask<W>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<W: Worker> Eq for DelayedTask<W> {}

#[cfg(test)]
mod tests {
    use super::{RetryQueue, Task, Worker};
    use crate::bee::stock::EchoWorker;
    use crate::bee::{TaskId, TaskMeta};
    use std::{thread, time::Duration};

    type TestWorker = EchoWorker<usize>;
    const DELAY: u64 = Duration::from_secs(1).as_nanos() as u64;

    impl<W: Worker> RetryQueue<W> {
        fn len(&self) -> usize {
            unsafe { self.inner.get().as_ref().unwrap().len() }
        }
    }

    impl<W: Worker> Task<W> {
        /// Creates a new `Task` with the given `task_id`.
        fn with_attempt(task_id: TaskId, input: W::Input, attempt: u8) -> Self {
            Self {
                input,
                meta: TaskMeta::with_attempt(task_id, attempt),
                outcome_tx: None,
            }
        }
    }

    #[test]
    fn test_works() {
        let queue = RetryQueue::<TestWorker>::new(DELAY);

        let task1 = Task::with_attempt(1, 1, 1);
        let task2 = Task::with_attempt(2, 2, 2);
        let task3 = Task::with_attempt(3, 3, 3);

        queue.try_push(task1.clone()).unwrap();
        queue.try_push(task2.clone()).unwrap();
        queue.try_push(task3.clone()).unwrap();

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.try_pop(), None);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(task1));
        assert_eq!(queue.len(), 2);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(task2));
        assert_eq!(queue.len(), 1);

        thread::sleep(Duration::from_secs(2));
        assert_eq!(queue.try_pop(), Some(task3));
        assert_eq!(queue.len(), 0);

        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_into_vec() {
        let queue = RetryQueue::<TestWorker>::new(DELAY);

        let task1 = Task::with_attempt(1, 1, 1);
        let task2 = Task::with_attempt(2, 2, 2);
        let task3 = Task::with_attempt(3, 3, 3);

        queue.try_push(task1.clone()).unwrap();
        queue.try_push(task2.clone()).unwrap();
        queue.try_push(task3.clone()).unwrap();

        let mut v = Vec::new();
        queue.drain_into(&mut v);
        v.sort();

        assert_eq!(v, vec![task1, task2, task3]);
    }
}
