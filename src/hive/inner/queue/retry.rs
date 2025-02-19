use crate::atomic::{Atomic, AtomicU64};
use crate::bee::Worker;
use crate::hive::Task;
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

/// A queue where each item has an associated `Instant` at which it will be available.
///
/// This is implemented internally as a `UnsafeCell<BinaryHeap>`.
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
    pub fn new(delay_factor: u64) -> Self {
        Self {
            inner: UnsafeCell::new(BinaryHeap::new()),
            delay_factor: AtomicU64::new(delay_factor),
        }
    }

    pub fn set_delay_factor(&self, delay_factor: u64) {
        self.delay_factor.set(delay_factor);
    }

    /// Pushes an item onto the queue. Returns the `Instant` at which the item will be available,
    /// or an error with `item` if there was an error pushing the item.
    ///
    /// SAFETY: this method is only ever called within a single thread.
    pub fn try_push(&self, task: Task<W>) -> Result<Instant, Task<W>> {
        // compute the delay
        let delay = 2u64
            .checked_pow(task.attempt - 1)
            .and_then(|multiplier| {
                self.delay_factor
                    .get()
                    .checked_mul(multiplier)
                    .or(Some(u64::MAX))
                    .map(Duration::from_nanos)
            })
            .unwrap_or_default();
        unsafe {
            match self.inner.get().as_mut() {
                Some(queue) => {
                    let delayed = Delayed::new(task, delay);
                    let until = delayed.until;
                    queue.push(delayed);
                    Ok(until)
                }
                None => Err(task),
            }
        }
    }

    /// Returns the item at the head of the queue, if one exists and is available (i.e., its delay
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

    /// Consumes this `RetryQueue` and drains all items from the queue into `sink`.
    pub fn drain_into(self, sink: &mut Vec<Task<W>>) {
        let mut queue = self.inner.into_inner();
        sink.reserve(queue.len());
        sink.extend(queue.drain().map(|delayed| delayed.value))
    }
}

unsafe impl<W: Worker> Sync for RetryQueue<W> {}

type DelayedTask<W> = Delayed<Task<W>>;

#[derive(Debug)]
struct Delayed<T> {
    value: T,
    until: Instant,
}

impl<T> Delayed<T> {
    pub fn new(value: T, delay: Duration) -> Self {
        Delayed {
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
impl<T> Ord for Delayed<T> {
    fn cmp(&self, other: &Delayed<T>) -> Ordering {
        other.until.cmp(&self.until)
    }
}

impl<T> PartialOrd for Delayed<T> {
    fn partial_cmp(&self, other: &Delayed<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Delayed<T> {
    fn eq(&self, other: &Delayed<T>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T> Eq for Delayed<T> {}

#[cfg(test)]
mod tests {
    use super::{RetryQueue, Task, Worker};
    use crate::bee::stock::EchoWorker;
    use std::{thread, time::Duration};

    type TestWorker = EchoWorker<usize>;
    const DELAY: u64 = Duration::from_secs(1).as_nanos() as u64;

    impl<W: Worker> RetryQueue<W> {
        fn len(&self) -> usize {
            unsafe { self.inner.get().as_ref().unwrap().len() }
        }
    }

    #[test]
    fn test_works() {
        let queue = RetryQueue::<TestWorker>::new(DELAY);

        let task1 = Task::with_attempt(1, 1, None, 1);
        let task2 = Task::with_attempt(2, 2, None, 2);
        let task3 = Task::with_attempt(3, 3, None, 3);

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

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(task3));
        assert_eq!(queue.len(), 0);

        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_into_vec() {
        let queue = RetryQueue::<TestWorker>::new(DELAY);

        let task1 = Task::with_attempt(1, 1, None, 1);
        let task2 = Task::with_attempt(2, 2, None, 2);
        let task3 = Task::with_attempt(3, 3, None, 3);

        queue.try_push(task1.clone()).unwrap();
        queue.try_push(task2.clone()).unwrap();
        queue.try_push(task3.clone()).unwrap();

        let mut v = Vec::new();
        queue.drain_into(&mut v);
        v.sort();

        assert_eq!(v, vec![task1, task2, task3]);
    }
}
