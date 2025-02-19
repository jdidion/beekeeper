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
pub struct DelayQueue<T>(UnsafeCell<BinaryHeap<Delayed<T>>>);

impl<T> DelayQueue<T> {
    /// Pushes an item onto the queue. Returns the `Instant` at which the item will be available,
    /// or an error with `item` if there was an error pushing the item.
    ///
    /// SAFETY: this method is only ever called within a single thread.
    pub fn push(&self, item: T, delay: Duration) -> Result<Instant, T> {
        unsafe {
            match self.0.get().as_mut() {
                Some(queue) => {
                    let delayed = Delayed::new(item, delay);
                    let until = delayed.until;
                    queue.push(delayed);
                    Ok(until)
                }
                None => Err(item),
            }
        }
    }

    /// Returns the item at the head of the queue, if one exists and is available (i.e., its delay
    /// has been exceeded), and removes it.
    ///
    /// SAFETY: this method is only ever called within a single thread.
    pub fn try_pop(&self) -> Option<T> {
        unsafe {
            let queue_ptr = self.0.get();
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

    /// Consumes this `DelayQueue` and drains all items from the queue into `sink`.
    pub fn drain_into(self, sink: &mut Vec<T>) {
        let mut queue = self.0.into_inner();
        sink.reserve(queue.len());
        sink.extend(queue.drain().map(|delayed| delayed.value))
    }
}

unsafe impl<T: Send> Sync for DelayQueue<T> {}

impl<T> Default for DelayQueue<T> {
    fn default() -> Self {
        DelayQueue(UnsafeCell::new(BinaryHeap::new()))
    }
}

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
/// `BinaryHeap` of the `DelayQueue`.
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
    use super::DelayQueue;
    use std::{thread, time::Duration};

    impl<T> DelayQueue<T> {
        fn len(&self) -> usize {
            unsafe { self.0.get().as_ref().unwrap().len() }
        }
    }

    #[test]
    fn test_works() {
        let queue = DelayQueue::default();

        queue.push(1, Duration::from_secs(1)).unwrap();
        queue.push(2, Duration::from_secs(2)).unwrap();
        queue.push(3, Duration::from_secs(3)).unwrap();

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.try_pop(), None);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(1));
        assert_eq!(queue.len(), 2);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(2));
        assert_eq!(queue.len(), 1);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(3));
        assert_eq!(queue.len(), 0);

        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_into_vec() {
        let queue = DelayQueue::default();
        queue.push(1, Duration::from_secs(1)).unwrap();
        queue.push(2, Duration::from_secs(2)).unwrap();
        queue.push(3, Duration::from_secs(3)).unwrap();
        let mut v = Vec::new();
        queue.drain_into(&mut v);
        v.sort();
        assert_eq!(v, vec![1, 2, 3]);
    }
}
