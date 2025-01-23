use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

/// A queue where each item has an associated `Instant` at which it will be available.
///
/// This is implemented internally as a `BinaryHeap`. This data structure is *not* thread-safe.
#[derive(Debug)]
pub struct DelayQueue<T>(BinaryHeap<Delayed<T>>);

impl<T> DelayQueue<T> {
    /// Pushes an item onto the queue. Returns the `Instant` at which the item will be available.
    pub fn push(&mut self, item: T, delay: Duration) -> Instant {
        let delayed = Delayed::new(item, delay);
        let until = delayed.until;
        self.0.push(delayed);
        until
    }

    /// Returns the `Instant` at which the next item will be available. Returns `None` if the queue
    /// is empty.
    pub fn next_available(&self) -> Option<Instant> {
        self.0.peek().map(|head| head.until)
    }

    /// Returns the item at the head of the queue, if one exists and is available (i.e. its delay
    /// has been exceeded), and removes it.
    pub fn try_pop(&mut self) -> Option<T> {
        if self
            .0
            .peek()
            .map(|head| head.until <= Instant::now())
            .unwrap_or(false)
        {
            Some(self.0.pop().unwrap().value)
        } else {
            None
        }
    }

    /// Drains all items from the queue and returns them as an iterator.
    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.0.drain().map(|delayed| delayed.value)
    }
}

impl<T> Default for DelayQueue<T> {
    fn default() -> Self {
        DelayQueue(BinaryHeap::new())
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

    #[test]
    fn test_works() {
        let mut queue = DelayQueue::default();

        queue.push(1, Duration::from_secs(1));
        queue.push(2, Duration::from_secs(2));
        queue.push(3, Duration::from_secs(3));

        assert_eq!(queue.0.len(), 3);
        assert_eq!(queue.try_pop(), None);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(1));
        assert_eq!(queue.0.len(), 2);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(2));
        assert_eq!(queue.0.len(), 1);

        thread::sleep(Duration::from_secs(1));
        assert_eq!(queue.try_pop(), Some(3));
        assert_eq!(queue.0.len(), 0);

        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_into_vec() {
        let mut queue = DelayQueue::default();
        queue.push(1, Duration::from_secs(1));
        queue.push(2, Duration::from_secs(2));
        queue.push(3, Duration::from_secs(3));
        let mut v: Vec<_> = queue.drain().collect();
        v.sort();
        assert_eq!(v, vec![1, 2, 3]);
    }
}
