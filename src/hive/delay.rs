use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct DelayQueue<T>(BinaryHeap<Delayed<T>>);

impl<T> DelayQueue<T> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Pushes an item onto the queue.
    pub fn push(&mut self, item: T, delay: Duration) {
        self.0.push(Delayed::new(item, delay));
    }

    /// Returns the item at the head of the queue, if one exists, and removes it.
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

    /// Consumes this queue and returns all the items. Panics if more than one thread holds a
    /// reference to this queue.
    pub fn into_iter(self) -> impl Iterator<Item = T> {
        self.0.into_iter().map(|delayed| delayed.value)
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

        assert!(queue.is_empty());
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_into_vec() {
        let mut queue = DelayQueue::default();
        queue.push(1, Duration::from_secs(1));
        queue.push(2, Duration::from_secs(2));
        queue.push(3, Duration::from_secs(3));
        let mut v: Vec<_> = queue.into_iter().collect();
        v.sort();
        assert_eq!(v, vec![1, 2, 3]);
    }
}
