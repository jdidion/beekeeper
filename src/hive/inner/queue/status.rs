use crate::atomic::{Atomic, AtomicU8};

const OPEN: u8 = 0;
const CLOSED_PUSH: u8 = 1;
const CLOSED_POP: u8 = 2;

/// Represents the status of a task queue.
///
/// This is a simple state machine
/// OPEN -> CLOSED_PUSH -> CLOSED_POP
///   |________________________^
pub struct Status(AtomicU8);

impl Status {
    /// Returns `true` if the queue status is `CLOSED_PUSH` or `CLOSED_POP`.
    pub fn is_closed(&self) -> bool {
        self.0.get() > OPEN
    }

    /// Returns `true` if the queue can accept new tasks.
    pub fn can_push(&self) -> bool {
        self.0.get() < CLOSED_PUSH
    }

    /// Returns `true` if the queue can remove tasks.
    pub fn can_pop(&self) -> bool {
        self.0.get() < CLOSED_POP
    }

    /// Sets the queue status to `CLOSED_PUSH` if `urgent` is `false`, or `CLOSED_POP` if `urgent`
    /// is `true`.
    pub fn set(&self, urgent: bool) {
        // TODO: this update should be done with `fetch_max`
        let new_status = if urgent { CLOSED_POP } else { CLOSED_PUSH };
        if new_status > self.0.get() {
            self.0.set(new_status);
        }
    }
}

impl Default for Status {
    fn default() -> Self {
        Self(AtomicU8::new(OPEN))
    }
}
