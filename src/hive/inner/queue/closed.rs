use crate::atomic::{Atomic, AtomicU8};

const OPEN: u8 = 0;
const CLOSED_PUSH: u8 = 1;
const CLOSED_POP: u8 = 2;

pub struct Closed(AtomicU8);

impl Closed {
    pub fn is_closed(&self) -> bool {
        self.0.get() > OPEN
    }

    pub fn can_push(&self) -> bool {
        self.0.get() < CLOSED_PUSH
    }

    pub fn can_pop(&self) -> bool {
        self.0.get() < CLOSED_POP
    }

    pub fn set(&self, urgent: bool) {
        self.0.set(if urgent { CLOSED_POP } else { CLOSED_PUSH });
    }
}

impl Default for Closed {
    fn default() -> Self {
        Self(AtomicU8::new(OPEN))
    }
}
