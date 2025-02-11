use crate::atomic::{Atomic, AtomicBool};
use crate::bee::Worker;
use crate::hive::{GlobalPopError, GlobalQueue, Task};
use crossbeam_channel::RecvTimeoutError;
use std::time::Duration;

/// Type alias for the input task channel sender
type TaskSender<W> = crossbeam_channel::Sender<Task<W>>;
/// Type alias for the input task channel receiver
type TaskReceiver<W> = crossbeam_channel::Receiver<Task<W>>;

pub struct ChannelGlobalQueue<W: Worker> {
    tx: TaskSender<W>,
    rx: TaskReceiver<W>,
    closed: AtomicBool,
}

impl<W: Worker> ChannelGlobalQueue<W> {
    /// Returns a new `GlobalQueue` that uses the given channel sender for pushing new tasks
    /// and the given channel receiver for popping tasks.
    pub(super) fn new(tx: TaskSender<W>, rx: TaskReceiver<W>) -> Self {
        Self {
            tx,
            rx,
            closed: AtomicBool::default(),
        }
    }

    pub(super) fn try_pop_timeout(
        &self,
        timeout: Duration,
    ) -> Option<Result<Task<W>, GlobalPopError>> {
        match self.rx.recv_timeout(timeout) {
            Ok(task) => Some(Ok(task)),
            Err(RecvTimeoutError::Disconnected) => Some(Err(GlobalPopError::Closed)),
            Err(RecvTimeoutError::Timeout) if self.closed.get() && self.rx.is_empty() => {
                Some(Err(GlobalPopError::Closed))
            }
            Err(RecvTimeoutError::Timeout) => None,
        }
    }
}

impl<W: Worker> GlobalQueue<W> for ChannelGlobalQueue<W> {
    fn try_push(&self, task: Task<W>) -> Result<(), Task<W>> {
        if !self.closed.get() {
            self.tx.try_send(task).map_err(|err| err.into_inner())
        } else {
            Err(task)
        }
    }

    fn try_pop(&self) -> Option<Result<Task<W>, GlobalPopError>> {
        // time to wait in between polling the retry queue and then the task receiver
        const RECV_TIMEOUT: Duration = Duration::from_secs(1);
        self.try_pop_timeout(RECV_TIMEOUT)
    }

    fn try_iter(&self) -> impl Iterator<Item = Task<W>> + '_ {
        self.rx.try_iter()
    }

    fn drain(&self) -> Vec<Task<W>> {
        self.rx.try_iter().collect()
    }

    fn close(&self) {
        self.closed.set(true);
    }
}

impl<W: Worker> Default for ChannelGlobalQueue<W> {
    fn default() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self::new(tx, rx)
    }
}
