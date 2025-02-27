use super::{Shared, TaskQueues};
use crate::bee::{Queen, Worker};
use std::io::Error as SpawnError;
use std::sync::Arc;
use std::thread::JoinHandle;

/// Sentinel for a worker thread. Until the sentinel is cancelled, it will respawn the worker
/// thread if it panics.
pub struct Sentinel<W, Q, T, F>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
    F: Fn(usize, &Arc<Shared<Q, T>>) -> Result<JoinHandle<()>, SpawnError> + 'static,
{
    thread_index: usize,
    shared: Arc<Shared<Q, T>>,
    active: bool,
    respawn_fn: F,
}

impl<W, Q, T, F> Sentinel<W, Q, T, F>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
    F: Fn(usize, &Arc<Shared<Q, T>>) -> Result<JoinHandle<()>, SpawnError> + 'static,
{
    pub fn new(thread_index: usize, shared: Arc<Shared<Q, T>>, respawn_fn: F) -> Self {
        Self {
            thread_index,
            shared,
            active: true,
            respawn_fn,
        }
    }

    /// Cancel and destroy this sentinel.
    pub fn cancel(mut self) {
        self.active = false;
    }
}

impl<W, Q, T, F> Drop for Sentinel<W, Q, T, F>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
    F: Fn(usize, &Arc<Shared<Q, T>>) -> Result<JoinHandle<()>, SpawnError> + 'static,
{
    fn drop(&mut self) {
        if self.active {
            // if the sentinel is active, that means the thread panicked during task execution, so
            // we have to finish the task here before respawning
            self.shared.finish_task(std::thread::panicking());
            // only respawn if the sentinel is active and the hive has not been poisoned
            if !self.shared.is_poisoned() {
                // can't do anything with the previous result
                let _ = self
                    .shared
                    .respawn_thread(self.thread_index, |thread_index| {
                        (self.respawn_fn)(thread_index, &self.shared)
                    });
            }
        }
    }
}
