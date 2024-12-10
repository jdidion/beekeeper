use crate::hive::{CoreId, Cores, Outcome, Shared, Task};
use crate::task::{ApplyError, Context, Queen, Worker};
#[cfg(feature = "affinity")]
use std::iter;
use std::{sync::Arc, thread};

/// Spawn `num_threads` worker threads all with access to the same `shared` data.
#[cfg(not(feature = "affinity"))]
pub fn brood<W: Worker, Q: Queen<Kind = W>>(num_threads: usize, shared: &Arc<Shared<W, Q>>) {
    for _ in 0..num_threads {
        spawn::<W, Q>(Arc::clone(shared));
    }
}

/// Spawn `num_threads` worker threads all with access to the same `shared` data.
#[cfg(feature = "affinity")]
pub fn brood<W: Worker, Q: Queen<Kind = W>>(num_threads: usize, shared: &Arc<Shared<W, Q>>) {
    for _ in 0..num_threads {
        spawn::<W, Q>(Arc::clone(shared), None);
    }
}

/// Spawn `num_threads` worker threads and try to pin each thread to a specific core. The indices
/// of the cores to pin are provided in the `affinity` set.
#[cfg(feature = "affinity")]
pub fn brood_with_affinity<W: Worker, Q: Queen<Kind = W>>(
    num_threads: usize,
    shared: &Arc<Shared<W, Q>>,
    affinity: &Cores,
) {
    let affinity = affinity.iter_core_ids().map(Some).chain(iter::repeat(None));
    (0..num_threads)
        .map(|_| Arc::clone(shared))
        .zip(affinity)
        .for_each(|(shared, core)| spawn::<W, Q>(shared, core))
}

/// Spawns a new worker thread.
fn spawn<W: Worker, Q: Queen<Kind = W>>(
    shared: Arc<Shared<W, Q>>,
    #[cfg(feature = "affinity")] core: Option<(usize, CoreId)>,
) {
    shared
        .as_thread_builder()
        .spawn(move || {
            // Will spawn a new thread on panic unless it is cancelled
            #[cfg(feature = "affinity")]
            let sentinel: Sentinel<W, Q> = if core.is_some_and(|(core_index, core_id)| {
                shared.add_core_affinity(core_index) && core_id.set_for_current()
            }) {
                Sentinel::new(Arc::clone(&shared), core)
            } else {
                Sentinel::new(Arc::clone(&shared), None)
            };
            #[cfg(not(feature = "affinity"))]
            let sentinel: Sentinel<W, Q> = Sentinel::new(Arc::clone(&shared));
            let mut worker = shared.create_worker();
            let mut active = true;
            while active {
                // Shutdown this thread if the pool has become smaller
                active = !shared.too_many_threads()
                    && shared
                        // Get the next task - increments the counter
                        .next_task()
                        .map(Task::into_parts)
                        // Execute the task until it succeeds or we reach maximum retries - this
                        // should be the only place where a panic might occur
                        .map(|(input, mut ctx, outcome_tx)| {
                            let outcome = match worker.apply(input, &ctx) {
                                Err(ApplyError::Retryable { input, .. })
                                    if shared.has_retries() =>
                                {
                                    retry(input, &mut ctx, &mut worker, &shared)
                                }
                                result => Outcome::from_worker_result(result, ctx.index(), false),
                            };
                            // Send the outcome to the receiver or store it in the hive
                            if let Some(tx) = outcome_tx {
                                tx.send(outcome).is_ok()
                            } else {
                                shared.add_outcome(outcome);
                                true
                            }
                        })
                        // Finish the task - decrements the counter and notifies other threads
                        .inspect(|_| shared.finish_task(false))
                        // Shutdown this thread if the receiver hung up
                        .unwrap_or(false);
            }
            sentinel.cancel();
        })
        .unwrap();
}

#[inline]
fn retry<W: Worker, Q: Queen<Kind = W>>(
    mut input: W::Input,
    ctx: &mut Context,
    worker: &mut W,
    shared: &Shared<W, Q>,
) -> Outcome<W> {
    // Execute the task until it succeeds or we reach maximum retries
    let max_retries = shared.max_retries();
    debug_assert!(max_retries > 0);
    loop {
        ctx.inc_attempt();
        input = match worker.apply(input, &ctx) {
            Err(ApplyError::Retryable { input, .. }) if ctx.attempt() < max_retries => {
                if let Some(delay) = shared.get_delay(ctx.attempt()) {
                    thread::sleep(delay);
                }
                input
            }
            result => {
                return Outcome::from_worker_result(result, ctx.index(), true);
            }
        }
    }
}

/// Sentinel for a worker thread. Until the sentinel is cancelled, it will respawn the worker
/// thread if it panics.
struct Sentinel<W: Worker, Q: Queen<Kind = W>> {
    shared: Arc<Shared<W, Q>>,
    #[cfg(feature = "affinity")]
    core: Option<(usize, CoreId)>,
    active: bool,
}

impl<W: Worker, Q: Queen<Kind = W>> Sentinel<W, Q> {
    fn new(
        shared: Arc<Shared<W, Q>>,
        #[cfg(feature = "affinity")] core: Option<(usize, CoreId)>,
    ) -> Self {
        Sentinel {
            shared,
            #[cfg(feature = "affinity")]
            core,
            active: true,
        }
    }

    /// Cancel and destroy this sentinel.
    fn cancel(mut self) {
        self.active = false;
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Drop for Sentinel<W, Q> {
    fn drop(&mut self) {
        // the thread is only re-spawned if there are fewer than the max allowed threads in the hive
        let respawn = self.active && {
            self.shared.finish_task(thread::panicking());
            !self.shared.too_many_threads()
        };
        if respawn {
            #[cfg(feature = "affinity")]
            spawn::<W, Q>(Arc::clone(&self.shared), self.core);
            #[cfg(not(feature = "affinity"))]
            spawn::<W, Q>(Arc::clone(&self.shared))
        } else {
            #[cfg(feature = "affinity")]
            if let Some((core_index, _)) = self.core {
                self.shared.remove_core_affinity(core_index);
            }
        }
    }
}
