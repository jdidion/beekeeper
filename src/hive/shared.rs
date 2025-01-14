use super::{Config, Outcome, OutcomeSender, Shared, Task, TaskReceiver};
use crate::atomic::{Atomic, AtomicNumber, AtomicUsize};
use crate::bee::{Context, Queen, Worker};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::thread::Builder;
use std::time::Duration;
use std::{fmt, iter, mem};

// TODO: it's not clear if SeqCst ordering is actually necessary - need to do some fuzz testing.
// const SEQCST_ORDERING: Orderings = Orderings {
//     load: Ordering::SeqCst,
//     swap: Ordering::SeqCst,
//     fetch_update_set: Ordering::SeqCst,
//     fetch_update_fetch: Ordering::SeqCst,
//     fetch_add: Ordering::SeqCst,
//     fetch_sub: Ordering::SeqCst,
// };

impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
    pub fn new(config: Config, queen: Q, task_rx: TaskReceiver<W>) -> Self {
        Shared {
            config,
            queen: Mutex::new(queen),
            task_rx: Mutex::new(task_rx),
            num_tasks_queued: AtomicUsize::default(), //AtomicUsize::new(0, SEQCST_ORDERING),
            num_tasks_active: AtomicUsize::default(), //AtomicUsize::new(0, SEQCST_ORDERING),
            next_task_index: Default::default(),
            num_panics: Default::default(),
            suspended: Default::default(),
            suspended_condvar: Default::default(),
            join_condvar: Default::default(),
            outcomes: Default::default(),
            #[cfg(feature = "retry")]
            retry_queue: Default::default(),
            #[cfg(feature = "retry")]
            next_retry: Default::default(),
        }
    }

    /// Returns a `Builder` for creating a new thread in the `Hive`.
    pub fn thread_builder(&self) -> Builder {
        let mut builder = Builder::new();
        if let Some(ref name) = self.config.thread_name.get() {
            builder = builder.name(name.clone());
        }
        if let Some(ref stack_size) = self.config.thread_stack_size.get() {
            builder = builder.stack_size(stack_size.to_owned());
        }
        builder
    }

    /// Increases the maximum number of threads allowed in the `Hive` by `num_threads` and returns
    /// the previous value.
    pub fn add_threads(&self, num_threads: usize) -> usize {
        self.config.num_threads.add(num_threads).unwrap()
    }

    /// Ensures that the number of threads is at least `num_threads`. Returns the previous value.
    pub fn ensure_threads(&self, num_threads: usize) -> usize {
        self.config.num_threads.set_max(num_threads).unwrap()
    }

    /// Returns a new `Worker` from the queen, or an error if a `Worker` could not be created.
    pub fn create_worker(&self) -> Q::Kind {
        self.queen.lock().create()
    }

    /// Increments the number of queued tasks. Returns a new `Task` with the provided input and
    /// `outcome_tx` and the next index.
    pub fn prepare_task(&self, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> Task<W> {
        self.num_tasks_queued.add(1);
        let index = self.next_task_index.add(1);
        let ctx = Context::new(index, self.suspended.clone());
        Task::new(input, ctx, outcome_tx)
    }

    /// Increments the number of queued tasks by the number of provided inputs. Returns an iterator
    /// over `Task`s created from the provided inputs, `outcome_tx`s, and sequential indices.
    pub fn prepare_batch<'a, T: Iterator<Item = W::Input> + 'a>(
        &'a self,
        min_size: usize,
        inputs: T,
        outcome_tx: Option<OutcomeSender<W>>,
    ) -> impl Iterator<Item = Task<W>> + 'a {
        self.num_tasks_queued.add(min_size);
        let index_start = self.next_task_index.add(min_size);
        let index_end = index_start + min_size;
        inputs
            .map(Some)
            .chain(iter::repeat_with(|| None))
            .zip(
                (index_start..index_end)
                    .map(Some)
                    .chain(iter::repeat_with(|| None)),
            )
            .map_while(move |pair| match pair {
                (Some(input), Some(index)) => Some(Task {
                    input,
                    ctx: Context::new(index, self.suspended.clone()),
                    //attempt: 0,
                    outcome_tx: outcome_tx.clone(),
                }),
                (Some(input), None) => Some(self.prepare_task(input, outcome_tx.clone())),
                (None, Some(_)) => panic!("batch contained fewer than {min_size} items"),
                (None, None) => None,
            })
    }

    /// Called by a worker thread after completing a task. Notifies any thread that has `join`ed
    /// the `Hive` if there is no more work to be done.
    pub fn finish_task(&self, panicking: bool) {
        self.num_tasks_active.sub(1);
        if panicking {
            self.num_panics.add(1);
        }
        self.no_work_notify_all();
    }

    /// Returns `true` if there are either active tasks or if there are queued tasks and the
    /// cancelled flag hasn't been set.
    #[inline]
    pub fn has_work(&self) -> bool {
        self.num_tasks_active.get() > 0 || (!self.is_suspended() && self.num_tasks_queued.get() > 0)
    }

    /// Notify all observers joining this hive when there is no more work to do.
    pub fn no_work_notify_all(&self) {
        if !self.has_work() {
            self.join_condvar.notify_all();
        }
    }

    /// Sets the `cancelled` flag. Worker threads may terminate early. No new worker threads will
    /// be spawned. Returns `true` if the value was changed.
    pub fn set_suspended(&self, suspended: bool) -> bool {
        if self.suspended.set(suspended) == suspended {
            false
        } else {
            if !suspended {
                self.suspended_condvar.notify_all();
            }
            true
        }
    }

    /// Returns `true` if the `cancelled` flag has been set.
    pub fn is_suspended(&self) -> bool {
        self.suspended.get()
    }

    /// Returns a mutable reference to the retained task outcomes.
    pub fn outcomes(&self) -> impl DerefMut<Target = HashMap<usize, Outcome<W>>> + '_ {
        self.outcomes.lock()
    }

    /// Adds a new outcome to the retained task outcomes.
    pub fn add_outcome(&self, outcome: Outcome<W>) {
        let mut lock = self.outcomes.lock();
        lock.insert(*outcome.index(), outcome);
    }

    /// Removes and returns all retained task outcomes.
    pub fn take_outcomes(&self) -> HashMap<usize, Outcome<W>> {
        let mut lock = self.outcomes.lock();
        mem::take(&mut *lock)
    }

    /// Removes and returns all retained `Unprocessed` outcomes.
    pub fn take_unprocessed(&self) -> Vec<Outcome<W>> {
        let mut outcomes = self.outcomes.lock();
        let unprocessed_indices: Vec<_> = outcomes
            .keys()
            .cloned()
            .filter(|index| matches!(outcomes.get(index), Some(Outcome::Unprocessed { .. })))
            .collect();
        unprocessed_indices
            .into_iter()
            .map(|index| outcomes.remove(&index).unwrap())
            .collect()
    }

    /// Blocks the current thread until all active tasks have been processed. Also waits until all
    /// queued tasks have been processed unless the suspended flag has been set.
    pub fn wait_on_done(&self) {
        self.join_condvar.wait_while(|| self.has_work());
    }
}

impl<W: Worker, Q: Queen<Kind = W>> fmt::Debug for Shared<W, Q> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Shared")
            .field("name", &self.config.thread_name)
            .field("num_threads", &self.config.num_threads)
            .field("num_tasks_queued", &self.num_tasks_queued)
            .field("num_tasks_active", &self.num_tasks_active)
            .finish()
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use crate::bee::{Queen, Worker};
    use crate::hive::cores::{Core, Cores};
    use crate::hive::Shared;

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        /// Adds cores to which worker threads may be pinned.
        pub fn add_core_affinity(&self, new_cores: &Cores) {
            let _ = self.config.affinity.try_update_with(|mut affinity| {
                let updated = affinity.union(new_cores) > 0;
                updated.then_some(affinity)
            });
        }

        /// Returns the `Core` to which the specified worker thread may be pinned, if any.
        pub fn get_core_affinity(&self, thread_index: usize) -> Option<Core> {
            self.config
                .affinity
                .get()
                .and_then(|cores| cores.get(thread_index))
        }
    }
}

fn insert_unprocessed_into<W: Worker>(task: Task<W>, outcomes: &mut HashMap<usize, Outcome<W>>) {
    let (value, ctx, _) = task.into_parts();
    outcomes.insert(
        ctx.index(),
        Outcome::Unprocessed {
            input: value,
            index: ctx.index(),
        },
    );
}

// time to wait in between polling the retry queue and then the task receiver
const RECV_TIMEOUT: Duration = Duration::from_secs(1);

fn drain_task_receiver_into<W: Worker>(
    rx: TaskReceiver<W>,
    outcomes: &mut HashMap<usize, Outcome<W>>,
) {
    iter::from_fn(|| rx.try_recv().ok()).for_each(|task| insert_unprocessed_into(task, outcomes));
}

#[cfg(not(feature = "retry"))]
mod no_retry {
    use crate::atomic::{Atomic, AtomicNumber};
    use crate::bee::{Queen, Worker};
    use crate::hive::{Husk, Shared, Task};
    use std::sync::mpsc::RecvTimeoutError;

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        /// Returns the next queued `Task`. The thread blocks until a new task becomes available, and
        /// since this requires holding a lock on the task `Reciever`, this also blocks any other
        /// threads that call this method. Returns `None` if the task `Sender` has hung up and there
        /// are no tasks queued. Also returns `None` if the cancelled flag has been set.
        pub fn next_task(&self) -> Option<Task<W>> {
            loop {
                self.suspended_condvar.wait_while(|| self.is_suspended());

                match self.task_rx.lock().recv_timeout(super::RECV_TIMEOUT) {
                    Ok(task) => break Some(task),
                    Err(RecvTimeoutError::Disconnected) => break None,
                    Err(RecvTimeoutError::Timeout) => continue,
                }
            }
            .inspect(|_| {
                self.num_tasks_queued.sub(1);
                self.num_tasks_active.add(1);
            })
        }

        /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
        /// outcomes, and all configuration information necessary to create a new `Hive`. Any queued
        /// tasks are converted into `Outcome::Unprocessed` outcomes.
        pub fn into_husk(self) -> Husk<W, Q> {
            let task_rx = self.task_rx.into_inner();
            let mut outcomes = self.outcomes.into_inner();
            super::drain_task_receiver_into(task_rx, &mut outcomes);
            Husk::new(
                self.config.into_unsync(),
                self.queen.into_inner(),
                self.num_panics.into_inner(),
                outcomes,
            )
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use crate::atomic::{Atomic, AtomicNumber};
    use crate::bee::{Context, Queen, Worker};
    use crate::hive::{Husk, OutcomeSender, Shared, Task};
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::{Duration, Instant};

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        /// Returns `true` if the hive is configured to retry tasks.
        pub fn can_retry(&self, ctx: &Context) -> bool {
            self.config
                .max_retries
                .get()
                .map(|max_retries| ctx.attempt() < max_retries)
                .unwrap_or(false)
        }

        fn update_next_retry(&self, instant: Option<Instant>) {
            let mut next_retry = self.next_retry.write();
            if let Some(new_val) = instant {
                if next_retry.map(|cur_val| new_val < cur_val).unwrap_or(true) {
                    next_retry.replace(new_val);
                }
            } else {
                next_retry.take();
            }
        }

        pub fn queue_retry(
            &self,
            input: W::Input,
            ctx: Context,
            outcome_tx: Option<OutcomeSender<W>>,
        ) {
            let delay = self
                .config
                .retry_factor
                .get()
                .map(|retry_factor| {
                    2u64.checked_pow(ctx.attempt() - 1)
                        .and_then(|multiplier| {
                            retry_factor
                                .checked_mul(multiplier)
                                .or(Some(u64::MAX))
                                .map(Duration::from_nanos)
                        })
                        .unwrap()
                })
                .unwrap_or_default();
            let task = Task::new(input, ctx, outcome_tx);
            let mut queue = self.retry_queue.lock();
            self.num_tasks_queued.add(1);
            let available_at = queue.push(task, delay);
            self.update_next_retry(Some(available_at));
        }

        /// Returns the next queued `Task`. The thread blocks until a new task becomes available, and
        /// since this requires holding a lock on the task `Reciever`, this also blocks any other
        /// threads that call this method. Returns `None` if the task `Sender` has hung up and there
        /// are no tasks queued for retry.
        pub fn next_task(&self) -> Option<Task<W>> {
            loop {
                self.suspended_condvar.wait_while(|| self.is_suspended());

                let has_retry = {
                    let next_retry = self.next_retry.read();
                    next_retry.is_some_and(|next_retry| next_retry <= Instant::now())
                };
                if has_retry {
                    let mut queue = self.retry_queue.lock();
                    if let Some(task) = queue.try_pop() {
                        self.update_next_retry(queue.next_available());
                        break Some(task);
                    }
                }
                match self.task_rx.lock().recv_timeout(super::RECV_TIMEOUT) {
                    Ok(task) => break Some(task),
                    Err(RecvTimeoutError::Disconnected) => break None,
                    Err(RecvTimeoutError::Timeout) => continue,
                }
            }
            .inspect(|_| {
                self.num_tasks_queued.sub(1);
                self.num_tasks_active.add(1);
            })
        }

        /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
        /// outcomes, and all configuration information necessary to create a new `Hive`. Any queued
        /// tasks are converted into `Outcome::Unprocessed` outcomes.
        pub fn into_husk(self) -> Husk<W, Q> {
            let task_rx = self.task_rx.into_inner();
            let retry_queue = self.retry_queue.into_inner();
            let mut outcomes = self.outcomes.into_inner();
            super::drain_task_receiver_into(task_rx, &mut outcomes);
            retry_queue
                .into_iter()
                .for_each(|task| super::insert_unprocessed_into(task, &mut outcomes));
            Husk::new(
                self.config.into_unsync(),
                self.queen.into_inner(),
                self.num_panics.into_inner(),
                outcomes,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bee::stock::ThunkWorker;
    use crate::bee::DefaultQueen;

    type VoidThunkWorker = ThunkWorker<()>;
    type VoidThunkWorkerShared = super::Shared<VoidThunkWorker, DefaultQueen<VoidThunkWorker>>;

    #[test]
    fn test_sync_shared() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<VoidThunkWorkerShared>();
    }

    #[test]
    fn test_send_shared() {
        fn assert_send<T: Send>() {}
        assert_send::<VoidThunkWorkerShared>();
    }
}
