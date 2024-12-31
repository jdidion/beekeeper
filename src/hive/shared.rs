use super::{Config, Outcome, OutcomeSender, Shared, Task, TaskReceiver};
use crate::atomic::{Atomic, AtomicNumber};
use crate::task::{Context, Queen, Worker};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::thread::Builder;
use std::{fmt, iter, mem};

impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
    pub fn new(config: Config, queen: Q, task_rx: TaskReceiver<W>) -> Self {
        Shared {
            config,
            queen: Mutex::new(queen),
            task_rx: Mutex::new(task_rx),
            panic_count: Default::default(),
            queued_count: Default::default(),
            active_count: Default::default(),
            next_index: Default::default(),
            suspended: Default::default(),
            outcomes: Default::default(),
            empty_condvar: Default::default(),
            empty_trigger: Default::default(),
            join_generation: Default::default(),
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
        self.config.num_threads.add_update(num_threads)
    }

    /// Ensures that the number of threads is at least `num_threads`. Returns the previous value.
    pub fn ensure_threads(&self, num_threads: usize) -> usize {
        self.config.num_threads.set_max(num_threads)
    }

    /// Returns a new `Worker` from the queen, or an error if a `Worker` could not be created.
    pub fn create_worker(&self) -> Q::Kind {
        self.queen.lock().create()
    }

    /// Increments the number of queued tasks. Returns a new `Task` with the provided input and
    /// `outcome_tx` and the next index.
    pub fn prepare_task(&self, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> Task<W> {
        self.queued_count.add(1);
        let index = self.next_index.add(1);
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
        self.queued_count.add(min_size);
        let index_start = self.next_index.add(min_size);
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
        self.active_count.sub(1);
        if panicking {
            self.panic_count.add(1);
        }
        self.no_work_notify_all();
    }

    /// Returns `true` if there are either active tasks or if there are queued tasks and the
    /// cancelled flag hasn't been set.
    pub fn has_work(&self) -> bool {
        self.active_count.get() > 0 || (!self.is_suspended() && self.queued_count.get() > 0)
    }

    /// Notify all observers joining this hive if there is no more work to do.
    pub fn no_work_notify_all(&self) {
        if !self.has_work() {
            let _lock = self.empty_trigger.lock();
            self.empty_condvar.notify_all();
        }
    }

    /// Sets the `cancelled` flag. Worker threads may terminate early. No new worker threads will
    /// be spawned. Returns `true` if the value was changed.
    pub fn set_suspended(&self, suspended: bool) -> bool {
        let old_val = self.suspended.set(suspended);
        old_val == suspended
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
        lock.insert(outcome.index(), outcome);
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
            .filter(|index| matches!(outcomes.get(&index), Some(Outcome::Unprocessed { .. })))
            .collect();
        unprocessed_indices
            .into_iter()
            .map(|index| outcomes.remove(&index).unwrap())
            .collect()
    }

    /// Blocks the current thread until all active tasks have been processed. Also waits until all
    /// queued tasks have been processed unless the cancelled flag has been set.
    pub fn wait_on_done(&self) {
        if self.has_work() {
            let generation = self.join_generation.get();
            let mut lock = self.empty_trigger.lock();
            while generation == self.join_generation.get() && self.has_work() {
                self.empty_condvar.wait(&mut lock);
            }
            // increase generation for the first thread to come out of the loop
            let _ = self
                .join_generation
                .set_when(generation, generation.wrapping_add(1));
        }
    }
}

impl<W: Worker, Q: Queen<Kind = W>> fmt::Debug for Shared<W, Q> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Shared")
            .field("name", &self.config.thread_name)
            .field("queued_count", &self.queued_count)
            .field("active_count", &self.active_count)
            .field("max_count", &self.config.num_threads)
            .finish()
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use crate::hive::cores::{Core, Cores};
    use crate::hive::Shared;
    use crate::task::{Queen, Worker};

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

fn drain_task_receiver_into<W: Worker>(
    rx: TaskReceiver<W>,
    outcomes: &mut HashMap<usize, Outcome<W>>,
) {
    iter::from_fn(|| rx.try_recv().ok()).for_each(|task| insert_unprocessed_into(task, outcomes));
}

#[cfg(not(feature = "retry"))]
mod no_retry {
    use crate::atomic::AtomicNumber;
    use crate::hive::{Shared, Task};
    use crate::task::{Context, Queen, Worker};

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        /// Returns the next queued `Task`. The thread blocks until a new task becomes available, and
        /// since this requires holding a lock on the task `Reciever`, this also blocks any other
        /// threads that call this method. Returns `None` if the task `Sender` has hung up and there
        /// are no tasks queued. Also returns `None` if the cancelled flag has been set.
        fn next_task(&self) -> Option<Task<W>> {
            if self.is_suspended() {
                None
            } else {
                self.task_rx.lock().recv().ok().inspect(|_| {
                    self.active_count.add(1);
                    self.queued_count.sub(1);
                })
            }
        }

        /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
        /// outcomes, and all configuration information necessary to create a new `Hive`. Any queued
        /// tasks are converted into `Outcome::Unprocessed` outcomes.
        pub fn into_husk(self) -> Husk<W, Q> {
            let task_rx = self.task_rx.into_inner();
            let mut outcomes = self.outcomes.into_inner();
            drain_task_receiver_into(task_rx, &mut outcomes);
            Husk::new(
                self.config.into_unsync(),
                self.queen.into_inner(),
                self.panic_count.into_inner(),
                outcomes,
            )
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use crate::atomic::{Atomic, AtomicNumber};
    use crate::hive::{Husk, OutcomeSender, Shared, Task};
    use crate::task::{Context, Queen, Worker};
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
            let mut next_retry = self.next_retry.write().unwrap();
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
                                .or_else(|| Some(u64::MAX))
                                .map(Duration::from_nanos)
                        })
                        .unwrap()
                })
                .unwrap_or_default();
            let task = Task::new(input, ctx, outcome_tx);
            let mut queue = self.retry_queue.lock();
            self.queued_count.add(1);
            let available_at = queue.push(task, delay);
            self.update_next_retry(Some(available_at));
        }

        /// Returns the next queued `Task`. The thread blocks until a new task becomes available, and
        /// since this requires holding a lock on the task `Reciever`, this also blocks any other
        /// threads that call this method. Returns `None` if the task `Sender` has hung up and there
        /// are no tasks queued. Also returns `None` if the cancelled flag has been set.
        pub fn next_task(&self) -> Option<Task<W>> {
            // time to wait in between polling the retry queue and then the task receiver
            const RECV_TIMEOUT: Duration = Duration::from_secs(1);

            if self.is_suspended() {
                return None;
            }

            loop {
                let has_retry = {
                    let next_retry = self.next_retry.read().unwrap();
                    next_retry.is_some_and(|next_retry| next_retry <= Instant::now())
                };
                if has_retry {
                    let mut queue = self.retry_queue.lock();
                    if let Some(task) = queue.try_pop() {
                        self.update_next_retry(queue.next_available());
                        break Some(task);
                    }
                }
                match self.task_rx.lock().recv_timeout(RECV_TIMEOUT) {
                    Ok(task) => break Some(task),
                    Err(RecvTimeoutError::Disconnected) => break None,
                    Err(RecvTimeoutError::Timeout) => continue,
                }
            }
            .inspect(|_| {
                self.active_count.add(1);
                self.queued_count.sub(1);
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
                self.panic_count.into_inner(),
                outcomes,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::task::DefaultQueen;
    use crate::util::ThunkWorker;

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
