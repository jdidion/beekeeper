use super::counter::CounterError;
use super::{Config, Outcome, OutcomeSender, Shared, SpawnError, Task, TaskReceiver};
use crate::atomic::{Atomic, AtomicInt, AtomicUsize};
use crate::bee::{Context, Queen, TaskId, Worker};
use crate::channel::SenderExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::mpsc::RecvTimeoutError;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use std::{fmt, iter, mem};

impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
    /// Creates a new `Shared` instance with the given configuration, queen, and task receiver,
    /// and all other fields set to their default values.
    pub fn new(config: Config, queen: Q, task_rx: TaskReceiver<W>) -> Self {
        Shared {
            config,
            queen: Mutex::new(queen),
            task_rx: Mutex::new(task_rx),
            spawn_results: Default::default(),
            num_tasks: Default::default(),
            next_task_id: Default::default(),
            num_panics: Default::default(),
            num_referrers: AtomicUsize::new(1),
            poisoned: Default::default(),
            suspended: Default::default(),
            resume_gate: Default::default(),
            join_gate: Default::default(),
            outcomes: Default::default(),
            #[cfg(feature = "batching")]
            local_queues: Default::default(),
            #[cfg(feature = "retry")]
            retry_queues: Default::default(),
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

    /// Spawns the initial set of `self.config.num_threads` worker threads using the provided
    /// spawning function. Returns the number of worker threads that were successfully started.
    pub fn init_threads<F>(&self, f: F) -> usize
    where
        F: Fn(usize) -> Result<JoinHandle<()>, SpawnError>,
    {
        let num_threads = self.config.num_threads.get_or_default();
        if num_threads == 0 {
            return 0;
        }
        let mut spawn_results = self.spawn_results.lock();
        self.spawn_threads(0, num_threads, f, &mut spawn_results)
    }

    /// Increases the maximum number of threads allowed in the `Hive` by `num_threads`, and
    /// attempts to spawn threads with indices in `range = cur_index..cur_index + num_threads`
    /// using the provided spawning function. The results are stored in `self.spawn_results[range]`.
    /// Returns the number of new worker threads that were successfully started.
    pub fn grow_threads<F>(&self, num_threads: usize, f: F) -> usize
    where
        F: Fn(usize) -> Result<JoinHandle<()>, SpawnError>,
    {
        let mut spawn_results = self.spawn_results.lock();
        let start_index = self.config.num_threads.add(num_threads).unwrap();
        self.spawn_threads(start_index, num_threads, f, &mut spawn_results)
    }

    fn spawn_threads<F>(
        &self,
        start_index: usize,
        num_threads: usize,
        f: F,
        spawn_results: &mut Vec<Result<JoinHandle<()>, SpawnError>>,
    ) -> usize
    where
        F: Fn(usize) -> Result<JoinHandle<()>, SpawnError>,
    {
        assert_eq!(spawn_results.len(), start_index);
        let end_index = start_index + num_threads;
        // if worker threads need a local queue, initialize them before spawning
        #[cfg(feature = "batching")]
        self.init_local_queues(start_index, end_index);
        #[cfg(feature = "retry")]
        self.init_retry_queues(start_index, end_index);
        // spawn the worker threads and return the results
        let results: Vec<_> = (start_index..end_index).map(f).collect();
        spawn_results.reserve(num_threads);
        results
            .into_iter()
            .map(|result| {
                let started = result.is_ok();
                spawn_results.push(result);
                started
            })
            .filter(|started| *started)
            .count()
    }

    /// Attempts to spawn a thread to replace the one at the specified `index` using the provided
    /// spawning function. The result is stored in `self.spawn_results[index]`. Returns the
    /// spawn result for the previous thread at the same index.
    pub fn respawn_thread<F>(&self, index: usize, f: F) -> Result<JoinHandle<()>, SpawnError>
    where
        F: FnOnce(usize) -> Result<JoinHandle<()>, SpawnError>,
    {
        let result = f(index);
        let mut spawn_results = self.spawn_results.lock();
        assert!(spawn_results.len() > index);
        // Note: we do *not* want to wait on the `JoinHandle` for the previous thread as it may
        // still be processing a task
        std::mem::replace(&mut spawn_results[index], result)
    }

    /// Attempts to respawn any threads that are currently dead using the provided spawning
    /// function. Returns the number of threads that were successfully respawned.
    //#[cfg_attr(coverage(off)] // no idea how to test this
    pub fn respawn_dead_threads<F>(&self, f: F) -> usize
    where
        F: Fn(usize) -> Result<JoinHandle<()>, SpawnError>,
    {
        self.spawn_results
            .lock()
            .iter_mut()
            .enumerate()
            .filter(|(_, result)| result.is_err())
            .map(|(i, result)| {
                let new_result = f(i);
                let started = new_result.is_ok();
                *result = new_result;
                started
            })
            .filter(|started| *started)
            .count()
    }

    /// Returns a new `Worker` from the queen, or an error if a `Worker` could not be created.
    pub fn create_worker(&self) -> Q::Kind {
        self.queen.lock().create()
    }

    /// Increments the number of queued tasks. Returns a new `Task` with the provided input and
    /// `outcome_tx` and the next ID.
    pub fn prepare_task(&self, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> Task<W> {
        self.num_tasks
            .increment_left(1)
            .expect("overflowed queued task counter");
        let task_id = self.next_task_id.add(1);
        let ctx = Context::new(task_id, self.suspended.clone());
        Task::new(input, ctx, outcome_tx)
    }

    /// Increments the number of queued tasks by the number of provided inputs. Returns an iterator
    /// over `Task`s created from the provided inputs, `outcome_tx`s, and sequential task_ids.
    pub fn prepare_batch<'a, T: Iterator<Item = W::Input> + 'a>(
        &'a self,
        min_size: usize,
        inputs: T,
        outcome_tx: Option<OutcomeSender<W>>,
    ) -> impl Iterator<Item = Task<W>> + 'a {
        self.num_tasks
            .increment_left(min_size as u64)
            .expect("overflowed queued task counter");
        let task_id_start = self.next_task_id.add(min_size);
        let task_id_end = task_id_start + min_size;
        inputs
            .map(Some)
            .chain(iter::repeat_with(|| None))
            .zip(
                (task_id_start..task_id_end)
                    .map(Some)
                    .chain(iter::repeat_with(|| None)),
            )
            .map_while(move |pair| match pair {
                (Some(input), Some(task_id)) => Some(Task {
                    input,
                    ctx: Context::new(task_id, self.suspended.clone()),
                    //attempt: 0,
                    outcome_tx: outcome_tx.clone(),
                }),
                (Some(input), None) => Some(self.prepare_task(input, outcome_tx.clone())),
                (None, Some(_)) => panic!("batch contained fewer than {min_size} items"),
                (None, None) => None,
            })
    }

    /// Sends an outcome to `outcome_tx`, or stores it in the `Hive` shared data if there is no
    /// sender, or if the send fails.
    pub fn send_or_store_outcome(&self, outcome: Outcome<W>, outcome_tx: Option<OutcomeSender<W>>) {
        if let Some(outcome) = if let Some(tx) = outcome_tx {
            tx.try_send_msg(outcome)
        } else {
            Some(outcome)
        } {
            self.add_outcome(outcome)
        }
    }

    pub fn abandon_task(&self, task: Task<W>) {
        let (outcome, outcome_tx) = task.into_unprocessed();
        self.send_or_store_outcome(outcome, outcome_tx);
        // decrement the queued counter since it was incremented but the task was never queued
        let _ = self.num_tasks.decrement_left(1);
        self.no_work_notify_all();
    }

    /// Converts each `Task` in the iterator into `Outcome::Unprocessed` and attempts to send it
    /// to its `OutcomeSender` if there is one, or stores it if there is no sender or the send
    /// fails. Returns a vector of task_ids of the tasks.
    pub fn abandon_batch<I>(&self, tasks: I) -> Vec<TaskId>
    where
        I: Iterator<Item = Task<W>>,
    {
        // don't unlock outcomes unless we have to
        let mut outcomes = Option::None;
        let task_ids: Vec<_> = tasks
            .map(|task| {
                let task_id = task.id();
                let (outcome, outcome_tx) = task.into_unprocessed();
                if let Some(outcome) = if let Some(tx) = outcome_tx {
                    tx.try_send_msg(outcome)
                } else {
                    Some(outcome)
                } {
                    outcomes
                        .get_or_insert_with(|| self.outcomes.lock())
                        .insert(task_id, outcome);
                }
                task_id
            })
            .collect();
        // decrement the queued counter since it was incremented but the tasks were never queued
        let _ = self.num_tasks.decrement_left(task_ids.len() as u64);
        self.no_work_notify_all();
        task_ids
    }

    /// Called by a worker thread after completing a task. Notifies any thread that has `join`ed
    /// the `Hive` if there is no more work to be done.
    #[inline]
    pub fn finish_task(&self, panicking: bool) {
        self.finish_tasks(1, panicking);
    }

    pub fn finish_tasks(&self, n: u64, panicking: bool) {
        self.num_tasks
            .decrement_right(n)
            .expect("active task counter was smaller than expected");
        if panicking {
            self.num_panics.add(1);
        }
        self.no_work_notify_all();
    }

    /// Returns a tuple with the number of (queued, active) tasks.
    #[inline]
    pub fn num_tasks(&self) -> (u64, u64) {
        self.num_tasks.get()
    }

    /// Returns `true` if the hive has not been poisoned and there are either active tasks or there
    /// are queued tasks and the cancelled flag hasn't been set.
    #[inline]
    pub fn has_work(&self) -> bool {
        !self.is_poisoned() && {
            let (queued, active) = self.num_tasks();
            active > 0 || (!self.is_suspended() && queued > 0)
        }
    }

    /// Blocks the current thread until all active tasks have been processed. Also waits until all
    /// queued tasks have been processed unless the suspended flag has been set.
    pub fn wait_on_done(&self) {
        self.join_gate.wait_while(|| self.has_work());
    }

    /// Notify all observers joining this hive when all tasks have been completed.
    pub fn no_work_notify_all(&self) {
        if !self.has_work() {
            self.join_gate.notify_all();
        }
    }

    /// Returns the number of `Hive`s holding a reference to this shared data.
    pub fn num_referrers(&self) -> usize {
        self.num_referrers.get()
    }

    /// Increments the number of referrers and returns the previous value.
    pub fn referrer_is_cloning(&self) -> usize {
        self.num_referrers.add(1)
    }

    /// Decrements the number of referrers and returns the previous value.
    pub fn referrer_is_dropping(&self) -> usize {
        self.num_referrers.sub(1)
    }

    /// Sets the `poisoned` flag to `true`. Converts all queued tasks to `Outcome::Unprocessed`
    /// and stores them in `outcomes`. Also automatically resumes the hive if it is suspendend,
    /// which enables blocked worker threads to terminate.
    pub fn poison(&self) {
        self.poisoned.set(true);
        self.drain_tasks_into_unprocessed();
        self.set_suspended(false);
    }

    /// Returns `true` if the hive has been poisoned. A poisoned have may accept new tasks but will
    /// never process them. Unprocessed tasks can be retrieved by calling `take_outcomes` or
    /// `try_into_husk`.
    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.get()
    }

    /// Sets the `suspended` flag. If `true`, worker threads may terminate early, and no new tasks
    /// will be started until this flag is set to `false`. Returns `true` if the value was changed.
    pub fn set_suspended(&self, suspended: bool) -> bool {
        if self.suspended.set(suspended) == suspended {
            false
        } else {
            if !suspended {
                self.resume_gate.notify_all();
            }
            true
        }
    }

    /// Returns `true` if the `suspended` flag has been set.
    #[inline]
    pub fn is_suspended(&self) -> bool {
        self.suspended.get()
    }

    /// Returns a mutable reference to the retained task outcomes.
    pub fn outcomes(&self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> + '_ {
        self.outcomes.lock()
    }

    /// Adds a new outcome to the retained task outcomes.
    pub fn add_outcome(&self, outcome: Outcome<W>) {
        let mut lock = self.outcomes.lock();
        lock.insert(*outcome.task_id(), outcome);
    }

    /// Removes and returns all retained task outcomes.
    pub fn take_outcomes(&self) -> HashMap<TaskId, Outcome<W>> {
        let mut lock = self.outcomes.lock();
        mem::take(&mut *lock)
    }

    /// Removes and returns all retained `Unprocessed` outcomes.
    pub fn take_unprocessed(&self) -> Vec<Outcome<W>> {
        let mut outcomes = self.outcomes.lock();
        let unprocessed_task_ids: Vec<_> = outcomes
            .keys()
            .cloned()
            .filter(|task_id| matches!(outcomes.get(task_id), Some(Outcome::Unprocessed { .. })))
            .collect();
        unprocessed_task_ids
            .into_iter()
            .map(|task_id| outcomes.remove(&task_id).unwrap())
            .collect()
    }
}

impl<W: Worker, Q: Queen<Kind = W>> fmt::Debug for Shared<W, Q> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (queued, active) = self.num_tasks();
        f.debug_struct("Shared")
            .field("name", &self.config.thread_name)
            .field("num_threads", &self.config.num_threads)
            .field("num_tasks_queued", &queued)
            .field("num_tasks_active", &active)
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
        pub fn add_core_affinity(&self, new_cores: Cores) {
            let _ = self.config.affinity.try_update_with(|mut affinity| {
                let updated = affinity.union(&new_cores) > 0;
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

#[inline]
fn task_recv_timeout<W: Worker>(rx: &TaskReceiver<W>) -> Option<Result<Task<W>, NextTaskError>> {
    // time to wait in between polling the retry queue and then the task receiver
    const RECV_TIMEOUT: Duration = Duration::from_secs(1);
    match rx.recv_timeout(RECV_TIMEOUT) {
        Ok(task) => Some(Ok(task)),
        Err(RecvTimeoutError::Disconnected) => Some(Err(NextTaskError::Disconnected)),
        Err(RecvTimeoutError::Timeout) => None,
    }
}

#[cfg(not(feature = "batching"))]
mod no_batching {
    use super::{NextTaskError, Shared, Task};
    use crate::bee::{Queen, Worker};

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        /// Tries to receive a task from the input channel.
        ///
        /// Returns an error if the channel has disconnected. Returns `None` if a task is not
        /// received within the timeout period (currently hard-coded to 1 second).
        #[inline]
        pub(super) fn get_task(&self, _: usize) -> Option<Result<Task<W>, NextTaskError>> {
            super::task_recv_timeout(&self.task_rx.lock())
        }
    }
}

#[cfg(feature = "batching")]
mod batching {
    use super::{NextTaskError, Shared, Task};
    use crate::bee::{Queen, Worker};
    use crossbeam_queue::ArrayQueue;
    use std::collections::HashSet;
    use std::time::Duration;

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        pub(super) fn init_local_queues(&self, start_index: usize, end_index: usize) {
            let mut local_queues = self.local_queues.write();
            assert_eq!(local_queues.len(), start_index);
            // ArrayQueue cannot be zero-sized
            let queue_size = self.batch_size().max(1);
            (start_index..end_index).for_each(|_| local_queues.push(ArrayQueue::new(queue_size)))
        }

        /// Returns the local queue batch size.
        pub fn batch_size(&self) -> usize {
            self.config.batch_size.get().unwrap_or_default()
        }

        /// Changes the local queue batch size. This requires allocating a new queue for each
        /// worker thread.
        ///
        /// Note: this method will block the current thread waiting for all local queues to become
        /// writable; if `batch_size` is less than the current batch size, this method will also
        /// block while any thread's queue length is > `batch_size` before moving the elements.
        pub fn set_batch_size(&self, batch_size: usize) -> usize {
            // update the batch size first so any new threads spawned won't need to have their
            // queues resized
            let prev_batch_size = self
                .config
                .batch_size
                .try_set(batch_size)
                .unwrap_or_default();
            if prev_batch_size == batch_size {
                return prev_batch_size;
            }
            let num_threads = self.config.num_threads.get_or_default();
            if num_threads == 0 {
                return prev_batch_size;
            }
            // keep track of which queues need to be resized
            // TODO: this method could cause a hang if one of the worker threads is stuck - we
            // might want to keep track of each queue's size and if we don't see it shrink within
            // a certain amount of time, we give up on that thread and leave it with a wrong-sized
            // queue (which should never cause a panic)
            let mut to_resize: HashSet<usize> = (0..num_threads).collect();
            // iterate until we've resized them all
            loop {
                // scope the mutable access to local_queues
                {
                    let mut local_queues = self.local_queues.write();
                    to_resize.retain(|thread_index| {
                        let queue = if let Some(queue) = local_queues.get_mut(*thread_index) {
                            queue
                        } else {
                            return false;
                        };
                        if queue.len() > batch_size {
                            return true;
                        }
                        let new_queue = ArrayQueue::new(batch_size);
                        while let Some(task) = queue.pop() {
                            if let Err(task) = new_queue.push(task) {
                                // for some reason we can't push the task to the new queue
                                // this should never happen, but just in case we turn it into
                                // an unprocessed outcome
                                self.abandon_task(task);
                            }
                        }
                        // this is safe because the worker threads can't get readable access to the
                        // queue while this thread holds the lock
                        let old_queue = std::mem::replace(queue, new_queue);
                        assert!(old_queue.is_empty());
                        false
                    });
                }
                if to_resize.is_empty() {
                    return prev_batch_size;
                } else {
                    // short sleep to give worker threads the chance to pull from their queues
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        /// Returns the next task from the local queue if there are any, otherwise attempts to
        /// fetch at least 1 and up to `batch_size + 1` tasks from the input channel and puts all
        /// but the first one into the local queue.
        #[inline]
        pub(super) fn get_task(
            &self,
            thread_index: usize,
        ) -> Option<Result<Task<W>, NextTaskError>> {
            let local_queue = &self.local_queues.read()[thread_index];
            // pop from the local queue if it has any tasks
            if !local_queue.is_empty() {
                return Some(Ok(local_queue.pop().unwrap()));
            }
            // otherwise pull at least 1 and up to `batch_size + 1` tasks from the input channel
            let task_rx = self.task_rx.lock();
            // wait for the next task from the receiver
            let first = super::task_recv_timeout(&task_rx);
            // if we fail after trying to get one, don't keep trying to fill the queue
            if first.as_ref().map(|result| result.is_ok()).unwrap_or(false) {
                let batch_size = self.batch_size();
                // batch size 0 means batching is disabled
                if batch_size > 0 {
                    // otherwise try to take up to `batch_size` tasks from the input channel
                    // and add them to the local queue, but don't block if the input channel
                    // is empty
                    for result in task_rx
                        .try_iter()
                        .take(batch_size)
                        .map(|task| local_queue.push(task))
                    {
                        if let Err(task) = result {
                            // for some reason we can't push the task to the local queue;
                            // this should never happen, but just in case we turn it into an
                            // unprocessed outcome and stop iterating
                            self.abandon_task(task);
                            break;
                        }
                    }
                }
            }
            first
        }
    }
}

/// Sends each `Task` to its associated outcome sender (if any) or stores it in `outcomes`.
/// TODO: if `outcomes` were `DerefMut` then the argument could either be a mutable referece or
/// a Lazy<Mutex> that aquires the lock on first access. Unfortunately, rust's Lazy does not support
/// mutable access, so we'd need something like OnceCell or OnceMutex.
fn send_or_store<W: Worker, I: Iterator<Item = Task<W>>>(
    tasks: I,
    outcomes: &mut HashMap<TaskId, Outcome<W>>,
) {
    tasks.for_each(|task| {
        let (outcome, outcome_tx) = task.into_unprocessed();
        if let Some(outcome) = if let Some(tx) = outcome_tx {
            tx.try_send_msg(outcome)
        } else {
            Some(outcome)
        } {
            outcomes.insert(*outcome.task_id(), outcome);
        }
    });
}

#[derive(thiserror::Error, Debug)]
pub enum NextTaskError {
    #[error("Task receiver disconnected")]
    Disconnected,
    #[error("The hive has been poisoned")]
    Poisoned,
    #[error("Task counter has invalid state")]
    InvalidCounter(CounterError),
}

#[cfg(not(feature = "retry"))]
mod no_retry {
    use super::{NextTaskError, Task};
    use crate::atomic::Atomic;
    use crate::bee::{Queen, Worker};
    use crate::hive::{Husk, Shared};

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        /// Returns the next queued `Task`. The thread blocks until a new task becomes available, and
        /// since this requires holding a lock on the task `Reciever`, this also blocks any other
        /// threads that call this method. Returns `None` if the task `Sender` has hung up and there
        /// are no tasks queued. Also returns `None` if the cancelled flag has been set.
        pub fn next_task(&self, thread_index: usize) -> Result<Task<W>, NextTaskError> {
            loop {
                self.resume_gate.wait_while(|| self.is_suspended());

                if self.is_poisoned() {
                    return Err(NextTaskError::Poisoned);
                }

                if let Some(result) = self.get_task(thread_index) {
                    break result;
                }
            }
            .and_then(|task| match self.num_tasks.transfer(1) {
                Ok(_) => Ok(task),
                Err(e) => {
                    // poison the hive so it can't be used anymore
                    self.poison();
                    Err(NextTaskError::InvalidCounter(e))
                }
            })
        }

        /// Drains all queued tasks, converts them into `Outcome::Unprocessed` outcomes, and tries
        /// to send them or (if the task does not have a sender, or if the send fails) stores them
        /// in the `outcomes` map.
        pub fn drain_tasks_into_unprocessed(&self) {
            let task_rx = self.task_rx.lock();
            let mut outcomes = self.outcomes.lock();
            super::send_or_store(task_rx.try_iter(), &mut outcomes);
        }

        /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
        /// outcomes, and all configuration information necessary to create a new `Hive`. Any queued
        /// tasks are converted into `Outcome::Unprocessed` outcomes and either sent to the task's
        /// sender or (if there is no sender, or the send fails) stored in the `outcomes` map.
        pub fn try_into_husk(self) -> Husk<W, Q> {
            let task_rx = self.task_rx.into_inner();
            let mut outcomes = self.outcomes.into_inner();
            super::send_or_store(task_rx.try_iter(), &mut outcomes);
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
    use super::NextTaskError;
    use crate::atomic::Atomic;
    use crate::bee::{Context, Queen, Worker};
    use crate::hive::delay::DelayQueue;
    use crate::hive::{Husk, OutcomeSender, Shared, Task};
    use std::time::{Duration, Instant};

    impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
        /// Initializes the retry queues worker threads in the specified range.
        pub(super) fn init_retry_queues(&self, start_index: usize, end_index: usize) {
            let mut retry_queues = self.retry_queues.write();
            assert_eq!(retry_queues.len(), start_index);
            (start_index..end_index).for_each(|_| retry_queues.push(DelayQueue::default()))
        }

        /// Returns `true` if the hive is configured to retry tasks and the `attempt` field of the
        /// given `ctx` is less than the maximum number of retries.
        pub fn can_retry(&self, ctx: &Context) -> bool {
            self.config
                .max_retries
                .get()
                .map(|max_retries| ctx.attempt() < max_retries)
                .unwrap_or(false)
        }

        /// Adds a task to the retry queue with a delay based on `ctx.attempt()`.
        pub fn queue_retry(
            &self,
            thread_index: usize,
            input: W::Input,
            ctx: Context,
            outcome_tx: Option<OutcomeSender<W>>,
        ) -> Option<Instant> {
            // compute the delay
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
            // try to queue the task
            let task = Task::new(input, ctx, outcome_tx);
            self.num_tasks
                .increment_left(1)
                .expect("overflowed queued task counter");
            if let Some(queue) = self.retry_queues.read().get(thread_index) {
                queue.push(task, delay)
            } else {
                Err(task)
            }
            // if unable to queue the task, abandon it
            .map_err(|task| self.abandon_task(task))
            .ok()
        }

        /// Returns the next queued `Task`. The thread blocks until a new task becomes available,
        /// and since this requires holding a lock on the task `Reciever`, this also blocks any
        /// other threads that call this method. Returns an error if the task `Sender` has hung up
        /// and there are no tasks queued for retry.
        pub fn next_task(&self, thread_index: usize) -> Result<Task<W>, NextTaskError> {
            loop {
                self.resume_gate.wait_while(|| self.is_suspended());

                if self.is_poisoned() {
                    return Err(NextTaskError::Poisoned);
                }

                if let Some(task) = self
                    .retry_queues
                    .read()
                    .get(thread_index)
                    .and_then(|queue| queue.try_pop())
                {
                    break Ok(task);
                }

                if let Some(result) = self.get_task(thread_index) {
                    break result;
                }
            }
            .and_then(|task| match self.num_tasks.transfer(1) {
                Ok(_) => Ok(task),
                Err(e) => Err(NextTaskError::InvalidCounter(e)),
            })
        }

        /// Drains all queued tasks, converts them into `Outcome::Unprocessed` outcomes, and tries
        /// to send them or (if the task does not have a sender, or if the send fails) stores them
        /// in the `outcomes` map.
        pub fn drain_tasks_into_unprocessed(&self) {
            let mut outcomes = self.outcomes.lock();
            let task_rx = self.task_rx.lock();
            super::send_or_store(task_rx.try_iter(), &mut outcomes);
            let mut retry_queue = self.retry_queues.write();
            for queue in retry_queue.iter_mut() {
                super::send_or_store(queue.drain(), &mut outcomes);
            }
        }

        /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
        /// outcomes, and all configuration information necessary to create a new `Hive`. Any queued
        /// tasks are converted into `Outcome::Unprocessed` outcomes and either sent to the task's
        /// sender or (if there is no sender, or the send fails) stored in the `outcomes` map.
        pub fn try_into_husk(self) -> Husk<W, Q> {
            let mut outcomes = self.outcomes.into_inner();
            let task_rx = self.task_rx.into_inner();
            super::send_or_store(task_rx.try_iter(), &mut outcomes);
            let mut retry_queue = self.retry_queues.into_inner();
            for queue in retry_queue.iter_mut() {
                super::send_or_store(queue.drain(), &mut outcomes);
            }
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
