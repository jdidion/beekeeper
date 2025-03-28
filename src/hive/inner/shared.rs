use super::{Config, PopTaskError, Shared, Task, TaskInput, TaskQueues, Token, WorkerQueues};
use crate::atomic::{Atomic, AtomicInt, AtomicUsize};
use crate::bee::{Queen, TaskId, Worker};
use crate::channel::SenderExt;
use crate::hive::{Husk, Outcome, OutcomeSender, SpawnError};
use parking_lot::MutexGuard;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::thread::{Builder, JoinHandle};
use std::{fmt, iter};

impl<W: Worker, Q: Queen<Kind = W>, T: TaskQueues<Q::Kind>> Shared<Q, T> {
    /// Creates a new `Shared` instance with the given configuration, queen, and task receiver,
    /// and all other fields set to their default values.
    pub fn new(config: Config, queen: Q) -> Self {
        let task_queues = T::new(Token);
        Shared {
            config,
            queen,
            task_queues,
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

    /// Returns the current number of worker threads.
    pub fn num_threads(&self) -> usize {
        self.config.num_threads.get_or_default()
    }

    /// Spawns the initial set of `self.config.num_threads` worker threads using the provided
    /// spawning function. The results are stored in `self.spawn_results[0..num_threads]`. Returns
    /// the number of worker threads that were successfully started.
    pub fn init_threads<F>(&self, f: F) -> usize
    where
        F: Fn(usize) -> Result<JoinHandle<()>, SpawnError>,
    {
        let num_threads = self.num_threads();
        if num_threads == 0 {
            return 0;
        }
        let mut spawn_results = self.spawn_results.lock();
        self.spawn_threads(0, num_threads, f, &mut spawn_results)
    }

    /// Increases the maximum number of threads allowed in the `Hive` by `num_threads`, and
    /// attempts to spawn threads with indices in the range `cur_index..cur_index + num_threads`
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
        self.task_queues
            .init_for_threads(start_index, end_index, &self.config);
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

    /// Returns the mutex guard for the results of spawing worker threads.
    pub fn spawn_results(&self) -> MutexGuard<Vec<Result<JoinHandle<()>, SpawnError>>> {
        self.spawn_results.lock()
    }

    /// Returns the `WorkerQueues` instance for the worker thread with the specified index.
    pub fn worker_queues(&self, thread_index: usize) -> T::WorkerQueues {
        self.task_queues.worker_queues(thread_index)
    }

    /// Returns a new `Worker` from the queen, or an error if a `Worker` could not be created.
    pub fn create_worker(&self) -> Q::Kind {
        self.queen.create()
    }

    /// Creates a new `Task` for the given input and outcome channel, and adds it to the global
    /// queue.
    pub fn send_one_global<I>(&self, input: I, outcome_tx: Option<&OutcomeSender<W>>) -> TaskId
    where
        I: Into<TaskInput<W>>,
    {
        if self.num_threads() == 0 {
            dbg!("WARNING: no worker threads are active for hive");
        }
        let task = self.prepare_task(input, outcome_tx);
        // when the `local-batch` feature is enabled, immediately abandon any task whose weight is
        // greater than the configured limit
        #[cfg(feature = "local-batch")]
        let task = match self.abandon_if_too_heavy(task) {
            Ok(task) => task,
            Err(task_id) => return task_id,
        };
        let task_id = task.id();
        self.push_global(task);
        task_id
    }

    /// Creates a new `Task` for each input in the given batch and sends them to the global queue.
    pub fn send_batch_global<I, B>(
        &self,
        batch: B,
        outcome_tx: Option<&OutcomeSender<W>>,
    ) -> Vec<TaskId>
    where
        I: Into<TaskInput<W>>,
        B: IntoIterator<Item = I>,
        B::IntoIter: ExactSizeIterator,
    {
        #[cfg(debug_assertions)]
        if self.num_threads() == 0 {
            dbg!("WARNING: no worker threads are active for hive");
        }
        let iter = batch.into_iter();
        let (min_size, _) = iter.size_hint();
        self.num_tasks
            .increment_left(min_size as u64)
            .expect("overflowed queued task counter");
        let task_id_start = self.next_task_id.add(min_size);
        let task_id_end = task_id_start + min_size;
        let tasks = iter
            .map(Some)
            .chain(iter::repeat_with(|| None))
            .zip(
                (task_id_start..task_id_end)
                    .map(Some)
                    .chain(iter::repeat_with(|| None)),
            )
            .map_while(move |pair| match pair {
                (Some(input), Some(task_id)) => {
                    Some(Task::new(task_id, input.into(), outcome_tx.cloned()))
                }
                (Some(input), None) => Some(self.prepare_task(input, outcome_tx)),
                (None, Some(_)) => panic!("batch contained fewer than {min_size} items"),
                (None, None) => None,
            });
        if !self.is_poisoned() {
            tasks
                .map(|task| {
                    let task_id = task.id();
                    // try to send the task to the hive; if sending fails, convert the task into an
                    // `Unprocessed` outcome and try to send it to the outcome channel; if that
                    // fails, store the outcome in the hive
                    if let Err(task) = self.task_queues.try_push_global(task) {
                        self.abandon_task(task);
                    }
                    task_id
                })
                .collect()
        } else {
            // if the hive is poisoned, convert all tasks into `Unprocessed` outcomes and try to
            // send them to their outcome channels or store them in the hive
            self.abandon_batch(tasks)
        }
    }

    /// Increments the number of queued tasks. Returns a new `Task` with the provided input and
    /// `outcome_tx` and the next ID.
    pub fn prepare_task<I>(&self, input: I, outcome_tx: Option<&OutcomeSender<W>>) -> Task<W>
    where
        I: Into<TaskInput<W>>,
    {
        self.num_tasks
            .increment_left(1)
            .expect("overflowed queued task counter");
        let task_id = self.next_task_id.add(1);
        Task::new(task_id, input.into(), outcome_tx.cloned())
    }

    /// Adds `task` to the global queue if possible, otherwise abandons it - converts it to an
    /// `Unprocessed` outcome and sends it to the outcome channel or stores it in the hive.
    pub fn push_global(&self, task: Task<W>) {
        // try to send the task to the hive; if the hive is poisoned or if sending fails, convert
        // the task into an `Unprocessed` outcome and try to send it to the outcome channel; if
        // that fails, store the outcome in the hive
        if let Some(abandoned_task) = if self.is_poisoned() {
            Some(task)
        } else {
            self.task_queues.try_push_global(task).err()
        } {
            self.abandon_task(abandoned_task);
        }
    }

    /// Returns the next available `Task`. If there is a task in any local queue, it is returned,
    /// otherwise a task is requested from the global queue.
    ///
    /// If the hive is suspended, the calling thread blocks until the `Hive` is resumed.
    /// The calling thread also blocks until a task becomes available.
    ///
    /// Returns an error if the hive is poisoned or if the local queues are empty, and the global
    /// queue is disconnected.
    pub fn get_next_task(&self, worker_queues: &T::WorkerQueues) -> Option<Task<W>> {
        loop {
            // block while the hive is suspended
            self.wait_on_resume();
            // stop iteration if the hive is poisoned
            if self.is_poisoned() {
                return None;
            }
            // get the next task from the queue - break if its closed
            match worker_queues.try_pop() {
                Ok(task) => break Some(task),
                Err(PopTaskError::Closed) => break None,
                Err(PopTaskError::Empty) => continue,
            }
        }
        // if a task was successfully received, decrement the queued counter and increment the
        // active counter
        .and_then(|task| match self.num_tasks.transfer(1) {
            Ok(_) => Some(task),
            Err(_) => {
                // the hive is in a corrupted state - abandon this task and then poison the hive
                // so it can't be used anymore
                self.abandon_task(task);
                self.poison();
                None
            }
        })
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
                        .get_or_insert_with(|| self.outcomes.get_mut())
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

    #[cfg(feature = "local-batch")]
    pub fn abandon_if_too_heavy(&self, task: Task<W>) -> Result<Task<W>, TaskId> {
        let weight_limit = self.config.weight_limit.get().unwrap_or(0);
        if weight_limit > 0 && task.meta().weight() as u64 > weight_limit {
            let task_id = task.id();
            let (outcome, outcome_tx) = task.into_overweight();
            self.send_or_store_outcome(outcome, outcome_tx);
            // decrement the queued counter since it was incremented but the task was never queued
            let _ = self.num_tasks.decrement_left(1);
            self.no_work_notify_all();
            Err(task_id)
        } else {
            Ok(task)
        }
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

    /// Called by a worker thread after completing a task. Notifies any thread that has `join`ed
    /// the `Hive` if there is no more work to be done.
    #[inline]
    pub fn finish_task(&self, panicking: bool) {
        self.num_tasks
            .decrement_right(1)
            .expect("active task counter was smaller than expected");
        if panicking {
            self.num_panics.add(1);
        }
        self.no_work_notify_all();
    }

    /// Returns a reference to the `Queen`.
    ///
    /// Note that, if the queen is a `QueenMut`, the returned value will be a `QueenCell`, and it
    /// is necessary to call its `get()` method to obtain a reference to the inner queen.
    pub fn queen(&self) -> &Q {
        &self.queen
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

    pub fn num_panics(&self) -> usize {
        self.num_panics.get()
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

    /// Performs the following actions:
    /// 1. Sets the `poisoned` flag to `true
    /// 2. Closes all task queues so no more tasks may be pushed
    /// 3. Resumes the hive if it is suspendend, which enables blocked worker threads to terminate.
    pub fn poison(&self) {
        self.poisoned.set(true);
        self.close_task_queues(true);
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

    #[inline]
    pub fn wait_on_resume(&self) {
        self.resume_gate.wait_while(|| self.is_suspended());
    }

    /// Returns a mutable reference to the retained task outcomes.
    pub fn outcomes(&self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> {
        self.outcomes.get_mut()
    }

    /// Adds a new outcome to the retained task outcomes.
    pub fn add_outcome(&self, outcome: Outcome<W>) {
        self.outcomes.push(outcome);
    }

    /// Removes and returns all retained `Unprocessed` outcomes.
    pub fn take_unprocessed(&self) -> Vec<Outcome<W>> {
        let mut outcomes = self.outcomes.get_mut();
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

    /// Close the tasks queues so no more tasks can be added.
    pub fn close_task_queues(&self, urgent: bool) {
        self.task_queues.close(urgent, Token);
    }

    fn flush(
        task_queues: T,
        mut outcomes: HashMap<TaskId, Outcome<W>>,
    ) -> HashMap<TaskId, Outcome<W>> {
        for task in task_queues.drain().into_iter() {
            let task_id = task.id();
            let (outcome, outcome_tx) = task.into_unprocessed();
            if let Some(outcome) = if let Some(tx) = outcome_tx {
                tx.try_send_msg(outcome)
            } else {
                Some(outcome)
            } {
                outcomes.insert(task_id, outcome);
            }
        }
        outcomes
    }

    /// Consumes this `Shared`, closes and drains task queues, converts any queued tasks into
    /// `Outcome::Unprocessed outcomes, and tries to send them or (if the task does not have a
    /// sender, or if the send fails) stores them in the `outcomes` map. Returns the outcome map.
    pub fn into_outcomes(self) -> HashMap<TaskId, Outcome<W>> {
        Self::flush(self.task_queues, self.outcomes.into_inner())
    }

    /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
    /// outcomes, and all configuration information necessary to create a new `Hive`. Any queued
    /// tasks are converted into `Outcome::Unprocessed` outcomes and either sent to the task's
    /// sender or (if there is no sender, or the send fails) stored in the `outcomes` map.
    pub fn into_husk(self) -> Husk<Q> {
        Husk::new(
            self.config.into_unsync(),
            self.queen,
            self.num_panics.into_inner(),
            Self::flush(self.task_queues, self.outcomes.into_inner()),
        )
    }
}

impl<W, Q, T> fmt::Debug for Shared<Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
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

#[cfg(any(feature = "local-batch", feature = "retry"))]
mod update_config {
    use super::Shared;
    use crate::atomic::{Atomic, AtomicOption};
    use crate::bee::{Queen, Worker};
    use crate::hive::TaskQueues;
    use std::fmt::Debug;

    impl<W, Q, T> Shared<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
        fn maybe_update<P, A>(&self, new_value: P, option: &AtomicOption<P, A>) -> P
        where
            P: Eq + Copy + Clone + Debug + Default,
            A: Atomic<P>,
        {
            let prev_value = option.try_set(new_value).unwrap_or_default();
            if prev_value == new_value {
                return prev_value;
            }
            let num_threads = self.num_threads();
            if num_threads == 0 {
                return prev_value;
            }
            self.task_queues
                .update_for_threads(0, num_threads, &self.config);
            prev_value
        }

        /// Changes the local queue batch size. This requires allocating a new queue for each
        /// worker thread.
        ///
        /// Note: this method will block the current thread waiting for all local queues to become
        /// writable; if `batch_limit` is less than the current batch size, this method will also
        /// block while any thread's queue length is > `batch_limit` before moving the elements.
        #[cfg(feature = "local-batch")]
        pub fn set_worker_batch_limit(&self, batch_limit: usize) -> usize {
            self.maybe_update(batch_limit, &self.config.batch_limit)
        }

        /// Changes the local queue batch weight limit.
        #[cfg(feature = "local-batch")]
        pub fn set_worker_weight_limit(&self, weight_limit: u64) -> u64 {
            self.maybe_update(weight_limit, &self.config.weight_limit)
        }

        /// Sets the worker retry limit and returns the previous value.
        #[cfg(feature = "retry")]
        pub fn set_worker_retry_limit(&self, max_retries: u8) -> u8 {
            self.maybe_update(max_retries, &self.config.max_retries)
        }

        /// Sets the worker retry factor and returns the previous value.
        #[cfg(feature = "retry")]
        pub fn set_worker_retry_factor(
            &self,
            duration: std::time::Duration,
        ) -> std::time::Duration {
            std::time::Duration::from_nanos(
                self.maybe_update(duration.as_nanos() as u64, &self.config.retry_factor),
            )
        }
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use super::{Shared, TaskQueues};
    use crate::bee::{Queen, Worker};
    use crate::hive::cores::{Core, Cores};

    impl<W, Q, T> Shared<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
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

#[cfg(feature = "local-batch")]
mod local_batch {
    use crate::bee::{Queen, Worker};
    use crate::hive::inner::{Shared, TaskQueues};

    impl<W, Q, T> Shared<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
        /// Returns the local queue batch size.
        pub fn worker_batch_limit(&self) -> usize {
            self.config.batch_limit.get().unwrap_or_default()
        }

        /// Returns the local queue batch weight limit. A value of `0` means there is no weight
        pub fn worker_weight_limit(&self) -> u64 {
            self.config.weight_limit.get().unwrap_or_default()
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use crate::bee::{Queen, TaskMeta, Worker};
    use crate::hive::inner::{Shared, Task, TaskQueues};
    use crate::hive::{OutcomeSender, WorkerQueues};
    use std::time::Instant;

    impl<W, Q, T> Shared<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
        /// Returns the current worker retry limit.
        pub fn worker_retry_limit(&self) -> u8 {
            self.config.max_retries.get().unwrap_or_default()
        }

        /// Returns the current worker retry factor.
        pub fn worker_retry_factor(&self) -> std::time::Duration {
            std::time::Duration::from_nanos(self.config.retry_factor.get().unwrap_or_default())
        }

        /// Returns `true` if the hive is configured to retry tasks and the `attempt` field of the
        /// given `task_meta` is less than the maximum number of retries.
        pub fn can_retry(&self, task_meta: &TaskMeta) -> bool {
            self.config
                .max_retries
                .get()
                .map(|max_retries| task_meta.attempt() < max_retries)
                .unwrap_or(false)
        }

        /// Adds a task with the given `task_id`, `input`, and `outcome_tx` to the local retry
        /// queue for the specified `thread_index`.
        pub fn try_send_retry(
            &self,
            input: W::Input,
            meta: TaskMeta,
            outcome_tx: Option<&OutcomeSender<W>>,
            worker_queues: &T::WorkerQueues,
        ) -> Result<Instant, Task<W>> {
            self.num_tasks
                .increment_left(1)
                .expect("overflowed queued task counter");
            let task = Task::next_retry_attempt(input, meta, outcome_tx.cloned());
            worker_queues.try_push_retry(task)
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::bee::DefaultQueen;
    use crate::bee::stock::ThunkWorker;
    use crate::hive::ChannelTaskQueues;

    type VoidThunkWorker = ThunkWorker<()>;
    type VoidThunkWorkerShared =
        super::Shared<DefaultQueen<VoidThunkWorker>, ChannelTaskQueues<VoidThunkWorker>>;

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
