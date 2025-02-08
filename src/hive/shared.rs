use super::{
    Config, GlobalQueue, Husk, LocalQueues, Outcome, OutcomeSender, Shared, SpawnError, Task,
};
use crate::atomic::{Atomic, AtomicInt, AtomicUsize};
use crate::bee::{Queen, TaskId, Worker};
use crate::channel::SenderExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::thread::{Builder, JoinHandle};
use std::{fmt, iter};

impl<W, Q, G, L> Shared<W, Q, G, L>
where
    W: Worker,
    Q: Queen<Kind = W>,
    G: GlobalQueue<W>,
    L: LocalQueues<W, G>,
{
    /// Creates a new `Shared` instance with the given configuration, queen, and task receiver,
    /// and all other fields set to their default values.
    pub fn new(config: Config, global_queue: G, queen: Q) -> Self {
        Shared {
            config,
            global_queue,
            queen: Mutex::new(queen),
            local_queues: Default::default(),
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
        self.local_queues
            .init_for_threads(start_index, end_index, self);
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
    fn prepare_task(&self, input: W::Input, outcome_tx: Option<&OutcomeSender<W>>) -> Task<W> {
        self.num_tasks
            .increment_left(1)
            .expect("overflowed queued task counter");
        let task_id = self.next_task_id.add(1);
        Task::new(task_id, input, outcome_tx.cloned())
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
            self.global_queue.try_push(task).err()
        } {
            self.abandon_task(abandoned_task);
        }
    }

    /// Creates a new `Task` for the given input and outcome channel, and adds it to the global
    /// queue.
    pub fn send_one_global(
        &self,
        input: W::Input,
        outcome_tx: Option<&OutcomeSender<W>>,
    ) -> TaskId {
        let task = self.prepare_task(input, outcome_tx);
        let task_id = task.id();
        self.push_global(task);
        task_id
    }

    /// Creates a new `Task` for the given input and outcome channel, and attempts to add it to
    /// the local queue for the specified `thread_index`. Falls back to adding it to the global
    /// queue.
    pub fn send_one_local(
        &self,
        input: W::Input,
        outcome_tx: Option<&OutcomeSender<W>>,
        thread_index: usize,
    ) -> TaskId {
        let task = self.prepare_task(input, outcome_tx);
        let task_id = task.id();
        self.local_queues.push(task, thread_index, self);
        task_id
    }

    /// Creates a new `Task` for each input in the given batch and sends them to the global queue.
    pub fn send_batch_global<T>(
        &self,
        inputs: T,
        outcome_tx: Option<&OutcomeSender<W>>,
    ) -> Vec<TaskId>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        let iter = inputs.into_iter();
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
                    Some(Task::new(task_id, input, outcome_tx.cloned()))
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
                    if let Err(task) = self.global_queue.try_push(task) {
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

    /// Returns the next available `Task`. If there is a task in any local queue, it is returned,
    /// otherwise a task is requested from the global queue.
    ///
    /// If the hive is suspended, the calling thread blocks until the `Hive` is resumed.
    /// The calling thread also blocks until a task becomes available.
    ///
    /// Returns an error if the hive is poisoned or if the local queues are empty, and the global
    /// queue is disconnected.
    pub fn get_next_task(&self, thread_index: usize) -> Option<Task<W>> {
        loop {
            // block while the hive is suspended
            self.wait_on_resume();
            // stop iteration if the hive is poisoned
            if self.is_poisoned() {
                return None;
            }
            // try to get a task from the local queues
            if let Some(task) = self.local_queues.try_pop(thread_index, self) {
                break Ok(task);
            }
            // fall back to requesting a task from the global queue
            if let Some(result) = self.global_queue.try_pop() {
                break result;
            }
        }
        // if a task was successfully received, decrement the queued counter and increment the
        // active counter
        .map(|task| match self.num_tasks.transfer(1) {
            Ok(_) => Some(task),
            Err(_) => {
                // the hive is in a corrupted state - abandon this task and then poison the hive
                // so it can't be used anymore
                self.abandon_task(task);
                self.poison();
                None
            }
        })
        .ok()
        .flatten()
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

    #[inline]
    pub fn wait_on_resume(&self) {
        self.resume_gate.wait_while(|| self.is_suspended());
    }

    /// Returns a mutable reference to the retained task outcomes.
    pub fn outcomes(&self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> + '_ {
        self.outcomes.get_mut()
    }

    /// Adds a new outcome to the retained task outcomes.
    pub fn add_outcome(&self, outcome: Outcome<W>) {
        self.outcomes.push(outcome);
    }

    /// Removes and returns all retained task outcomes.
    pub fn take_outcomes(&self) -> HashMap<TaskId, Outcome<W>> {
        self.outcomes.drain()
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

    /// Drains all queued tasks, converts them into `Outcome::Unprocessed` outcomes, and tries
    /// to send them or (if the task does not have a sender, or if the send fails) stores them
    /// in the `outcomes` map.
    fn drain_tasks_into_unprocessed(&self) {
        self.abandon_batch(self.global_queue.drain().into_iter());
        self.abandon_batch(self.local_queues.drain().into_iter());
    }

    /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
    /// outcomes, and all configuration information necessary to create a new `Hive`. Any queued
    /// tasks are converted into `Outcome::Unprocessed` outcomes and either sent to the task's
    /// sender or (if there is no sender, or the send fails) stored in the `outcomes` map.
    pub fn into_husk(self) -> Husk<W, Q> {
        self.drain_tasks_into_unprocessed();
        Husk::new(
            self.config.into_unsync(),
            self.queen.into_inner(),
            self.num_panics.into_inner(),
            self.outcomes.into_inner(),
        )
    }
}

impl<W, Q, G, L> fmt::Debug for Shared<W, Q, G, L>
where
    W: Worker,
    Q: Queen<Kind = W>,
    G: GlobalQueue<W>,
    L: LocalQueues<W, G>,
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

#[cfg(feature = "affinity")]
mod affinity {
    use crate::bee::{Queen, Worker};
    use crate::hive::cores::{Core, Cores};
    use crate::hive::{GlobalQueue, LocalQueues, Shared};

    impl<W, Q, G, L> Shared<W, Q, G, L>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        G: GlobalQueue<W>,
        L: LocalQueues<W, G>,
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

#[cfg(feature = "batching")]
mod batching {
    use crate::bee::{Queen, Worker};
    use crate::hive::{GlobalQueue, LocalQueues, Shared};

    impl<W, Q, G, L> Shared<W, Q, G, L>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        G: GlobalQueue<W>,
        L: LocalQueues<W, G>,
    {
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
            self.local_queues.resize(0, num_threads, batch_size, self);
            prev_batch_size
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use crate::bee::{Queen, Worker};
    use crate::hive::{GlobalQueue, LocalQueues, OutcomeSender, Shared, Task, TaskId};
    use std::time::Instant;

    impl<W, Q, G, L> Shared<W, Q, G, L>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        G: GlobalQueue<W>,
        L: LocalQueues<W, G>,
    {
        /// Returns `true` if the hive is configured to retry tasks and the `attempt` field of the
        /// given `ctx` is less than the maximum number of retries.
        pub fn can_retry(&self, attempt: u32) -> bool {
            self.config
                .max_retries
                .get()
                .map(|max_retries| attempt < max_retries)
                .unwrap_or(false)
        }

        /// Adds a task with the given `task_id`, `input`, and `outcome_tx` to the local retry
        /// queue for the specified `thread_index`.
        pub fn send_retry(
            &self,
            task_id: TaskId,
            input: W::Input,
            outcome_tx: Option<OutcomeSender<W>>,
            attempt: u32,
            thread_index: usize,
        ) -> Option<Instant> {
            self.num_tasks
                .increment_left(1)
                .expect("overflowed queued task counter");
            let task = Task::with_attempt(task_id, input, outcome_tx, attempt);
            self.local_queues.retry(task, thread_index, self)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bee::stock::ThunkWorker;
    use crate::bee::DefaultQueen;
    use crate::hive::task::ChannelGlobalQueue;
    use crate::hive::ChannelLocalQueues;

    type VoidThunkWorker = ThunkWorker<()>;
    type VoidThunkWorkerShared = super::Shared<
        VoidThunkWorker,
        DefaultQueen<VoidThunkWorker>,
        ChannelGlobalQueue<VoidThunkWorker>,
        ChannelLocalQueues<VoidThunkWorker>,
    >;

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
