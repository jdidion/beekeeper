use super::{
    ChannelBuilder, ChannelTaskQueues, Config, DerefOutcomes, Husk, Outcome, OutcomeBatch,
    OutcomeIteratorExt, OutcomeSender, Shared, SpawnError, TaskQueues, TaskQueuesBuilder,
    WorkerQueues,
};
use crate::bee::{DefaultQueen, Queen, TaskContext, TaskId, Worker};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

#[derive(thiserror::Error, Debug)]
#[error("The hive has been poisoned")]
pub struct Poisoned;

/// A pool of worker threads that each execute the same function.
///
/// See the [module documentation](crate::hive) for details.
pub struct Hive<Q: Queen, T: TaskQueues<Q::Kind>>(Option<Arc<Shared<Q, T>>>);

impl<Q: Queen, T: TaskQueues<Q::Kind>> Hive<Q, T> {
    /// Creates a new `Hive`. This should only be called from `Builder`.
    ///
    /// The `Hive` will attempt to spawn the configured number of worker threads
    /// (`config.num_threads`) but the actual number of threads available may be lower if there
    /// are any errors during spawning.
    pub(super) fn new(config: Config, queen: Q) -> Self {
        let shared = Arc::new(Shared::new(config.into_sync(), queen));
        shared.init_threads(|thread_index| Self::try_spawn(thread_index, &shared));
        Self(Some(shared))
    }
}

impl<W: Worker, Q: Queen<Kind = W>, T: TaskQueues<W>> Hive<Q, T> {
    /// Spawns a new worker thread with the specified index and with access to the `shared` data.
    fn try_spawn(
        thread_index: usize,
        shared: &Arc<Shared<Q, T>>,
    ) -> Result<JoinHandle<()>, SpawnError> {
        let thread_builder = shared.thread_builder();
        let shared = Arc::clone(shared);
        // spawn a thread that executes the worker loop
        thread_builder.spawn(move || {
            // perform one-time initialization of the worker thread
            Self::init_thread(thread_index, &shared);
            // create a Sentinel that will spawn a new thread on panic until it is cancelled
            let sentinel = Sentinel::new(thread_index, Arc::clone(&shared));
            // get the thread-local interface to the task queues
            let worker_queues = shared.worker_queues(thread_index);
            // create a new worker to process tasks
            let mut worker = shared.create_worker();
            // execute the main loop: get the next task to process, which decrements the queued
            // counter and increments the active counter
            while let Some(task) = shared.get_next_task(&worker_queues) {
                // execute the task and dispose of the outcome
                Self::execute(task, &mut worker, &worker_queues, &shared);
                // finish the task - decrements the active counter and notifies other threads
                shared.finish_task(false);
            }
            // this is only reachable when the main loop exits due to the task receiver having
            // disconnected; cancel the Sentinel so this thread won't be re-spawned on drop
            sentinel.cancel();
        })
    }

    #[inline]
    fn shared(&self) -> &Arc<Shared<Q, T>> {
        self.0.as_ref().unwrap()
    }

    /// Attempts to increase the number of worker threads by `num_threads`. Returns the number of
    /// new worker threads that were successfully started (which may be fewer than `num_threads`),
    /// or a `Poisoned` error if the hive has been poisoned.
    pub fn grow(&self, num_threads: usize) -> Result<usize, Poisoned> {
        if num_threads == 0 {
            return Ok(0);
        }
        let shared = self.shared();
        // do not start any new threads if the hive is poisoned
        if shared.is_poisoned() {
            return Err(Poisoned);
        }
        let num_started = shared.grow_threads(num_threads, |thread_index| {
            Self::try_spawn(thread_index, shared)
        });
        Ok(num_started)
    }

    /// Sets the number of worker threads to the number of available CPU cores. Returns the number
    /// of new threads that were successfully started (which may be `0`), or a `Poisoned` error if
    /// the hive has been poisoned.
    pub fn use_all_cores(&self) -> Result<usize, Poisoned> {
        let num_threads = num_cpus::get().saturating_sub(self.max_workers());
        self.grow(num_threads)
    }

    /// Sends one `input` to the `Hive` for procesing and returns the result, blocking until the
    /// result is available. Creates a channel to send the input and receive the outcome. Returns
    /// an [`Outcome`] with the task output or an error.
    pub fn apply(&self, input: W::Input) -> Outcome<W> {
        let (tx, rx) = super::outcome_channel();
        let task_id = self.shared().send_one_global(input, Some(&tx));
        drop(tx);
        rx.recv().unwrap_or_else(|_| Outcome::Missing { task_id })
    }

    /// Sends one `input` to the `Hive` for processing and returns its ID. The [`Outcome`] of
    /// the task will be sent to `tx` upon completion.
    pub fn apply_send<S: Borrow<OutcomeSender<W>>>(&self, input: W::Input, tx: S) -> TaskId {
        self.shared().send_one_global(input, Some(tx.borrow()))
    }

    /// Sends one `input` to the `Hive` for processing and returns its ID immediately. The
    /// [`Outcome`] of the task will be retained and available for later retrieval.
    pub fn apply_store(&self, input: W::Input) -> TaskId {
        self.shared().send_one_global(input, None)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns an iterator over the
    /// [`Outcome`]s in the same order as the inputs.
    ///
    /// This method is more efficient than [`map`](Self::map) when the input is an
    /// [`ExactSizeIterator`].
    pub fn swarm<I>(&self, batch: I) -> impl Iterator<Item = Outcome<W>>
    where
        I: IntoIterator<Item = W::Input>,
        I::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = super::outcome_channel();
        let task_ids = self.shared().send_batch_global(batch, Some(&tx));
        drop(tx);
        rx.select_ordered(task_ids)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns an unordered iterator
    /// over the [`Outcome`]s.
    ///
    /// The `Outcome`s will be sent in the order they are completed; use [`swarm`](Self::swarm) to
    /// instead receive the `Outcome`s in the order they were submitted. This method is more
    /// efficient than [`map_unordered`](Self::map_unordered) when the input is an
    /// [`ExactSizeIterator`].
    pub fn swarm_unordered<I>(&self, batch: I) -> impl Iterator<Item = Outcome<W>>
    where
        I: IntoIterator<Item = W::Input>,
        I::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = super::outcome_channel();
        let task_ids = self.shared().send_batch_global(batch, Some(&tx));
        rx.select_unordered(task_ids)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns a [`Vec`] of task IDs.
    /// The [`Outcome`]s of the tasks will be sent to `tx` upon completion.
    ///
    /// This method is more efficient than [`map_send`](Self::map_send) when the input is an
    /// [`ExactSizeIterator`].
    pub fn swarm_send<I, S>(&self, batch: I, outcome_tx: S) -> Vec<TaskId>
    where
        S: Borrow<OutcomeSender<W>>,
        I: IntoIterator<Item = W::Input>,
        I::IntoIter: ExactSizeIterator,
    {
        self.shared()
            .send_batch_global(batch, Some(outcome_tx.borrow()))
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns a [`Vec`] of task IDs.
    /// The [`Outcome`]s of the task are retained and available for later retrieval.
    ///
    /// This method is more efficient than `map_store` when the input is an [`ExactSizeIterator`].
    pub fn swarm_store<I>(&self, batch: I) -> Vec<TaskId>
    where
        I: IntoIterator<Item = W::Input>,
        I::IntoIter: ExactSizeIterator,
    {
        self.shared().send_batch_global(batch, None)
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing and returns an
    /// iterator over the [`Outcome`]s in the same order as the inputs.
    ///
    /// [`swarm`](Self::swarm) should be preferred when `inputs` is an [`ExactSizeIterator`].
    pub fn map(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
    ) -> impl Iterator<Item = Outcome<W>> {
        let (tx, rx) = super::outcome_channel();
        let task_ids: Vec<_> = inputs
            .into_iter()
            .map(|task| self.apply_send(task, &tx))
            .collect();
        drop(tx);
        rx.select_ordered(task_ids)
    }

    /// Iterates over `inputs`, sends each one to the `Hive` for processing, and returns an
    /// iterator over the [`Outcome`]s in order they become available.
    ///
    /// [`swarm_unordered`](Self::swarm_unordered) should be preferred when `inputs` is an
    /// [`ExactSizeIterator`].
    pub fn map_unordered(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
    ) -> impl Iterator<Item = Outcome<W>> {
        let (tx, rx) = super::outcome_channel();
        // `map` is required (rather than `inspect`) because we need owned items
        let task_ids: Vec<_> = inputs
            .into_iter()
            .map(|task| self.apply_send(task, &tx))
            .collect();
        drop(tx);
        rx.select_unordered(task_ids)
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing. Returns a [`Vec`]
    /// of task IDs. The [`Outcome`]s of the tasks will be sent to `tx` upon completion.
    ///
    /// [`swarm_send`](Self::swarm_send) should be preferred when `inputs` is an
    /// [`ExactSizeIterator`].
    pub fn map_send<S: Borrow<OutcomeSender<W>>>(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
        tx: S,
    ) -> Vec<TaskId> {
        inputs
            .into_iter()
            .map(|input| self.apply_send(input, tx.borrow()))
            .collect()
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing. Returns a [`Vec`]
    /// of task IDs. The [`Outcome`]s of the task are retained and available for later retrieval.
    ///
    /// [`swarm_store`](Self::swarm_store) should be preferred when `inputs` is an
    /// [`ExactSizeIterator`].
    pub fn map_store(&self, inputs: impl IntoIterator<Item = W::Input>) -> Vec<TaskId> {
        inputs
            .into_iter()
            .map(|input| self.apply_store(input))
            .collect()
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `F` returns an input that is sent to the `Hive` for processing.
    /// Returns an [`OutcomeBatch`] of the outputs and the final state value.
    pub fn scan<I, St, F>(
        &self,
        items: impl IntoIterator<Item = I>,
        init: St,
        f: F,
    ) -> (OutcomeBatch<W>, St)
    where
        F: FnMut(&mut St, I) -> W::Input,
    {
        let (tx, rx) = super::outcome_channel();
        let (task_ids, fold_value) = self.scan_send(items, &tx, init, f);
        drop(tx);
        let outcomes = rx.select_unordered(task_ids).into();
        (outcomes, fold_value)
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `F` returns an input that is sent to the `Hive` for processing,
    /// or an error. Returns an [`OutcomeBatch`] of the outputs, a [`Vec`] of errors, and the final
    /// state value.
    pub fn try_scan<I, E, St, F>(
        &self,
        items: impl IntoIterator<Item = I>,
        init: St,
        mut f: F,
    ) -> (OutcomeBatch<W>, Vec<E>, St)
    where
        F: FnMut(&mut St, I) -> Result<W::Input, E>,
    {
        let (tx, rx) = super::outcome_channel();
        let (task_ids, errors, fold_value) = items.into_iter().fold(
            (Vec::new(), Vec::new(), init),
            |(mut task_ids, mut errors, mut acc), inp| {
                match f(&mut acc, inp) {
                    Ok(input) => task_ids.push(self.apply_send(input, &tx)),
                    Err(err) => errors.push(err),
                }
                (task_ids, errors, acc)
            },
        );
        drop(tx);
        let outcomes = rx.select_unordered(task_ids).into();
        (outcomes, errors, fold_value)
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing.
    /// The outputs are sent to `tx` in the order they become available. Returns a [`Vec`] of the
    /// task IDs and the final state value.
    pub fn scan_send<I, S, St, F>(
        &self,
        items: impl IntoIterator<Item = I>,
        tx: S,
        init: St,
        mut f: F,
    ) -> (Vec<TaskId>, St)
    where
        S: Borrow<OutcomeSender<W>>,
        F: FnMut(&mut St, I) -> W::Input,
    {
        items
            .into_iter()
            .fold((Vec::new(), init), |(mut task_ids, mut acc), item| {
                let input = f(&mut acc, item);
                task_ids.push(self.apply_send(input, tx.borrow()));
                (task_ids, acc)
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing,
    /// or an error. The outputs are sent to `tx` in the order they become available. This
    /// function returns the final state value and a [`Vec`] of results, where each result is
    /// either a task ID or an error.
    pub fn try_scan_send<I, E, S, St, F>(
        &self,
        items: impl IntoIterator<Item = I>,
        tx: S,
        init: St,
        mut f: F,
    ) -> (Vec<Result<TaskId, E>>, St)
    where
        S: Borrow<OutcomeSender<W>>,
        F: FnMut(&mut St, I) -> Result<W::Input, E>,
    {
        items
            .into_iter()
            .fold((Vec::new(), init), |(mut results, mut acc), inp| {
                results.push(f(&mut acc, inp).map(|input| self.apply_send(input, tx.borrow())));
                (results, acc)
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing.
    /// This function returns the final state value and a [`Vec`] of task IDs. The [`Outcome`]s of
    /// the tasks are retained and available for later retrieval.
    pub fn scan_store<I, St, F>(
        &self,
        items: impl IntoIterator<Item = I>,
        init: St,
        mut f: F,
    ) -> (Vec<TaskId>, St)
    where
        F: FnMut(&mut St, I) -> W::Input,
    {
        items
            .into_iter()
            .fold((Vec::new(), init), |(mut task_ids, mut acc), item| {
                let input = f(&mut acc, item);
                task_ids.push(self.apply_store(input));
                (task_ids, acc)
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing,
    /// or an error. This function returns the final value of the state value and a [`Vec`] of
    /// results, where each result is either a task ID or an error. The [`Outcome`]s of the
    /// tasks are retained and available for later retrieval.
    pub fn try_scan_store<I, E, St, F>(
        &self,
        items: impl IntoIterator<Item = I>,
        init: St,
        mut f: F,
    ) -> (Vec<Result<TaskId, E>>, St)
    where
        F: FnMut(&mut St, I) -> Result<W::Input, E>,
    {
        items
            .into_iter()
            .fold((Vec::new(), init), |(mut results, mut acc), item| {
                results.push(f(&mut acc, item).map(|input| self.apply_store(input)));
                (results, acc)
            })
    }

    /// Blocks the calling thread until all tasks finish.
    pub fn join(&self) {
        self.shared().wait_on_done();
    }

    /// Returns a read-only reference to the [`Queen`].
    pub fn queen(&self) -> &Q {
        self.shared().queen()
    }

    /// Returns the number of worker threads that have been requested, i.e., the maximum number of
    /// tasks that could be processed concurrently. This may be greater than
    /// [`active_workers`](Self::active_workers) if any of the worker threads failed to start.
    pub fn max_workers(&self) -> usize {
        self.shared().num_threads()
    }

    /// Returns the number of worker threads that have been successfully started. This may be
    /// fewer than [`max_workers`](Self::max_workers) if any of the worker threads failed to start.
    pub fn alive_workers(&self) -> usize {
        self.shared()
            .spawn_results()
            .iter()
            .filter(|result| result.is_ok())
            .count()
    }

    /// Returns `true` if there are any "dead" worker threads that failed to spawn.
    pub fn has_dead_workers(&self) -> bool {
        self.shared()
            .spawn_results()
            .iter()
            .any(|result| result.is_err())
    }

    /// Attempts to respawn any dead worker threads. Returns the number of worker threads that were
    /// successfully respawned.
    pub fn revive_workers(&self) -> usize {
        let shared = self.shared();
        shared.respawn_dead_threads(|thread_index| Self::try_spawn(thread_index, shared))
    }

    /// Returns the number of tasks currently (queued for processing, being processed).
    pub fn num_tasks(&self) -> (u64, u64) {
        self.shared().num_tasks()
    }

    /// Returns the number of times one of this `Hive`'s worker threads has panicked.
    pub fn num_panics(&self) -> usize {
        self.shared().num_panics()
    }

    /// Returns `true` if this `Hive` has been poisoned - i.e., its internal state has been
    /// corrupted such that it is no longer able to process tasks.
    ///
    /// Note that, when a `Hive` is poisoned, it is still possible to call methods that extract
    /// its stored [`Outcome`]s (e.g., [`take_stored`](Self::take_stored)) or consume it (e.g.,
    /// [`try_into_husk`](Self::try_into_husk)).
    pub fn is_poisoned(&self) -> bool {
        self.shared().is_poisoned()
    }

    /// Returns `true` if the suspended flag is set.
    pub fn is_suspended(&self) -> bool {
        self.shared().is_suspended()
    }

    /// Sets the suspended flag, which notifies worker threads that they a) MAY terminate their
    /// current task early (returning an [`Outcome::Unprocessed`]), and b) MUST not accept new
    /// tasks, and instead block until the suspended flag is cleared.
    ///
    /// Call [`resume`](Self::resume) to unset the suspended flag and continue processing tasks.
    ///
    /// Note: this does *not* prevent new tasks from being queued, and there is a window of time
    /// (~1 second) after the suspended flag is set within which a worker thread may still accept a
    /// new task.
    ///
    /// # Examples
    ///
    /// ```
    /// use beekeeper::bee::stock::{Thunk, ThunkWorker};
    /// use beekeeper::hive::{Builder, ChannelBuilder};
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// # fn main() {
    /// let hive = ChannelBuilder::empty()
    ///     .num_threads(4)
    ///     .with_worker_default::<ThunkWorker<()>>()
    ///     .build();
    /// hive.map((0..10).map(|_| Thunk::of(|| thread::sleep(Duration::from_secs(3)))));
    /// thread::sleep(Duration::from_secs(1)); // Allow first set of tasks to be started.
    /// // There should be 4 active tasks and 6 queued tasks.
    /// hive.suspend();
    /// assert_eq!(hive.num_tasks(), (6, 4));
    /// // Wait for active tasks to complete.
    /// hive.join();
    /// assert_eq!(hive.num_tasks(), (6, 0));
    /// hive.resume();
    /// // Wait for remaining tasks to complete.
    /// hive.join();
    /// assert_eq!(hive.num_tasks(), (0, 0));
    /// # }
    /// ```
    pub fn suspend(&self) {
        self.shared().set_suspended(true);
    }

    /// Unsets the suspended flag, allowing worker threads to continue processing queued tasks.
    pub fn resume(&self) {
        self.shared().set_suspended(false);
    }

    /// Removes all `Unprocessed` outcomes from this `Hive` and returns them as an iterator over
    /// the input values.
    fn take_unprocessed_inputs(&self) -> impl ExactSizeIterator<Item = W::Input> {
        self.shared()
            .take_unprocessed()
            .into_iter()
            .map(|outcome| match outcome {
                Outcome::Unprocessed { input, task_id: _ } => input,
                _ => unreachable!(),
            })
    }

    /// If this `Hive` is suspended, resumes this `Hive` and re-submits any unprocessed tasks for
    /// processing, with their results to be sent to `tx`. Returns a [`Vec`] of task IDs that
    /// were resumed.
    pub fn resume_send(&self, outcome_tx: &OutcomeSender<W>) -> Vec<TaskId> {
        self.shared()
            .set_suspended(false)
            .then(|| self.swarm_send(self.take_unprocessed_inputs(), outcome_tx))
            .unwrap_or_default()
    }

    /// If this `Hive` is suspended, resumes this `Hive` and re-submit any unprocessed tasks for
    /// processing, with their results to be stored in the queue. Returns a [`Vec`] of task IDs
    /// that were resumed.
    pub fn resume_store(&self) -> Vec<TaskId> {
        self.shared()
            .set_suspended(false)
            .then(|| self.swarm_store(self.take_unprocessed_inputs()))
            .unwrap_or_default()
    }

    /// Returns all stored outcomes as a [`HashMap`] of task IDs to `Outcome`s.
    pub fn take_stored(&self) -> HashMap<TaskId, Outcome<W>> {
        self.shared().take_outcomes()
    }

    /// Consumes this `Hive` and attempts to acquire the shared data object.
    ///
    /// This closes the task queues so that no more tasks may be submitted. If `urgent` is `true`,
    /// worker threads are also prevented from taking any more tasks from the queues; otherwise,
    /// this method blocks while all queued are processed.
    ///
    /// If this `Hive` has been cloned, and those clones have not been dropped, this method returns
    /// `None`.
    fn try_close(mut self, urgent: bool) -> Option<Shared<Q, T>> {
        if self.shared().num_referrers() > 1 {
            return None;
        }
        // take the inner value and replace it with `None`
        let shared = self.0.take().unwrap();
        // close the global queue to prevent new tasks from being submitted
        shared.close_task_queues(urgent);
        // wait for all tasks to finish
        shared.wait_on_done();
        // unwrap the Arc and return the inner Shared value
        Some(
            super::util::unwrap_arc(shared)
                .expect("timeout waiting to take ownership of shared data"),
        )
    }

    /// Consumes this `Hive` and attempts to shut it down gracefully.
    ///
    /// If this `Hive` has been cloned, and those clones have not been dropped, this method returns
    /// `false`.
    ///
    /// This closes the task queues so that no more tasks may be submitted. If `urgent` is `true`,
    /// worker threads are also prevented from taking any more tasks from the queues, and all
    /// queued tasks are converted to `Unprocessed` outcomes and sent or discarded; otherwise,
    /// this method blocks while all queued tasks are processed.
    ///
    /// Note that it is not necessary to call this method explicitly - all resources are dropped
    /// automatically when the last clone of the hive is dropped.
    pub fn close(self, urgent: bool) -> bool {
        self.try_close(urgent).is_some()
    }

    /// Consumes this `Hive` and returns a map of stored outcomes.
    ///
    /// If this `Hive` has been cloned, and those clones have not been dropped, this method
    /// returns `None` since it cannot take exclusive ownership of the internal shared data.
    ///
    /// This closes the task queues so that no more tasks may be submitted. If `urgent` is `true`,
    /// worker threads are also prevented from taking any more tasks from the queues, and all
    /// queued tasks are converted to `Unprocessed` outcomes and sent or stored; otherwise,
    /// this method blocks while all queued tasks are processed.
    ///
    /// This method first joins on the `Hive` to wait for all tasks to finish.
    pub fn try_into_outcomes(self, urgent: bool) -> Option<HashMap<TaskId, Outcome<W>>> {
        self.try_close(urgent).map(|shared| shared.into_outcomes())
    }

    /// Consumes this `Hive` and attempts to return a [`Husk`] containing the remnants of this
    /// `Hive`, including any stored task outcomes, and all the data necessary to create a new
    /// `Hive`.
    ///
    /// If this `Hive` has been cloned, and those clones have not been dropped, this method
    /// returns `None` since it cannot take exclusive ownership of the internal shared data.
    ///
    /// This closes the task queues so that no more tasks may be submitted. If `urgent` is `true`,
    /// worker threads are also prevented from taking any more tasks from the queues, and all
    /// queued tasks are converted to `Unprocessed` outcomes and sent or stored; otherwise,
    /// this method blocks while all queued tasks are processed.
    ///
    /// This method first joins on the `Hive` to wait for all tasks to finish.
    pub fn try_into_husk(self, urgent: bool) -> Option<Husk<Q>> {
        self.try_close(urgent).map(|shared| shared.into_husk())
    }
}

pub type DefaultHive<W> = Hive<DefaultQueen<W>, ChannelTaskQueues<W>>;

impl<W: Worker + Send + Sync + Default> Default for DefaultHive<W> {
    fn default() -> Self {
        ChannelBuilder::default().with_worker_default().build()
    }
}

impl<W, Q, T> Clone for Hive<Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    /// Creates a shallow copy of this `Hive` containing references to its same internal state,
    /// i.e., all clones of a `Hive` submit tasks to the same shared worker thread pool.
    fn clone(&self) -> Self {
        let shared = self.0.as_ref().unwrap();
        shared.referrer_is_cloning();
        Self(Some(shared.clone()))
    }
}

impl<W, Q, T> fmt::Debug for Hive<Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(shared) = self.0.as_ref() {
            f.debug_struct("Hive").field("shared", &shared).finish()
        } else {
            f.write_str("Hive {}")
        }
    }
}

impl<W, Q, T> PartialEq for Hive<Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn eq(&self, other: &Hive<Q, T>) -> bool {
        let self_shared = self.shared();
        let other_shared = &other.shared();
        Arc::ptr_eq(self_shared, other_shared)
    }
}

impl<W, Q, T> Eq for Hive<Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
}

impl<W, Q, T> DerefOutcomes<W> for Hive<Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    #[inline]
    fn outcomes_deref(&self) -> impl Deref<Target = HashMap<TaskId, Outcome<W>>> {
        self.shared().outcomes()
    }

    #[inline]
    fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> {
        self.shared().outcomes()
    }
}

impl<W, Q, T> Drop for Hive<Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn drop(&mut self) {
        // if this Hive has already been turned into a Husk, it's inner value will be `None`
        if let Some(shared) = self.0.as_ref() {
            // reduce the referrer count
            let _ = shared.referrer_is_dropping();
            // if this Hive is the only one with a pointer to the shared data, poison it
            // to prevent any worker threads that still have access to the shared data from
            // re-spawning.
            if shared.num_referrers() == 0 {
                shared.poison();
            }
        }
    }
}

/// Sentinel for a worker thread. Until the sentinel is cancelled, it will respawn the worker
/// thread if it panics.
struct Sentinel<W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    thread_index: usize,
    shared: Arc<Shared<Q, T>>,
    active: bool,
}

impl<W, Q, T> Sentinel<W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn new(thread_index: usize, shared: Arc<Shared<Q, T>>) -> Self {
        Self {
            thread_index,
            shared,
            active: true,
        }
    }

    /// Cancel and destroy this sentinel.
    fn cancel(mut self) {
        self.active = false;
    }
}

impl<W, Q, T> Drop for Sentinel<W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn drop(&mut self) {
        if self.active {
            // if the sentinel is active, that means the thread panicked during task execution, so
            // we have to finish the task here before respawning
            self.shared.finish_task(thread::panicking());
            // only respawn if the sentinel is active and the hive has not been poisoned
            if !self.shared.is_poisoned() {
                // can't do anything with the previous result
                let _ = self
                    .shared
                    .respawn_thread(self.thread_index, |thread_index| {
                        Hive::try_spawn(thread_index, &self.shared)
                    });
            }
        }
    }
}

#[cfg(not(feature = "affinity"))]
mod no_affinity {
    use crate::bee::{Queen, Worker};
    use crate::hive::{Hive, Shared, TaskQueues};

    impl<W: Worker, Q: Queen<Kind = W>, T: TaskQueues<W>> Hive<Q, T> {
        #[inline]
        pub(super) fn init_thread(_: usize, _: &Shared<Q, T>) {}
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use crate::bee::{Queen, Worker};
    use crate::hive::cores::Cores;
    use crate::hive::{Hive, Poisoned, Shared, TaskQueues};

    impl<W, Q, T> Hive<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
        /// Tries to pin the worker thread to a specific CPU core.
        #[inline]
        pub(super) fn init_thread(thread_index: usize, shared: &Shared<Q, T>) {
            if let Some(core) = shared.get_core_affinity(thread_index) {
                core.try_pin_current();
            }
        }

        /// Attempts to increase the number of worker threads by `num_threads`.
        ///
        /// The provided `affinity` specifies additional CPU core indices to which the worker
        /// threads may be pinned - these are added to the existing pool of core indices (if any).
        ///
        /// Returns the number of new worker threads that were successfully started (which may be
        /// fewer than `num_threads`) or a `Poisoned` error if the hive has been poisoned.
        pub fn grow_with_affinity<C: Into<Cores>>(
            &self,
            num_threads: usize,
            affinity: C,
        ) -> Result<usize, Poisoned> {
            self.shared().add_core_affinity(affinity.into());
            self.grow(num_threads)
        }

        /// Sets the number of worker threads to the number of available CPU cores. An attempt is
        /// made to pin each worker thread to a different CPU core.
        ///
        /// Returns the number of new threads spun up (if any) or a `Poisoned` error if the hive
        /// has been poisoned.
        pub fn use_all_cores_with_affinity(&self) -> Result<usize, Poisoned> {
            self.shared().add_core_affinity(Cores::all());
            self.use_all_cores()
        }
    }
}

#[cfg(feature = "batching")]
mod batching {
    use crate::bee::{Queen, Worker};
    use crate::hive::{Hive, TaskQueues};

    impl<W, Q, T> Hive<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
        /// Returns the batch limit for worker threads.
        pub fn worker_batch_limit(&self) -> usize {
            self.shared().worker_batch_limit()
        }

        /// Sets the batch limit for worker threads.
        ///
        /// Depending on this hive's `TaskQueues` implementation, this method may:
        /// * have no effect (if it does not support local batching)
        /// * block the current thread until all worker thread queues can be resized.
        pub fn set_worker_batch_limit(&self, batch_limit: usize) {
            self.shared().set_worker_batch_limit(batch_limit);
        }
    }
}

#[cfg(not(feature = "retry"))]
mod no_retry {
    use super::HiveTaskContext;
    use crate::bee::{Context, Queen, Worker};
    use crate::hive::{Hive, Outcome, Shared, Task, TaskQueues};
    use std::sync::Arc;

    impl<W, Q, T> Hive<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
        pub(super) fn execute(
            task: Task<W>,
            worker: &mut W,
            worker_queues: &T::WorkerQueues,
            shared: &Arc<Shared<Q, T>>,
        ) {
            let (task_id, input, outcome_tx) = task.into_parts();
            let task_ctx = HiveTaskContext {
                worker_queues,
                shared,
                outcome_tx: outcome_tx.as_ref(),
            };
            let ctx = Context::new(task_id, Some(&task_ctx));
            let result = worker.apply(input, &ctx);
            let subtask_ids = ctx.into_subtask_ids();
            let outcome = Outcome::from_worker_result(result, task_id, subtask_ids);
            shared.send_or_store_outcome(outcome, outcome_tx);
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use super::HiveTaskContext;
    use crate::bee::{ApplyError, Context, Queen, Worker};
    use crate::hive::{Hive, Outcome, Shared, Task, TaskQueues};
    use std::sync::Arc;
    use std::time::Duration;

    impl<W, Q, T> Hive<Q, T>
    where
        W: Worker,
        Q: Queen<Kind = W>,
        T: TaskQueues<W>,
    {
        /// Returns the current retry limit for this hive.
        pub fn worker_retry_limit(&self) -> u32 {
            self.shared().worker_retry_limit()
        }

        /// Updates the retry limit for this hive and returns the previous value.
        pub fn set_worker_retry_limit(&self, limit: u32) -> u32 {
            self.shared().set_worker_retry_limit(limit)
        }

        /// Returns the current retry factor for this hive.
        pub fn worker_retry_factor(&self) -> Duration {
            self.shared().worker_retry_factor()
        }

        /// Updates the retry factor for this hive and returns the previous value.
        pub fn set_worker_retry_factor(&self, duration: Duration) -> Duration {
            self.shared().set_worker_retry_factor(duration)
        }

        pub(super) fn execute(
            task: Task<W>,
            worker: &mut W,
            worker_queues: &T::WorkerQueues,
            shared: &Arc<Shared<Q, T>>,
        ) {
            let (task_id, input, attempt, outcome_tx) = task.into_parts();
            let task_ctx = HiveTaskContext {
                worker_queues,
                shared,
                outcome_tx: outcome_tx.as_ref(),
            };
            let ctx = Context::new(task_id, attempt, Some(&task_ctx));
            // execute the task until it succeeds or we reach maximum retries - this should
            // be the only place where a panic can occur
            let result = worker.apply(input, &ctx);
            let subtask_ids = ctx.into_subtask_ids();
            #[cfg(feature = "retry")]
            let result = match result {
                Err(ApplyError::Retryable { input, error })
                    if subtask_ids.is_none() && shared.can_retry(attempt) =>
                {
                    match shared.try_send_retry(
                        task_id,
                        input,
                        outcome_tx.as_ref(),
                        attempt + 1,
                        worker_queues,
                    ) {
                        Ok(_) => return,
                        Err(task) => Result::<W::Output, ApplyError<W::Input, W::Error>>::Err(
                            ApplyError::Fatal {
                                input: Some(task.into_parts().1),
                                error,
                            },
                        ),
                    }
                }
                result => result,
            };
            let outcome = Outcome::from_worker_result(result, task_id, subtask_ids);
            shared.send_or_store_outcome(outcome, outcome_tx);
        }
    }
}

struct HiveTaskContext<'a, W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    worker_queues: &'a T::WorkerQueues,
    shared: &'a Arc<Shared<Q, T>>,
    outcome_tx: Option<&'a OutcomeSender<W>>,
}

impl<W, Q, T> TaskContext<W::Input> for HiveTaskContext<'_, W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn should_cancel_tasks(&self) -> bool {
        self.shared.is_suspended()
    }

    fn submit_task(&self, input: W::Input) -> TaskId {
        let task = self.shared.prepare_task(input, self.outcome_tx);
        let task_id = task.id();
        self.worker_queues.push(task);
        task_id
    }
}

impl<W, Q, T> fmt::Debug for HiveTaskContext<'_, W, Q, T>
where
    W: Worker,
    Q: Queen<Kind = W>,
    T: TaskQueues<W>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveTaskContext").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Poisoned;
    use crate::bee::stock::{Caller, Thunk, ThunkWorker};
    use crate::hive::{
        outcome_channel, Builder, ChannelBuilder, Outcome, OutcomeIteratorExt, TaskQueuesBuilder,
    };
    use std::collections::HashMap;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_suspend() {
        let hive = ChannelBuilder::empty()
            .num_threads(4)
            .with_worker_default::<ThunkWorker<()>>()
            .build();
        let outcome_iter =
            hive.map((0..10).map(|_| Thunk::of(|| thread::sleep(Duration::from_secs(3)))));
        // Allow first set of tasks to be started.
        thread::sleep(Duration::from_secs(1));
        // There should be 4 active tasks and 6 queued tasks.
        hive.suspend();
        assert_eq!(hive.num_tasks(), (6, 4));
        // Wait for active tasks to complete.
        hive.join();
        assert_eq!(hive.num_tasks(), (6, 0));
        hive.resume();
        // Wait for remaining tasks to complete.
        hive.join();
        assert_eq!(hive.num_tasks(), (0, 0));
        let outputs: Vec<_> = outcome_iter.into_outputs().collect();
        assert_eq!(outputs.len(), 10);
    }

    #[test]
    fn test_spawn_after_poison() {
        let hive = ChannelBuilder::empty()
            .num_threads(4)
            .with_worker_default::<ThunkWorker<()>>()
            .build();
        assert_eq!(hive.max_workers(), 4);
        assert_eq!(hive.alive_workers(), 4);
        // poison hive using private method
        hive.0.as_ref().unwrap().poison();
        // attempt to spawn a new task
        assert!(matches!(hive.grow(1), Err(Poisoned)));
        // make sure the worker count wasn't increased
        assert_eq!(hive.max_workers(), 4);
        assert_eq!(hive.alive_workers(), 4);
    }

    #[test]
    fn test_apply_after_poison() {
        let hive = ChannelBuilder::empty()
            .num_threads(4)
            .with_worker(Caller::of(|i: usize| i * 2))
            .build();
        // poison hive using private method
        hive.0.as_ref().unwrap().poison();
        // submit a task, check that it comes back unprocessed
        let (tx, rx) = outcome_channel();
        let sent_input = 1;
        let sent_task_id = hive.apply_send(sent_input, &tx);
        let outcome = rx.recv().unwrap();
        match outcome {
            Outcome::Unprocessed { input, task_id } => {
                assert_eq!(input, sent_input);
                assert_eq!(task_id, sent_task_id);
            }
            _ => panic!("Expected unprocessed outcome"),
        }
    }

    #[test]
    fn test_swarm_after_poison() {
        let hive = ChannelBuilder::empty()
            .num_threads(4)
            .with_worker(Caller::of(|i: usize| i * 2))
            .build();
        // poison hive using private method
        hive.0.as_ref().unwrap().poison();
        // submit a task, check that it comes back unprocessed
        let (tx, rx) = outcome_channel();
        let inputs = 0..10;
        let task_ids: HashMap<usize, usize> = hive
            .swarm_send(inputs.clone(), &tx)
            .into_iter()
            .zip(inputs)
            .collect();
        for outcome in rx.into_iter().take(10) {
            match outcome {
                Outcome::Unprocessed { input, task_id } => {
                    let expected_input = task_ids.get(&task_id);
                    assert!(expected_input.is_some());
                    assert_eq!(input, *expected_input.unwrap());
                }
                _ => panic!("Expected unprocessed outcome"),
            }
        }
    }
}
