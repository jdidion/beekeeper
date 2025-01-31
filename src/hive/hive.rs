use super::prelude::*;
use super::{Config, DerefOutcomes, HiveInner, OutcomeSender, Shared, SpawnError, TaskSender};
use crate::atomic::Atomic;
use crate::bee::{DefaultQueen, Queen, TaskId, Worker};
use crossbeam_utils::Backoff;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};

#[derive(thiserror::Error, Debug)]
#[error("The hive has been poisoned")]
pub struct Poisoned;

impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
    /// Spawns a new worker thread with the specified index and with access to the `shared` data.
    fn try_spawn(
        thread_index: usize,
        shared: Arc<Shared<W, Q>>,
    ) -> Result<JoinHandle<()>, SpawnError> {
        // spawn a thread that executes the worker loop
        shared.thread_builder().spawn(move || {
            // perform one-time initialization of the worker thread
            Self::init_thread(thread_index, &shared);
            // create a Sentinel that will spawn a new thread on panic until it is cancelled
            let sentinel = Sentinel::new(thread_index, Arc::clone(&shared));
            // create a new Worker instance
            let mut worker = shared.create_worker();
            // execute the main loop
            // get the next task to process - this decrements the queued counter and increments
            // the active counter
            while let Ok(task) = shared.next_task(thread_index) {
                // execute the task until it succeeds or we reach maximum retries - this should
                // be the only place where a panic can occur
                Self::execute(task, &mut worker, &shared);
                // finish the task - decrements the active counter and notifies other threads
                shared.finish_task(false);
            }
            // this is only reachable when the main loop exits due to the task receiver having
            // disconnected; cancel the Sentinel so this thread won't be re-spawned on drop
            sentinel.cancel();
        })
    }

    /// Creates a new `Hive`. This should only be called from `Builder`.
    ///
    /// The `Hive` will attempt to spawn the configured number of worker threads
    /// (`config.num_threads`) but the actual number of threads available may be lower if there
    /// are any errors during spawning.
    pub(super) fn new(config: Config, queen: Q) -> Self {
        let (task_tx, task_rx) = mpsc::channel();
        let shared = Arc::new(Shared::new(config.into_sync(), queen, task_rx));
        shared.init_threads(|thread_index| Self::try_spawn(thread_index, Arc::clone(&shared)));
        Self(Some(HiveInner { task_tx, shared }))
    }

    #[inline]
    fn task_tx(&self) -> &TaskSender<W> {
        &self.0.as_ref().unwrap().task_tx
    }

    #[inline]
    fn shared(&self) -> &Arc<Shared<W, Q>> {
        &self.0.as_ref().unwrap().shared
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
            Self::try_spawn(thread_index, Arc::clone(shared))
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

    /// Sends one input to the `Hive` for processing and returns its ID. The `Outcome`
    /// of the task is sent to the `outcome_tx` channel if provided, otherwise it is retained in
    /// the `Hive` for later retrieval.
    ///
    /// This method is called by all the `*apply*` methods.
    fn send_one(&self, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> TaskId {
        #[cfg(debug_assertions)]
        if self.max_workers() == 0 {
            dbg!("WARNING: no worker threads are active for hive");
        }
        let shared = self.shared();
        let task = shared.prepare_task(input, outcome_tx);
        let task_id = task.id();
        // try to send the task to the hive; if the hive is poisoned or if sending fails, convert
        // the task into an `Unprocessed` outcome and try to send it to the outcome channel; if
        // that fails, store the outcome in the hive
        if let Some(abandoned_task) = if self.is_poisoned() {
            Some(task)
        } else {
            self.task_tx().send(task).err().map(|err| err.0)
        } {
            shared.abandon_task(abandoned_task);
        }
        task_id
    }

    /// Sends one `input` to the `Hive` for procesing and returns the result, blocking until the
    /// result is available. Creates a channel to send the input and receive the outcome. Returns
    /// an [`Outcome`] with the task output or an error.
    pub fn apply(&self, input: W::Input) -> Outcome<W> {
        let (tx, rx) = outcome_channel();
        let task_id = self.send_one(input, Some(tx));
        rx.recv().unwrap_or_else(|_| Outcome::Missing { task_id })
    }

    /// Sends one `input` to the `Hive` for processing and returns its ID. The [`Outcome`] of
    /// the task will be sent to `tx` upon completion.
    pub fn apply_send(&self, input: W::Input, tx: OutcomeSender<W>) -> TaskId {
        self.send_one(input, Some(tx))
    }

    /// Sends one `input` to the `Hive` for processing and returns its ID immediately. The
    /// [`Outcome`] of the task will be retained and available for later retrieval.
    pub fn apply_store(&self, input: W::Input) -> TaskId {
        self.send_one(input, None)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns a `Vec` of their
    /// task IDs. The [`Outcome`]s of the tasks are sent to the `outcome_tx` channel if provided,
    /// otherwise they are retained in the `Hive` for later retrieval.
    ///
    /// The batch is provided as an [`ExactSizeIterator`], which enables the hive to reserve a
    /// range of task IDs (a single atomic operation) rather than one at a time.
    ///
    /// This method is called by all the `swarm*` methods.
    fn send_batch<T>(&self, batch: T, outcome_tx: Option<OutcomeSender<W>>) -> Vec<TaskId>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        #[cfg(debug_assertions)]
        if self.max_workers() == 0 {
            dbg!("WARNING: no worker threads are active for hive");
        }
        let task_tx = self.task_tx();
        let iter = batch.into_iter();
        let (batch_size, _) = iter.size_hint();
        let shared = self.shared();
        let batch = shared.prepare_batch(batch_size, iter, outcome_tx);
        if !self.is_poisoned() {
            batch
                .map(|task| {
                    let task_id = task.id();
                    // try to send the task to the hive; if sending fails, convert the task into an
                    // `Unprocessed` outcome and try to send it to the outcome channel; if that
                    // fails, store the outcome in the hive
                    if let Err(err) = task_tx.send(task) {
                        shared.abandon_task(err.0);
                    }
                    task_id
                })
                .collect()
        } else {
            // if the hive is poisoned, convert all tasks into `Unprocessed` outcomes and try to
            // send them to their outcome channels or store them in the hive
            self.shared().abandon_batch(batch)
        }
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns an iterator over the
    /// [`Outcome`]s in the same order as the inputs.
    ///
    /// This method is more efficient than [`map`](Self::map) when the input is an
    /// [`ExactSizeIterator`].
    pub fn swarm<T>(&self, batch: T) -> impl Iterator<Item = Outcome<W>>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = outcome_channel();
        let task_ids = self.send_batch(batch, Some(tx));
        rx.select_ordered(task_ids)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns an unordered iterator
    /// over the [`Outcome`]s.
    ///
    /// The `Outcome`s will be sent in the order they are completed; use [`swarm`](Self::swarm) to
    /// instead receive the `Outcome`s in the order they were submitted. This method is more
    /// efficient than [`map_unordered`](Self::map_unordered) when the input is an
    /// [`ExactSizeIterator`].
    pub fn swarm_unordered<T>(&self, batch: T) -> impl Iterator<Item = Outcome<W>>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = outcome_channel();
        let task_ids = self.send_batch(batch, Some(tx));
        rx.select_unordered(task_ids)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns a [`Vec`] of task IDs.
    /// The [`Outcome`]s of the tasks will be sent to `tx` upon completion.
    ///
    /// This method is more efficient than [`map_send`](Self::map_send) when the input is an
    /// [`ExactSizeIterator`].
    pub fn swarm_send<T>(&self, batch: T, outcome_tx: OutcomeSender<W>) -> Vec<TaskId>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        self.send_batch(batch, Some(outcome_tx))
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns a [`Vec`] of task IDs.
    /// The [`Outcome`]s of the task are retained and available for later retrieval.
    ///
    /// This method is more efficient than `map_store` when the input is an [`ExactSizeIterator`].
    pub fn swarm_store<T>(&self, batch: T) -> Vec<TaskId>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        self.send_batch(batch, None)
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing and returns an
    /// iterator over the [`Outcome`]s in the same order as the inputs.
    ///
    /// [`swarm`](Self::swarm) should be preferred when `inputs` is an [`ExactSizeIterator`].
    pub fn map(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
    ) -> impl Iterator<Item = Outcome<W>> {
        let (tx, rx) = outcome_channel();
        let task_ids: Vec<_> = inputs
            .into_iter()
            .map(|task| self.apply_send(task, tx.clone()))
            .collect();
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
        let (tx, rx) = outcome_channel();
        // `map` is required (rather than `inspect`) because we need owned items
        let task_ids: Vec<_> = inputs
            .into_iter()
            .map(|task| self.apply_send(task, tx.clone()))
            .collect();
        rx.select_unordered(task_ids)
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing. Returns a [`Vec`]
    /// of task IDs. The [`Outcome`]s of the tasks will be sent to `tx` upon completion.
    ///
    /// [`swarm_send`](Self::swarm_send) should be preferred when `inputs` is an
    /// [`ExactSizeIterator`].
    pub fn map_send(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
        tx: OutcomeSender<W>,
    ) -> Vec<TaskId> {
        inputs
            .into_iter()
            .map(|input| self.apply_send(input, tx.clone()))
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
    pub fn scan<St, T, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        f: F,
    ) -> (OutcomeBatch<W>, St)
    where
        F: FnMut(&mut St, T) -> W::Input,
    {
        let (tx, rx) = outcome_channel();
        let (task_ids, fold_value) = self.scan_send(items, tx, init, f);
        let outcomes = rx.select_unordered(task_ids).into();
        (outcomes, fold_value)
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `F` returns an input that is sent to the `Hive` for processing,
    /// or an error. Returns an [`OutcomeBatch`] of the outputs, a [`Vec`] of errors, and the final
    /// state value.
    pub fn try_scan<St, T, E, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        mut f: F,
    ) -> (OutcomeBatch<W>, Vec<E>, St)
    where
        F: FnMut(&mut St, T) -> Result<W::Input, E>,
    {
        let (tx, rx) = outcome_channel();
        let (task_ids, errors, fold_value) = items.into_iter().fold(
            (Vec::new(), Vec::new(), init),
            |(mut task_ids, mut errors, mut acc), inp| {
                match f(&mut acc, inp) {
                    Ok(input) => task_ids.push(self.apply_send(input, tx.clone())),
                    Err(err) => errors.push(err),
                }
                (task_ids, errors, acc)
            },
        );
        let outcomes = rx.select_unordered(task_ids).into();
        (outcomes, errors, fold_value)
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing.
    /// The outputs are sent to `tx` in the order they become available. Returns a [`Vec`] of the
    /// task IDs and the final state value.
    pub fn scan_send<St, T, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        tx: OutcomeSender<W>,
        init: St,
        mut f: F,
    ) -> (Vec<TaskId>, St)
    where
        F: FnMut(&mut St, T) -> W::Input,
    {
        items
            .into_iter()
            .fold((Vec::new(), init), |(mut task_ids, mut acc), item| {
                let input = f(&mut acc, item);
                task_ids.push(self.apply_send(input, tx.clone()));
                (task_ids, acc)
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing,
    /// or an error. The outputs are sent to `tx` in the order they become available. This
    /// function returns the final state value and a [`Vec`] of results, where each result is
    /// either a task ID or an error.
    pub fn try_scan_send<St, T, E, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        tx: OutcomeSender<W>,
        init: St,
        mut f: F,
    ) -> (Vec<Result<TaskId, E>>, St)
    where
        F: FnMut(&mut St, T) -> Result<W::Input, E>,
    {
        items
            .into_iter()
            .fold((Vec::new(), init), |(mut results, mut acc), inp| {
                results.push(f(&mut acc, inp).map(|input| self.apply_send(input, tx.clone())));
                (results, acc)
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing.
    /// This function returns the final state value and a [`Vec`] of task IDs. The [`Outcome`]s of
    /// the tasks are retained and available for later retrieval.
    pub fn scan_store<St, T, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        mut f: F,
    ) -> (Vec<TaskId>, St)
    where
        F: FnMut(&mut St, T) -> W::Input,
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
    pub fn try_scan_store<St, T, F, E>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        mut f: F,
    ) -> (Vec<Result<TaskId, E>>, St)
    where
        F: FnMut(&mut St, T) -> Result<W::Input, E>,
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

    /// Returns the [`MutexGuard`](parking_lot::MutexGuard) for the [`Queen`].
    ///
    /// Note that the `Queen` will remain locked until the returned guard is dropped, and that
    /// locking the `Queen` prevents new worker threads from being started.
    pub fn queen(&self) -> impl Deref<Target = Q> + '_ {
        self.shared().queen.lock()
    }

    /// Returns the number of worker threads that have been requested, i.e., the maximum number of
    /// tasks that could be processed concurrently. This may be greater than
    /// [`active_workers`](Self::active_workers) if any of the worker threads failed to start.
    pub fn max_workers(&self) -> usize {
        self.shared().config.num_threads.get_or_default()
    }

    /// Returns the number of worker threads that have been successfully started. This may be
    /// fewer than [`max_workers`](Self::max_workers) if any of the worker threads failed to start.
    pub fn alive_workers(&self) -> usize {
        self.shared()
            .spawn_results
            .lock()
            .iter()
            .filter(|result| result.is_ok())
            .count()
    }

    /// Returns `true` if there are any "dead" worker threads that failed to spawn.
    pub fn has_dead_workers(&self) -> bool {
        self.shared()
            .spawn_results
            .lock()
            .iter()
            .any(|result| result.is_err())
    }

    /// Attempts to respawn any dead worker threads. Returns the number of worker threads that were
    /// successfully respawned.
    pub fn revive_workers(&self) -> usize {
        let shared = self.shared();
        shared
            .respawn_dead_threads(|thread_index| Self::try_spawn(thread_index, Arc::clone(shared)))
    }

    /// Returns the number of tasks currently (queued for processing, being processed).
    pub fn num_tasks(&self) -> (u64, u64) {
        self.shared().num_tasks()
    }

    /// Returns the number of times one of this `Hive`'s worker threads has panicked.
    pub fn num_panics(&self) -> usize {
        self.shared().num_panics.get()
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
    /// use beekeeper::hive::Builder;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .num_threads(4)
    ///     .build_with_default::<ThunkWorker<()>>();
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
    pub fn resume_send(&self, outcome_tx: OutcomeSender<W>) -> Vec<TaskId> {
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

    /// Consumes this `Hive` and attempts to return a [`Husk`] containing the remnants of this
    /// `Hive`, including any stored task outcomes, and all the data necessary to create a new
    /// `Hive`.
    ///
    /// If this `Hive` has been cloned, and those clones have not been dropped, this method
    /// returns `None` since it cannot take exclusive ownership of the internal shared data.
    ///
    /// This method first joins on the `Hive` to wait for all tasks to finish.
    pub fn try_into_husk(mut self) -> Option<Husk<W, Q>> {
        if self.shared().num_referrers() > 1 {
            return None;
        }
        // take the inner value and replace it with `None`
        let inner = self.0.take().unwrap();
        // wait for all tasks to finish
        inner.shared.wait_on_done();
        // drop the task sender so receivers will drop automatically
        drop(inner.task_tx);
        // wait for worker threads to drop, then take ownership of the shared data and convert it
        // into a Husk
        let mut shared = inner.shared;
        let mut backoff = None::<Backoff>;
        loop {
            // TODO: may want to have some timeout or other kind of limit to prevent this from
            // looping forever if a worker thread somehow gets stuck, or if the `num_referrers`
            // counter is corrupted
            shared = match Arc::try_unwrap(shared) {
                Ok(shared) => {
                    return Some(shared.try_into_husk());
                }
                Err(shared) => {
                    backoff.get_or_insert_with(Backoff::new).spin();
                    shared
                }
            };
        }
    }
}

impl<W: Worker + Send + Sync + Default> Default for Hive<W, DefaultQueen<W>> {
    fn default() -> Self {
        Builder::default().build_with_default::<W>()
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Clone for Hive<W, Q> {
    /// Creates a shallow copy of this `Hive` containing references to its same internal state,
    /// i.e., all clones of a `Hive` submit tasks to the same shared worker thread pool.
    fn clone(&self) -> Self {
        let inner = self.0.as_ref().unwrap();
        self.shared().referrer_is_cloning();
        Self(Some(inner.clone()))
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Clone for HiveInner<W, Q> {
    fn clone(&self) -> Self {
        HiveInner {
            task_tx: self.task_tx.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Debug for Hive<W, Q> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(inner) = self.0.as_ref() {
            f.debug_struct("Hive")
                .field("task_tx", &inner.task_tx)
                .field("shared", &inner.shared)
                .finish()
        } else {
            f.write_str("Hive {}")
        }
    }
}

impl<W: Worker, Q: Queen<Kind = W>> PartialEq for Hive<W, Q> {
    fn eq(&self, other: &Hive<W, Q>) -> bool {
        Arc::ptr_eq(self.shared(), other.shared())
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Eq for Hive<W, Q> {}

impl<W: Worker, Q: Queen<Kind = W>> DerefOutcomes<W> for Hive<W, Q> {
    #[inline]
    fn outcomes_deref(&self) -> impl Deref<Target = HashMap<TaskId, Outcome<W>>> {
        self.shared().outcomes()
    }

    #[inline]
    fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<TaskId, Outcome<W>>> {
        self.shared().outcomes()
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Drop for Hive<W, Q> {
    fn drop(&mut self) {
        // if this Hive has already been turned into a Husk, it's inner value will be `None`
        if let Some(inner) = self.0.as_ref() {
            // reduce the referrer count
            let _ = inner.shared.referrer_is_dropping();
            // if this Hive is the only one with a pointer to the shared data, poison it
            // to prevent any worker threads that still have access to the shared data from
            // re-spawning.
            if inner.shared.num_referrers() == 0 {
                inner.shared.poison();
            }
        }
    }
}

/// Sentinel for a worker thread. Until the sentinel is cancelled, it will respawn the worker
/// thread if it panics.
struct Sentinel<W: Worker, Q: Queen<Kind = W>> {
    thread_index: usize,
    shared: Arc<Shared<W, Q>>,
    active: bool,
}

impl<W: Worker, Q: Queen<Kind = W>> Sentinel<W, Q> {
    fn new(thread_index: usize, shared: Arc<Shared<W, Q>>) -> Self {
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

impl<W: Worker, Q: Queen<Kind = W>> Drop for Sentinel<W, Q> {
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
                        Hive::try_spawn(thread_index, Arc::clone(&self.shared))
                    });
            }
        }
    }
}

#[cfg(not(feature = "affinity"))]
mod no_affinity {
    use crate::bee::{Queen, Worker};
    use crate::hive::{Hive, Shared};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        #[inline]
        pub(super) fn init_thread(_: usize, _: &Shared<W, Q>) {}
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use crate::bee::{Queen, Worker};
    use crate::hive::cores::Cores;
    use crate::hive::{Hive, Poisoned, Shared};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        /// Tries to pin the worker thread to a specific CPU core.
        #[inline]
        pub(super) fn init_thread(thread_index: usize, shared: &Shared<W, Q>) {
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
    use crate::hive::Hive;

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        /// Returns the batch size for worker threads.
        pub fn worker_batch_size(&self) -> usize {
            self.shared().batch_size()
        }

        /// Sets the batch size for worker threads. This will block the current thread until all
        /// worker thread queues can be resized.
        pub fn set_worker_batch_size(&self, batch_size: usize) {
            self.shared().set_batch_size(batch_size);
        }
    }
}

#[cfg(not(feature = "retry"))]
mod no_retry {
    use crate::bee::{Queen, Worker};
    use crate::hive::{Hive, Outcome, Shared, Task};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        #[inline]
        pub(super) fn execute(task: Task<W>, worker: &mut W, shared: &Shared<W, Q>) {
            let (input, ctx, outcome_tx) = task.into_parts();
            let result = worker.apply(input, &ctx);
            let outcome = Outcome::from_worker_result(result, ctx.task_id());
            shared.send_or_store_outcome(outcome, outcome_tx);
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use crate::bee::{ApplyError, Queen, Worker};
    use crate::hive::{Hive, Outcome, Shared, Task};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        #[inline]
        pub(super) fn execute(task: Task<W>, worker: &mut W, shared: &Shared<W, Q>) {
            let (input, mut ctx, outcome_tx) = task.into_parts();
            match worker.apply(input, &ctx) {
                Err(ApplyError::Retryable { input, .. }) if shared.can_retry(&ctx) => {
                    ctx.inc_attempt();
                    shared.queue_retry(input, ctx, outcome_tx);
                }
                result => {
                    let outcome = Outcome::from_worker_result(result, ctx.task_id());
                    shared.send_or_store_outcome(outcome, outcome_tx);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Poisoned;
    use crate::bee::stock::{Caller, Thunk, ThunkWorker};
    use crate::hive::{outcome_channel, Builder, Outcome, OutcomeIteratorExt};
    use std::collections::HashMap;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_suspend() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with_default::<ThunkWorker<()>>();
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
        let hive = Builder::new()
            .num_threads(4)
            .build_with_default::<ThunkWorker<()>>();
        assert_eq!(hive.max_workers(), 4);
        assert_eq!(hive.alive_workers(), 4);
        // poison hive using private method
        hive.shared().poison();
        // attempt to spawn a new task
        assert!(matches!(hive.grow(1), Err(Poisoned)));
        // make sure the worker count wasn't increased
        assert_eq!(hive.max_workers(), 4);
        assert_eq!(hive.alive_workers(), 4);
    }

    #[test]
    fn test_apply_after_poison() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with(Caller::of(|i: usize| i * 2));
        // poison hive using private method
        hive.shared().poison();
        // submit a task, check that it comes back unprocessed
        let (tx, rx) = outcome_channel();
        let sent_input = 1;
        let sent_task_id = hive.apply_send(sent_input, tx.clone());
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
        let hive = Builder::new()
            .num_threads(4)
            .build_with(Caller::of(|i: usize| i * 2));
        // poison hive using private method
        hive.shared().poison();
        // submit a task, check that it comes back unprocessed
        let (tx, rx) = outcome_channel();
        let inputs = 0..10;
        let task_ids: HashMap<usize, usize> = hive
            .swarm_send(inputs.clone(), tx)
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
