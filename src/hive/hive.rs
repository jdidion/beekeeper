use super::{
    outcome_channel, Config, Hive, Husk, Outcome, OutcomeBatch, OutcomeDerefStore,
    OutcomeIteratorExt, OutcomeSender, OutcomesDeref, Shared,
};
use crate::atomic::Atomic;
use crate::task::{Queen, Worker};
use crossbeam_utils::Backoff;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut, Range};
use std::sync::{mpsc, Arc};
use std::thread;

impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
    /// Spawns a new worker thread.
    fn spawn(index: usize, shared: Arc<Shared<W, Q>>) {
        shared
            .thread_builder()
            .spawn(move || {
                Self::init_thread(index, &shared);
                // Will spawn a new thread on panic until it is cancelled
                let sentinel = Sentinel::new(index, Arc::clone(&shared));
                let mut worker = shared.create_worker();
                let mut active = true;
                while active {
                    // Shutdown this thread if the pool has become smaller
                    active = shared
                        // Get the next task - increments the counter
                        .next_task()
                        // Execute the task until it succeeds or we reach maximum retries - this
                        // should be the only place where a panic might occur
                        .map(|task| Self::execute(task, &mut worker, &shared))
                        // Finish the task - decrements the counter and notifies other threads
                        .inspect(|_| shared.finish_task(false))
                        // Shutdown this thread if the receiver hung up
                        .unwrap_or(false);
                }
                sentinel.cancel();
            })
            .unwrap();
    }

    pub(super) fn new(config: Config, queen: Q) -> Self {
        let num_threads = config.num_threads.get().unwrap_or(0);
        let (task_tx, task_rx) = mpsc::channel();
        let shared = Arc::new(Shared::new(config.into_sync(), queen, task_rx));
        let hive = Self { task_tx, shared };
        hive.brood(0..num_threads);
        hive
    }

    /// Increases the number of worker threads by `num_threads`.
    pub fn grow(&self, num_threads: usize) {
        if num_threads > 0 {
            let start = self.shared.add_threads(num_threads);
            self.brood(start..start + num_threads);
        }
    }

    /// Sets the number of worker threads to the number of available CPU cores. Returns the number
    /// of new threads spun up (which may be `0`).
    pub fn use_all_cores(&self) -> usize {
        let num_threads = num_cpus::get();
        let cur_threads = self.shared.ensure_threads(num_threads);
        if num_threads > cur_threads {
            self.brood(cur_threads..num_threads);
            num_threads - cur_threads
        } else {
            0
        }
    }

    /// Spawns a worker thread for each thread index in the specified range. Each worker thread
    /// gets access to this `Hive`'s shared data.
    fn brood(&self, thread_indices: Range<usize>) {
        for thread_index in thread_indices {
            Self::spawn(thread_index, Arc::clone(&self.shared));
        }
    }

    /// Sends one input to the `Hive` for processing and returns its index. The `Outcome`
    /// of the task is sent to the `outcome_tx` channel if provided, otherwise it is retained in
    /// the `Hive` for later retrieval.
    ///
    /// This method is called by all the `*apply*` methotds.
    fn send_one(&self, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> usize {
        #[cfg(debug_assertions)]
        if self.num_threads() == 0 {
            dbg!("WARNING: no worker threads are active for hive");
        }
        let task = self.shared.prepare_task(input, outcome_tx);
        let index = task.index();
        self.task_tx
            .send(task)
            .expect("unable to send task into queue");
        index
    }

    /// Sends one `input` to the `Hive` for procesing and returns the result, blocking until the
    /// result is available.
    ///
    /// Creates a channel to send the input and receive the outcome. Panics if the channel hangs
    /// up before the outcome is received.
    pub fn try_apply(&self, input: W::Input) -> Outcome<W> {
        let (tx, rx) = outcome_channel();
        self.send_one(input, Some(tx));
        rx.recv().expect("channel hung up unexpectedly")
    }

    /// Sends one `input` to the `Hive` for processing and returns its index. The `Outcome` of the
    /// task will be sent to `tx` upon completion.
    pub fn apply_send(&self, input: W::Input, tx: OutcomeSender<W>) -> usize {
        self.send_one(input, Some(tx))
    }

    /// Sends one `input` to the `Hive` for processing and returns its index immediately. The
    /// `Outcome` of the task will be retained and available for later retrieval.
    pub fn apply_store(&self, input: W::Input) -> usize {
        self.send_one(input, None)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns a `Vec` of their
    /// indices. The `Outcome`s of the tasks are sent to the `outcome_tx` channel if provided,
    /// otherwise they are retained in the `Hive` for later retrieval.
    ///
    /// The batch is provided as an `ExactSizeIterator`, which enables the hive to reserve a range
    /// of indicies (a single atomic operation) rather than one at a time.
    ///
    /// This method is called by all the `swarm*` methods.
    fn send_batch<T>(&self, batch: T, outcome_tx: Option<OutcomeSender<W>>) -> Vec<usize>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        #[cfg(debug_assertions)]
        if self.num_threads() == 0 {
            dbg!("WARNING: no worker threads are active for hive");
        }
        let iter = batch.into_iter();
        let (batch_size, _) = iter.size_hint();
        self.shared
            .prepare_batch(batch_size, iter, outcome_tx)
            .map(|task| {
                let index = task.index();
                self.task_tx
                    .send(task)
                    .expect("unable to send task into queue");
                index
            })
            .collect()
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns an iterator over the
    /// `Outcome`s in the same order as the inputs.
    ///
    /// This method is more efficient than `map_iter` when the input is an `ExactSizeIterator`.
    pub fn swarm<T>(&self, batch: T) -> impl Iterator<Item = Outcome<W>>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = outcome_channel();
        let num_tasks = self.send_batch(batch, Some(tx)).len();
        rx.take_ordered(num_tasks)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns an iterator over the
    /// `Outcome`s.
    ///
    /// The `Outcome`s will be sent in the order they are completed. Use `swarm` to instead
    /// receive the `Outcome`s in the order they were submitted.
    ///
    /// This method is more efficient than `map_unordered` when the input is an
    /// `ExactSizeIterator`.
    pub fn swarm_unordered<T>(&self, batch: T) -> impl Iterator<Item = Outcome<W>>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = outcome_channel();
        let num_tasks = self.send_batch(batch, Some(tx)).len();
        rx.into_iter().take(num_tasks)
    }

    /// Sends a `batch`` of inputs to the `Hive` for processing, and returns a range of indices.
    /// The `Outcome`s of the tasks will be sent to `tx` upon completion.
    ///
    /// This method is more efficient than `map_send` when the input is an `ExactSizeIterator`.
    pub fn swarm_send<T>(&self, batch: T, outcome_tx: OutcomeSender<W>) -> Vec<usize>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        self.send_batch(batch, Some(outcome_tx))
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns a `Vec` of indicies.
    /// The `Outcome`s of the task are retained and available for later retrieval.
    ///
    /// This method is more efficient than `map_store` when the input is an `ExactSizeIterator`.
    pub fn swarm_store<T>(&self, batch: T) -> Vec<usize>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        self.send_batch(batch, None)
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing and returns an
    /// iterator over the `Outcome`s in the same order as the inputs.
    ///
    /// `swarm` should be preferred when `inputs` is an `ExactSizeIterator`.
    pub fn map(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
    ) -> impl Iterator<Item = Outcome<W>> {
        let (tx, rx) = outcome_channel();
        let num_tasks = inputs
            .into_iter()
            .map(|task| self.apply_send(task, tx.clone()))
            .count();
        rx.take_ordered(num_tasks)
    }

    /// Iterates over `inputs`, sends each one to the `Hive` for processing, and returns an
    /// iterator over the `Outcome`s in order they become available.
    ///
    /// `swarm_unordered` should be preferred when `inputs` is an `ExactSizeIterator`.
    pub fn map_unordered(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
    ) -> impl Iterator<Item = Outcome<W>> {
        let (tx, rx) = outcome_channel();
        let num_tasks = inputs
            .into_iter()
            .map(|task| self.apply_send(task, tx.clone()))
            .count();
        rx.into_iter().take(num_tasks)
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing. Returns a `Vec` of
    /// task indices. The `Outcome`s of the tasks will be sent to `tx` upon completion.
    ///
    /// `swarm_send` should be preferred when `inputs` is an `ExactSizeIterator`.
    pub fn map_send(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
        tx: OutcomeSender<W>,
    ) -> Vec<usize> {
        inputs
            .into_iter()
            .map(|input| self.apply_send(input, tx.clone()))
            .collect()
    }

    /// Iterates over `inputs` and sends each one to the `Hive` for processing. Returns a `Vec` of
    /// task indices. The `Outcome`s of the task are retained and available for later retrieval.
    ///
    /// `swarm_store` should be preferred when `inputs` is an `ExactSizeIterator`.
    pub fn map_store(&self, inputs: impl IntoIterator<Item = W::Input>) -> Vec<usize> {
        inputs
            .into_iter()
            .map(|input| self.apply_store(input))
            .collect()
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `F` returns an input that is sent to the `Hive` for processing.
    /// This function returns an `OutcomeBatch` of the outputs and the final state value.
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
        let (indices, fold_value) = self.scan_send(items, tx, init, f);
        let outcomes = rx.into_iter().take(indices.len()).map(Outcome::into).into();
        (outcomes, fold_value)
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `F` returns an input that is sent to the `Hive` for processing,
    /// or an error. This function returns an `OutcomeBatch` of the outputs and the final state
    /// value, or an error if any calls to `f` resulted in an error.
    pub fn try_scan<St, T, E, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        f: F,
    ) -> Result<(OutcomeBatch<W>, St), E>
    where
        F: FnMut(&mut St, T) -> Result<W::Input, E>,
    {
        let (tx, rx) = outcome_channel();
        let (indices, fold_value) = self.try_scan_send(items, tx, init, f)?;
        let outcomes = rx.into_iter().take(indices.len()).map(Outcome::into).into();
        Ok((outcomes, fold_value))
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing.
    /// The outputs are sent to `tx` in the order they become available. This function returns
    /// a `Vec` of the task indices and the final state value.
    pub fn scan_send<St, T, F>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        tx: OutcomeSender<W>,
        init: St,
        mut f: F,
    ) -> (Vec<usize>, St)
    where
        F: FnMut(&mut St, T) -> W::Input,
    {
        inputs
            .into_iter()
            .fold((Vec::new(), init), |(mut indices, mut acc), item| {
                let input = f(&mut acc, item);
                indices.push(self.apply_send(input, tx.clone()));
                (indices, acc)
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing,
    /// or an error. The outputs are sent to `tx` in the order they become available. This
    /// function returns a `Vec` of the task indicies and final state value, or an error if any
    /// calls to `f` resulted in an error.
    pub fn try_scan_send<St, T, E, F>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        tx: OutcomeSender<W>,
        init: St,
        mut f: F,
    ) -> Result<(Vec<usize>, St), E>
    where
        F: FnMut(&mut St, T) -> Result<W::Input, E>,
    {
        inputs
            .into_iter()
            .try_fold((Vec::new(), init), |(mut indicies, mut acc), inp| {
                let input = f(&mut acc, inp)?;
                indicies.push(self.apply_send(input, tx.clone()));
                Ok((indicies, acc))
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing.
    /// This function returns the final state value and a `Vec` of indices. The `Outcome`s of the
    /// tasks are retained and available for later retrieval.
    pub fn scan_store<St, T, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        mut f: F,
    ) -> (Vec<usize>, St)
    where
        F: FnMut(&mut St, T) -> W::Input,
    {
        items
            .into_iter()
            .fold((Vec::new(), init), |(mut indices, mut acc), item| {
                let input = f(&mut acc, item);
                indices.push(self.apply_store(input));
                (indices, acc)
            })
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `f` returns an input that is sent to the `Hive` for processing,
    /// or an error. This function returns the final value of the state value and a `Vec` of
    /// indices, or an error if any calls to `f` resulted in an error. The `Outcome`s of the tasks
    /// are retained and available for later retrieval.
    pub fn try_scan_store<St, T, F, E>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        mut f: F,
    ) -> Result<(Vec<usize>, St), E>
    where
        F: FnMut(&mut St, T) -> Result<W::Input, E>,
    {
        items
            .into_iter()
            .try_fold((Vec::new(), init), |(mut indices, mut acc), item| {
                let input = f(&mut acc, item)?;
                indices.push(self.apply_store(input));
                Ok((indices, acc))
            })
    }

    /// Returns the number of worker threads, i.e., the maximum number of tasks that can be
    /// processed concurrently.
    pub fn num_threads(&self) -> usize {
        self.shared.config.num_threads.get_or_default()
    }

    /// Returns the number of tasks currently queued for processing.
    pub fn num_tasks_queued(&self) -> usize {
        self.shared.num_tasks_queued.get()
    }

    /// Returns the number of tasks currently being processed.
    pub fn num_tasks_active(&self) -> usize {
        self.shared.num_tasks_active.get()
    }

    /// Returns the number of times one of this `Hive`'s worker threads has panicked.
    pub fn num_panics(&self) -> usize {
        self.shared.num_panics.get()
    }

    /// Sets the suspended flag, which notifies worker threads that they a) MAY terminate their
    /// current task early (returning an `Unprocessed` outcome), and b) MUST not accept new tasks,
    /// and instead block until the suspended flag is cleared.
    ///
    /// Call `resume` to unset the suspended flag and continue processing tasks.
    ///
    /// Note: this does *not* prevent new tasks from being queued, and there is a window of time
    /// (~1 second) after the suspended flag is set within which a worker thread may still accept a
    /// new task.
    ///
    /// # Examples
    /// # use drudge::hive::Builder;
    /// # use drudge::util::{Thunk, ThunkWorker};
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .num_threads(4)
    ///     .build_with_default::<ThunkWorker<()>>();
    /// hive.map((0..10).map(|_| Thunk.of(|| thread::sleep(Duration::from_secs(5));));
    /// thread::sleep(Duration::from_secs(1)); // Allow first set of tasks to be processed.
    /// // There should be 4 active tasks and 6 queued tasks.
    /// hive.suspend();
    /// assert_eq!(hive.num_tasks_active(), 4);
    /// assert_eq!(hive.num_tasks_queued(), 6);
    /// // Wait for active tasks to complete.
    /// hive.join();
    /// assert_eq!(hive.num_tasks_active(), 0);
    /// assert_eq!(hive.num_tasks_queued(), 6);
    /// hive.resume();
    /// // Wait for remaining tasks to complete.
    /// hive.join();
    /// }
    pub fn suspend(&self) {
        self.shared.set_suspended(true);
    }

    /// Unsets the suspended flag, allowing worker threads to continue processing queued tasks.
    pub fn resume(&self) {
        self.shared.set_suspended(false);
    }

    fn take_unprocessed_inputs(&self) -> impl ExactSizeIterator<Item = W::Input> {
        self.shared
            .take_unprocessed()
            .into_iter()
            .map(|outcome| match outcome {
                Outcome::Unprocessed { input, index: _ } => input,
                _ => unreachable!(),
            })
    }

    /// Resume this `Hive` and re-submit any unprocessed tasks for processing, with their results
    /// to be sent to `tx`. Returns a `Vec` of task indices that were resumed.
    pub fn resume_send(&self, outcome_tx: OutcomeSender<W>) -> Vec<usize> {
        self.shared
            .set_suspended(false)
            .then(|| self.swarm_send(self.take_unprocessed_inputs(), outcome_tx))
            .unwrap_or_default()
    }

    /// Resume this `Hive` and re-submit any unprocessed tasks for processing, with their results
    /// to be stored in the queue. Returns a `Vec` of task indices that were resumed.
    pub fn resume_store(&self) -> Vec<usize> {
        self.shared
            .set_suspended(false)
            .then(|| self.swarm_store(self.take_unprocessed_inputs()))
            .unwrap_or_default()
    }

    /// Returns `true` if the cancelled flag is set.
    pub fn is_suspended(&self) -> bool {
        self.shared.is_suspended()
    }

    /// Returns any stored `Outcome`s.
    pub fn take_stored(&self) -> HashMap<usize, Outcome<W>> {
        self.shared.take_outcomes()
    }

    /// Blocks this thread until all tasks finish.
    pub fn join(&self) {
        self.shared.wait_on_done();
    }

    /// Consumes this `Hive` and returns a `Husk` containing the remnants of this `Hive`, including
    /// any stored task outcomes, and all the data necessary to create a new `Hive`. If `join` is
    /// `true`, this method will block until all tasks finish before consuming this `Hive`,
    /// otherwise the
    pub fn into_husk(self) -> Husk<W, Q> {
        self.join();
        drop(self.task_tx);
        self.shared.no_work_notify_all();
        let mut backoff = None::<Backoff>;
        while Arc::strong_count(&self.shared) > 1 {
            backoff.get_or_insert_with(|| Backoff::new()).spin();
        }
        let shared = match Arc::try_unwrap(self.shared) {
            Ok(data) => data,
            Err(_) => panic!("Arc::try_unwrap failed"),
        };
        shared.into_husk()
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Clone for Hive<W, Q> {
    fn clone(&self) -> Self {
        Self {
            task_tx: self.task_tx.clone(),
            shared: self.shared.clone(),
        }
    }
}

impl<W: Worker, Q: Queen<Kind = W>> PartialEq for Hive<W, Q> {
    fn eq(&self, other: &Hive<W, Q>) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl<W: Worker, Q: Queen<Kind = W>> Eq for Hive<W, Q> {}

impl<W: Worker, Q: Queen<Kind = W>> OutcomesDeref<W> for Hive<W, Q> {
    fn outcomes_deref(&self) -> impl Deref<Target = HashMap<usize, Outcome<W>>> {
        self.shared.outcomes()
    }

    fn outcomes_deref_mut(&mut self) -> impl DerefMut<Target = HashMap<usize, Outcome<W>>> {
        self.shared.outcomes()
    }
}

impl<W: Worker, Q: Queen<Kind = W>> OutcomeDerefStore<W> for Hive<W, Q> {}

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
        self.shared.finish_task(thread::panicking());
        // the thread is only respawned if the sentinel is active
        if self.active {
            Hive::spawn(self.thread_index, Arc::clone(&self.shared));
        }
    }
}

#[cfg(not(feature = "affinity"))]
mod no_affinity {
    use crate::hive::{Hive, Shared};
    use crate::task::{Queen, Worker};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        #[inline]
        pub(super) fn init_thread(_: usize, _: &Shared<W, Q>) {}
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use crate::hive::cores::Cores;
    use crate::hive::{Hive, Shared};
    use crate::task::{Queen, Worker};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        #[inline]
        pub(super) fn init_thread(thread_index: usize, shared: &Shared<W, Q>) {
            if let Some(core) = shared.get_core_affinity(thread_index) {
                core.try_pin_current();
            }
        }

        /// Increases the number of worker threads by `num_threads`.
        ///
        /// The provided `affinity` specifies additional CPU core indices to which the worker threads
        /// may be pinned - these are added to the existing pool of core indicies (if any).
        pub fn grow_with_affinity(&self, num_threads: usize, affinity: &Cores) {
            self.shared.add_core_affinity(affinity);
            self.grow(num_threads);
        }

        /// Sets the number of worker threads to the number of available CPU cores. An attempt is made
        /// to pin each worker thread to a different CPU core.
        ///
        /// Returns the number of new threads spun up (if any).
        pub fn use_all_cores_with_affinity(&self) -> usize {
            self.shared.add_core_affinity(&Cores::all());
            self.use_all_cores()
        }
    }
}

#[cfg(not(feature = "retry"))]
mod no_retry {
    use crate::hive::{Hive, Outcome, Shared, Task};
    use crate::task::{Queen, Worker};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        #[inline]
        pub(super) fn execute(task: Task<W>, worker: &mut W, shared: &Shared<W, Q>) -> bool {
            let (input, ctx, outcome_tx) = task.into_parts();
            let result = worker.apply(input, &ctx);
            let outcome = Outcome::from_worker_result(result, ctx.index(), false);
            // Send the outcome to the receiver or store it in the hive
            if let Some(tx) = outcome_tx {
                tx.send(outcome).is_ok()
            } else {
                shared.add_outcome(outcome);
                true
            }
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use crate::hive::{Hive, Outcome, Shared, Task};
    use crate::task::{ApplyError, Queen, Worker};

    impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
        #[inline]
        pub(super) fn execute(task: Task<W>, worker: &mut W, shared: &Shared<W, Q>) -> bool {
            let (input, mut ctx, outcome_tx) = task.into_parts();
            match worker.apply(input, &ctx) {
                Err(ApplyError::Retryable { input, .. }) if shared.can_retry(&ctx) => {
                    ctx.inc_attempt();
                    shared.queue_retry(input, ctx, outcome_tx);
                    true
                }
                result => {
                    let outcome = Outcome::from_worker_result(result, ctx.index(), true);
                    // Send the outcome to the receiver or store it in the hive
                    if let Some(tx) = outcome_tx {
                        tx.send(outcome).is_ok()
                    } else {
                        shared.add_outcome(outcome);
                        true
                    }
                }
            }
        }
    }

    // #[inline]
    // fn retry<W: Worker, Q: Queen<Kind = W>>(
    //     mut input: W::Input,
    //     ctx: &mut Context,
    //     worker: &mut W,
    //     shared: &Shared<W, Q>,
    // ) -> Outcome<W> {
    //     // Execute the task until it succeeds or we reach maximum retries
    //     let max_retries = shared.max_retries();
    //     debug_assert!(max_retries > 0);
    //     loop {
    //         ctx.inc_attempt();
    //         input = match worker.apply(input, &ctx) {
    //             Err(ApplyError::Retryable { input, .. }) if ctx.attempt() < max_retries => {
    //                 if let Some(delay) = shared.get_delay(ctx.attempt()) {
    //                     thread::sleep(delay);
    //                 }
    //                 input
    //             }
    //             result => {
    //                 return Outcome::from_worker_result(result, ctx.index(), true);
    //             }
    //         }
    //     }
    // }
}
