#[cfg(feature = "affinity")]
use crate::hive::Cores;
use crate::hive::{
    self, spawn, HiveError, HiveResult, Husk, Outcome, OutcomeIteratorExt, OutcomeSender, Shared,
    Stored, TaskResult, TaskSender,
};
use crate::task::{Queen, Worker};
use crossbeam_utils::Backoff;
use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// A pool of worker threads that each execute the same function. A `Hive` is created by a
/// [`Builder`].
///
/// A `Hive` has a [`Queen`] that creates a [`Worker`] for each thread in the pool. The `Worker` has
/// a [`try_apply`] method that is called to execute a task, which consists of an input value, a
/// `Context`, and an optional output channel. If the `Worker` processes the task successfully, the
/// output is sent to the output channel if one is provided, otherwise it is retained in the `Hive`
/// for later retrieval. If the `Worker` encounters an error, then it will retry the task if the
/// error is retryable and the `Hive` has been configured to retry tasks. If a task cannot be
/// processed after the maximum number of retries, then an error is sent to the output channel or
/// retained in the `Hive` for later retrieval.
///
/// A `Worker` should never panic, but if it does, the worker thread will terminate and the `Hive`
/// will spawn a new worker thread with a new `Worker`.
///
/// When a `Hive` is dropped, all the worker threads are terminated automatically. Prior to
/// dropping the `Hive`, the `into_husk()` method can be called to retrieve all of the `Hive` data
/// necessary to build a new `Hive`, as well as any stored outcomes (those that were not sent to an
/// output channel).
///
/// [`Builder`]: hive/struct.Builder.html
/// [`Worker`]: task/trait.Worker.html
/// [`Queen`]: task/trait.Queen.html
/// [`try_apply`]: task/trait.Worker.html#method.try_apply
#[derive(Debug)]
pub struct Hive<W: Worker, Q: Queen<Kind = W>> {
    task_tx: TaskSender<W>,
    shared: Arc<Shared<W, Q>>,
}

impl<W: Worker, Q: Queen<Kind = W>> Hive<W, Q> {
    pub(crate) fn new(task_tx: TaskSender<W>, shared: Arc<Shared<W, Q>>) -> Self {
        Self { task_tx, shared }
    }

    /// Sends one input to the `Hive` for processing and returns its index. The `Outcome`
    /// of the task is sent to the `outcome_tx` channel if provided, otherwise it is retained in
    /// the `Hive` for later retrieval.
    ///
    /// This method is called by all the `*apply*` methotds.
    fn send_one(&self, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> usize {
        #[cfg(debug_assertions)]
        if self.max_count() == 0 {
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
    pub fn try_apply(&self, input: W::Input) -> TaskResult<W> {
        let (tx, rx) = hive::outcome_channel();
        self.send_one(input, Some(tx));
        rx.recv().expect("channel hung up unexpectedly").into()
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
        if self.max_count() == 0 {
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
    pub fn swarm<T>(&self, batch: T) -> impl Iterator<Item = TaskResult<W>>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = hive::outcome_channel();
        let num_tasks = self.send_batch(batch, Some(tx)).len();
        rx.take_results(num_tasks)
    }

    /// Sends a `batch` of inputs to the `Hive` for processing, and returns an iterator over the
    /// `Outcome`s.
    ///
    /// The `Outcome`s will be sent in the order they are completed. Use `swarm` to instead
    /// receive the `Outcome`s in the order they were submitted.
    ///
    /// This method is more efficient than `map_unordered` when the input is an
    /// `ExactSizeIterator`.
    pub fn swarm_unordered<T>(&self, batch: T) -> impl Iterator<Item = TaskResult<W>>
    where
        T: IntoIterator<Item = W::Input>,
        T::IntoIter: ExactSizeIterator,
    {
        let (tx, rx) = hive::outcome_channel();
        let num_tasks = self.send_batch(batch, Some(tx)).len();
        rx.into_iter().take(num_tasks).map(Outcome::into)
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
    ) -> impl Iterator<Item = TaskResult<W>> {
        let (tx, rx) = hive::outcome_channel();
        let num_tasks = inputs
            .into_iter()
            .map(|task| self.apply_send(task, tx.clone()))
            .count();
        rx.take_results(num_tasks)
    }

    /// Iterates over `inputs`, sends each one to the `Hive` for processing, and returns an
    /// iterator over the `Outcome`s in order they become available.
    ///
    /// `swarm_unordered` should be preferred when `inputs` is an `ExactSizeIterator`.
    pub fn map_unordered(
        &self,
        inputs: impl IntoIterator<Item = W::Input>,
    ) -> impl Iterator<Item = TaskResult<W>> {
        let (tx, rx) = hive::outcome_channel();
        let num_tasks = inputs
            .into_iter()
            .map(|task| self.apply_send(task, tx.clone()))
            .count();
        rx.into_iter().take(num_tasks).map(Outcome::into)
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
    /// This function returns a `Vec` of the outputs in the order they were submitted, and the
    /// final state value.
    pub fn scan<St, T, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        f: F,
    ) -> HiveResult<(Vec<W::Output>, St), W>
    where
        F: FnMut(&mut St, T) -> W::Input,
    {
        let (tx, rx) = hive::outcome_channel();
        let (indices, fold_value) = self.scan_send(items, tx, init, f);
        let mut outcomes: Vec<_> = rx.into_iter().take(indices.len()).collect();
        outcomes.sort();
        let task_outputs = outcomes
            .into_iter()
            .map(Outcome::into)
            .collect::<HiveResult<Vec<_>, _>>()?;
        Ok((task_outputs, fold_value))
    }

    /// Iterates over `items` and calls `f` with a mutable reference to a state value (initialized
    /// to `init`) and each item. `F` returns an input that is sent to the `Hive` for processing,
    /// or an error. This function returns a `Vec` of the outputs in the order they were
    /// submitted and the final state value, or an error if any calls to `f` resulted in an error.
    pub fn try_scan<St, T, F>(
        &self,
        items: impl IntoIterator<Item = T>,
        init: St,
        f: F,
    ) -> HiveResult<(Vec<W::Output>, St), W>
    where
        F: FnMut(&mut St, T) -> Result<W::Input, W::Error>,
    {
        let (tx, rx) = hive::outcome_channel();
        let (indices, fold_value) = self
            .try_scan_send(items, tx, init, f)
            .map_err(HiveError::Failed)?;
        let mut outcomes: Vec<_> = rx.into_iter().take(indices.len()).collect();
        outcomes.sort();
        let task_outputs = outcomes
            .into_iter()
            .map(Outcome::into)
            .collect::<HiveResult<Vec<_>, _>>()?;
        Ok((task_outputs, fold_value))
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
    pub fn try_scan_send<St, T, F>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        tx: OutcomeSender<W>,
        init: St,
        mut f: F,
    ) -> Result<(Vec<usize>, St), W::Error>
    where
        F: FnMut(&mut St, T) -> Result<W::Input, W::Error>,
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

    /// Returns the number of tasks currently queued for processing.
    pub fn queued_count(&self) -> usize {
        self.shared.queued_count()
    }

    /// Returns the number of tasks currently being processed.
    pub fn active_count(&self) -> usize {
        self.shared.active_count()
    }

    /// Returns the maximum number of tasks that can be processed concurrently.
    pub fn max_count(&self) -> usize {
        self.shared.max_count()
    }

    /// Returns the number of times one of this `Hive`'s worker threads has panicked.
    pub fn panic_count(&self) -> usize {
        self.shared.panic_count()
    }

    /// Sets the maximum number of worker threads to `0` and sets the cancelled flag, which
    /// enables worker threads to terminate early. After calling this method, any new tasks
    /// will not be processed.
    pub fn cancel(&self) {
        self.set_num_threads(0);
        self.shared.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.shared.is_cancelled()
    }

    /// Returns any stored `Outcome`s.
    pub fn take_stored(&self) -> HashMap<usize, Outcome<W>> {
        self.shared.take_outcomes()
    }

    /// Sets the maximum number of tasks that can be processed concurrently.
    pub fn set_num_threads(&self, num_threads: usize) {
        if num_threads > 0 {
            assert!(!self.is_cancelled());
        }
        let prev_num_threads = self.shared.replace_num_threads(num_threads);
        if num_threads > prev_num_threads {
            // Spawn new threads
            spawn::brood(num_threads - prev_num_threads, &self.shared);
        }
    }

    /// Sets the number of threads for processing tasks to the number of available CPU cores.
    pub fn use_all_cores(&self) {
        self.set_num_threads(num_cpus::get());
    }

    /// Sets the maximum number of tasks that can be processed concurrently. If the new value is
    /// greater than the current number of threads in the `Hive`, new threads are spun up. The
    /// provided `affinity` set specifies the CPU core indices that will be used to try to pin each
    /// thread to a specific core.
    #[cfg(feature = "affinity")]
    pub fn set_num_threads_with_affinity(&self, num_threads: usize, affinity: &Cores) {
        if num_threads > 0 {
            assert!(!self.is_cancelled());
        }
        let prev_num_threads = self.shared.replace_num_threads(num_threads);
        if num_threads > prev_num_threads {
            // Spawn new threads
            spawn::brood_with_affinity(num_threads - prev_num_threads, &self.shared, affinity);
        }
    }

    /// Sets the number of threads for processing tasks to the number of available CPU cores. If
    /// there are currently fewer threads in the `Hive` than available cores, then new threads are
    /// spun up and an attempt is made to pin each new thread to a specific core.
    #[cfg(feature = "affinity")]
    pub fn use_all_cores_with_affinity(&self) {
        let num_cpus = num_cpus::get();
        let mut affinity = (0..num_cpus).collect::<Cores>();
        self.shared.diff_core_affinity(&mut affinity);
        self.set_num_threads_with_affinity(num_cpus, &affinity);
    }

    /// Blocks this thread until all tasks finish.
    pub fn join(&self) {
        self.shared.wait_on_empty();
    }

    /// Consumes this `Hive` and returns a `Husk` containing the remnants of this `Hive`, including
    /// any stored task outcomes, and all the data necessary to create a new `Hive`.
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

impl<W: Worker, Q: Queen<Kind = W>> Stored<W> for Hive<W, Q> {
    fn outcomes(&self) -> impl Deref<Target = HashMap<usize, Outcome<W>>> {
        self.shared.outcomes()
    }

    fn outcomes_mut(&mut self) -> impl DerefMut<Target = HashMap<usize, Outcome<W>>> {
        self.shared.outcomes()
    }
}
