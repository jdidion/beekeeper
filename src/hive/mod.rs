//! A worker pool implementation.
//!
//! A [`Hive<W, Q>`](crate::hive::Hive) has a pool of worker threads that it uses to execute tasks.
//!
//! The `Hive` has a [`Queen`] of type `Q`, which it uses to create a [`Worker`] of type `W` for
//! each thread it starts in the pool.
//!
//! Each task is submitted to the `Hive` as an input of type `W::Input`, and, optionally, a
//! channel where the [`Outcome`] of processing the task will be sent upon completion. To these,
//! the `Hive` adds additional context to create the task. It then adds the task to an internal
//! queue that is shared with all the worker threads.
//!
//! Each worker thread executes a loop in which it receives a task, evaluates it with its `Worker`,
//! and either sends the `Outcome` to the task's outcome channel if one was provided, or stores the
//! `Outcome` in the `Hive` for later retrieval.
//!
//! When a `Hive` is no longer needed, it may be simply dropped, which will cause the worker
//! threads to terminate automatically. Alternatively, a `Hive` may be turned into a [`Husk`],
//! which will preserve its internal state and enable later retrieval of stored outcomes.
//!
//! # Creating a `Hive`
//!
//! The typical way to create a `Hive` is using a [`Builder`]. Use
//! [`Builder::new()`](crate::hive::builder::Builder::new) to create an empty (completely
//! unconfigured) `Builder`, or [`Builder::default()`](crate::hive::builder::Builder::default) to
//! create a `Builder` configured with the global default values (see below).
//!
//! See the [`Builder`] documentation for more details on the options that may be configured, and
//! the `build*` methods available to create the `Hive`.
//!
//! Building a `Hive` consumes the `Builder`. To create multiple identical `Hive`s, you can `clone`
//! the `Builder`.
//!
//! ```
//! use beekeeper::hive::prelude::*;
//! # type MyWorker1 = beekeeper::bee::stock::EchoWorker<usize>;
//! # type MyWorker2 = beekeeper::bee::stock::EchoWorker<u32>;
//!
//! let builder1 = channel_builder(true);
//! let builder2 = builder1.clone();
//!
//! let hive1 = builder1.with_worker_default::<MyWorker1>().build();
//! let hive2 = builder2.with_worker_default::<MyWorker2>().build();
//! ```
//!
//! If you want a `Hive` with the global defaults for a `Worker` type that implements `Default`,
//! you can call [`DefaultHive::<W>::default`](crate::hive::Hive::default) rather than use a
//! `Builder`.
//!
//! ```
//! # use beekeeper::hive::DefaultHive;
//! # type MyWorker = beekeeper::bee::stock::EchoWorker<usize>;
//! let hive = DefaultHive::<MyWorker>::default();
//! ```
//!
//! ## Thread affinity (requires `feature = "affinity"`)
//!
//! Threads are a feature of modern operating systems that enable more processes to execute than
//! there are available CPU cores. This requires the OS to "schedule" each process by moving its
//! state into a CPU cache when it needs to run, and out of the cache when it is "interrupted" by
//! another process. This overhead can be significant for CPU-bound processes.
//!
//! CPU "affinity" is a feature supported by most modern operating systems, in which a thread can
//! be "pinned" to a specific CPU core such that its state is retained in that core's cache (even)
//! if that thread is paused. For highly active threads, this has the advantage of reducing
//! scheduling overhead. However, for threads that are only periodically active, this can lead to
//! under-utlization of CPU cores as well as degredation of un-pinned processes.
//!
//! With the `affinity` feature enabled, several additional methods become available in `Builder`
//! and `Hive` that enable pinning worker threads to specific CPU cores. You specify a CPU core in
//! terms of its index, which is a value in the range `0..n`, where `n` is the number of available
//! CPU cores. Internally, a mapping is maintained between the index and the OS-specific core ID.
//!
//! The [`Builder::core_affinity`](crate::hive::builder::Builder::core_affinity) method accepts a
//! range of core indices that are reserved as *available* for the `Hive` to use for thread-pinning,
//! but they may or may not actually be used (depending on the number of worker threads and core
//! availability). The number of available cores can be smaller or larger than the number of
//! threads. Any thread that is spawned for which there is no corresponding core index is simply
//! started with no core affinity.
//!
//! ```
//! use beekeeper::hive::prelude::*;
//! # type MyWorker = beekeeper::bee::stock::EchoWorker<usize>;
//!
//! let hive = channel_builder(false)
//!     .num_threads(4)
//!     // 16 cores will be available for pinning but only 4 will be used initially
//!     .core_affinity(0..16)
//!     .with_worker_default::<MyWorker>()
//!     .build();
//!
//! // increase the number of threads by 12 - the new threads will use the additiona
//! // 12 available cores for pinning
//! hive.grow(12);
//!
//! // increase the number of threads and also provide additional cores for pinning
//! // this requires the `affinity` feature
//! // hive.grow_with_affinity(4, 16..20);
//! ```
//!
//! As an application developer depending on `beekeeper`, you must ensure you assign each core
//! index to at most a single thread (i.e., don't reuse indices for different co-existing `Hive`s).
//! It is strongly suggested that you require the user of your application to specify the CPU
//! indices available to your application for thread pinning; the user is required to ensure that
//! they don't re-use CPU indices with different co-existing applications that support thread
//! pinning.
//!
//! ## Retrying tasks (requires `feature = "retry"`)
//!
//! Some types of tasks (e.g., those requirng network I/O operations) may fail transiently but
//! could be successful if retried at a later time. Such retry behavior is supported by the `retry`
//! feature and only requires a) configuring the `Builder` by setting
//! [`max_retries`](crate::hive::Builder::max_retries) and (optionally)
//! [`retry_factor`](crate::hive::Builder::retry_factor); and b) implementing the `Worker`
//! to return [`ApplyError::Retryable`](crate::bee::ApplyError::Retryable) for transient failures.
//!
//! When a `Retryable` error occurs, the following steps happen:
//! * The `attempt` number in the task's [`Context`] is incremented.
//! * If the `attempt` number exceeds `max_retries`, the error is converted to
//!   `Outcome::MaxRetriesAttempted` and sent/stored.
//! * Otherwise, the task is added to the `Hive`'s retry queue.
//!     * If a `retry_factor` is configured, then the task is queued with a delay of at least
//!       `2^(attempt - 1) * retry_factor`.
//!     * If a `retry_factor` is not configured, then the task is queued with no delay.
//! * When a worker thread becomes available, it first checks the retry queue to see if there is
//!   a task to retry before taking a new task from the input channel.
//!
//! Note that `ApplyError::Retryable` is not feature-gated - a `Worker` can be implemented to be
//! retry-aware but used with a `Hive` for which retry is not enabled, or in an application where
//! the `retry` feature is not enabled. In such cases, `Retryable` errors are automatically
//! converted to `Fatal` errors by the worker thread.
//!
//! ## Batching tasks (requires `feature = "local-batch"`)
//!
//! The performance of a `Hive` can degrade as the number of worker threads grows and/or the
//! average duration of a task shrinks, due to increased contention between worker threads when
//! receiving tasks from the shared input channel. To improve performance, workers can take more
//! than one task each time they access the input channel, and store the extra tasks in a local
//! queue. This behavior is activated by enabling the `local-batch` feature.
//!
//! With the `local-batch` feature enabled, `Builder` gains the
//! [`batch_limit`](crate::hive::Builder::batch_limit) method for configuring size of worker threads'
//! local queues, and `Hive` gains the [`set_worker_batch_limit`](crate::hive::Hive::set_batch_limit)
//! method for changing the batch size of an existing `Hive`.
//!
//! ## Global defaults
//!
//! The [`hive`](crate::hive) module has functions for setting the global default values for some
//! of the `Builder` parameters. These default values are used to pre-configure the `Builder` when
//! using `Builder::default()`.
//!
//! The available global defaults are:
//!
//! * `num_threads`
//!     * [`set_num_threads_default`]: sets the default to a specific value
//!     * [`set_num_threads_default_all`]: sets the default to all available CPU cores
//! * [`batch_limit`](crate::hive::set_BATCH_LIMIT_default) (requires `feature = "local-batch"`)
//! * [`max_retries`](crate::hive::set_max_retries_default] (requires `feature = "retry"`)
//! * [`retry_factor`](crate::hive::set_retry_factor_default] (requires `feature = "retry"`)
//!
//! The global defaults can be reset their original values using the [`reset_defaults`] function.
//!
//! # Cloning a `Hive`
//!
//! A `Hive` is simply a wrapper around a data structure that is shared between the `Hive`, its
//! worker threads, and any clones that have been made of the `Hive`. In other works, cloning a
//! `Hive` simply creates another reference to the same shared data (similar to cloning an [`Arc`]).
//! The worker threads and the shared data structure are dropped automatically when the last `Hive`
//! referring to them is dropped (see "Disposing of a Hive" below).
//!
//! # Submitting tasks
//!
//! `Hive` has four groups of methods for submitting tasks:
//! - `apply`: submits a single task to the hive.
//! - `map`: submits an arbitrary-sized batch (an `Iterator`) of tasks to the hive.
//! - `swarm`: submits a batch of tasks to the hive, where the size of the batch is known (i.e.,
//!   it implements `IntoIterator<IntoIter = ExactSizeIterator>`).
//! - `scan`: like map/swarm, but instead of a batch of inputs (of type `W::Input`), it takes a
//!   batch of items (of type `T`), a state value (`St`), and a callable
//!   (`FnMut(&mut St, T) -> W::Input`) that is called on each item and returns an input that is
//!   sent to the hive for processing. The state value may be updated by the callable, and the
//!   final value is returned.
//!
//! Each group of functions has multiple variants:
//! * The methods that end with `_send` all take a channel sender as a second argument and will
//!   deliver results to that channel as they become available. See the note below on proper use of
//!   channels.
//! * The methods that end with `_store` are all non-blocking functions that return the task IDs
//!   associated with the submitted tasks and will store the task results in the hive. The outcomes
//!   can be retrieved from the hive later by their IDs, e.g., using `remove_success`.
//! * For executing single tasks, there is `apply`, which submits the tasks and blocks waiting for
//!   the result.
//! * For executing batches of tasks, there are `map`/`swarm`/`scan`, which return an iterator that
//!   yields outcomes in the same order they were submitted. There are `_unordered` versions of the
//!   same functions that yield results as they become available.
//! * There are also `try_scan` variants of the `scan*` methods, which take a callable that returns
//!   `Result<W::Input, E>`.
//!
//! After submitting tasks, you can call [`Hive::join`](crate::hive::Hive::join) to block the
//! calling thread until all tasks have completed. Note that this may be required when using
//! non-blocking methods (such as those that end with `_store`).
//!
//! ## Outcome channels
//!
//! By default, [`std::sync::mpsc`] channels are used for sending task `Outcome`s.
//! However, `beekeeper` supports several alternative channel implementations via feature flags:
//! - [`crossbeam`](https://docs.rs/crossbeam/latest/crossbeam/channel/index.html)
//! - [`flume`](https://docs.rs/flume/latest/flume/index.html)
//! - [`loole`](https://docs.rs/loole/latest/loole/index.html)
//!
//! Note that only a single outcome channel implementation may be enabled at a time.
//!
//! You can create an instance of the enabled outcome channel type using the [`outcome_channel`]
//! function.
//!
//! `Hive` as several methods (with the `_send` suffix) for submitting tasks whose outcomes will be
//! delivered to a user-specified channel. Note that, for these methods, the `tx` parameter is of
//! type `Borrow<Sender<Outcome<W>>`, which allows you to pass in either a value or a reference.
//! Passing a value causes the `Sender` to be dropped after the call; passing a reference allows
//! you to use the same `Sender` for multiple `_send` calls, but you need to explicitly drop the
//! sender (e.g., `drop(tx)`), pass it by value to the last `_send` call, or be careful about how
//! you obtain outcomes from the `Receiver`. Methods such as `recv` and `iter` will block until the
//! `Sender` is dropped. Since `Receiver` implements `Iterator`, you can use the methods of
//! [`OutcomeIteratorExt`] to iterate over the outcomes for specific task IDs.
//!
//! ```rust,ignore
//! use beekeeper::hive::prelude::*;
//! let (tx, rx) = outcome_channel();
//! let hive = ...
//! let task_ids = hive.map_send(0..10, tx);
//! rx.select_unordered_outputs(task_ids).for_each(|output| ...);
//! ```
//!
//! You should *not* pass clones of the `Sender` to `_send` methods as this results in slightly
//! worse performance and still has the requirement that you manually drop the original `Sender`
//! value.
//!
//! # Retrieving outcomes
//!
//! Each task that is successfully submitted to a `Hive` will have a corresponding `Outcome`.
//! [`Outcome`] is similar to `Result`, except that the error variants are enumerated:
//! * [`Failure`](Outcome::Failure): the task failed with an error of type `W::Error`. If possible,
//!   the input value is also provided.
//! * [`Panic`](Outcome::Panic): the `Worker` panicked while processing the task. The panic
//!   [`payload`](crate::panic::Panic) is provided, and the unwinding can be
//!   [`resume`d](crate::panic::Panic::resume) to panic the handling thread. The input is also
//!   provided if possible.
//! * [`Unprocessed`](Outcome::Unprocessed): the input was not processed by the `Hive`, typically
//!   because the `Hive` was dropped first. The input value is always provided.
//! * [`Missing`](Outcome::Missing): an `Outcome` was requested by ID, but no `Outcome` with that
//!   ID was found. This variant is only used when a list of outcomes is requested, such as when
//!   using one of the `select_*` methods on an `Outcome` iterator (see below).
//!
//! An `Outcome` can be converted into a `Result` (using `into()`) or
//! [`unwrap`](crate::hive::Outcome::unwrap)ped into an output value of type `W::Output`.
//!
//! ## Outcome iterators
//!
//! The [`map`](crate::hive::Hive::map) and [`swarm`](crate::hive::Hive::swarm) methods return an
//! ordered iterator over the `Outcome`s, while [`map_unordered`](crate::hive::Hive::map_unordered)
//! and [`swarm_unordered`](crate::hive::Hive::swarm_unordered) return unordered iterators. These
//! methods create a dedicated outcome channel to use for each batch of tasks, and thus expect the
//! channel receiver to receive exactly the outcomes with the task IDs of the submitted tasks. If,
//! somehow, an unexpected `Outcome` is received, it is silently dropped. If any expected outcomes
//! have not been received after the channel sender has disconnected, then those task IDs are'
//! yielded as `Outcome::Missing` results.
//!
//! When the [`OutcomeIteratorExt`] trait is in scope, then additional methods become available on
//! any iterator over `Outcome`:
//! * The methods with `_result` suffix convert the outcome iterator into an iterator over
//!   `Result<W::Output, W::Error>`. Note that attempting to convert `Outcome`s other than
//!   `Success`, `Failure`, and `MaxRetriesAttempted` causes a panic.
//! * The methods with `_output` suffix convert the outcome iterator into an iterator over
//!   `W::Output`. Note that attempting to convert a non-`Success` outcome causes a panic.
//!
//! ## Outcome channels
//!
//! Using one of the `*_send` methods with a channel enables the `Hive` to send you `Outcome`s
//! asynchronously as they become available. This means that you will likely receive the outcomes
//! out of order (i.e., not in the same order as the provided inputs).
//!
//! All channel `Receiver` types have a `recv()` method that blocks waiting for the next value to
//! be received, or until the sending end of the channel is dropped (in which case an error is
//! returned).
//!
//! Alternatively, a `Receiver` type can be converted into a blocking iterator using its `iter()`
//! or `into_iter()` method. The iterator yields `Outcome` values until the sender is dropped. With
//! `OutcomeIteratorExt` in scope, any of the methods mentioned in the previous section may be used
//! to convert the outcomes. Notably, the `select_*` methods take a collection of task IDs and
//! return an iterator that yields items (`Outcome`s, `Result`s, or outputs) that match those
//! task IDs.
//!
//! ```
//! use beekeeper::hive::{DefaultHive, OutcomeIteratorExt, outcome_channel};
//! # type MyWorker = beekeeper::bee::stock::EchoWorker<usize>;
//!
//! let hive = DefaultHive::<MyWorker>::default();
//! let (tx, rx) = outcome_channel::<MyWorker>();
//! let batch1 = hive.swarm_send(0..10, &tx);
//! let batch2 = hive.swarm_send(10..20, tx);
//! let outputs: Vec<_> = rx.into_iter()
//!     .select_ordered_outputs(batch1.into_iter().chain(batch2.into_iter()))
//!     .collect();
//! ```
//!
//! ## Outcome stores
//!
//! The `scan_*` methods return an [`OutcomeBatch`], which is a wrapper around a
//! `HashMap<TaskId, Outcome>` that provides methods to access the desired outcomes.
//!
//! The [`OutcomeStore`] trait is implemented by `OutcomeBatch`, as well as `Hive` and `Husk`
//! (see below), which provides a common interface for accessing stored `Outcome`s.
//!
//! ```
//! use beekeeper::hive::{DefaultHive, OutcomeStore};
//! # type MyWorker = beekeeper::bee::stock::EchoWorker<usize>;
//!
//! let hive = DefaultHive::<MyWorker>::default();
//! let (outcomes, sum) = hive.scan(0..10, 0, |sum, i| {
//!     *sum += i;
//!     i * 2
//! });
//! assert_eq!(sum, 45);
//! assert_eq!(outcomes.num_successes(), 10);
//! let mut outputs = outcomes
//!     .iter_successes()
//!     .map(|(_, output)| *output)
//!     .collect::<Vec<_>>();
//! outputs.sort();
//! assert_eq!(outputs, (0..10).map(|i| i * 2).collect::<Vec<_>>());
//! ```
//!
//! # Suspend and resume
//!
//! Processing of tasks by a `Hive` can be temporarily suspended by calling the
//! [`suspend`](crate::hive::Hive::suspend) method. This prevents worker threads from starting
//! any new tasks, and it also notifies worker threads that they may (but are not required to)
//! cancel processing of their current tasks. Cancelled tasks are sent/stored as `Unprocessed`
//! outcomes.
//!
//! Processing can be resumed by calling the [`resume`](crate::hive::Hive::resume) method.
//! Alternatively, the [`resume_send`](crate::hive::Hive::resume_send) or
//! [`resume_store`](crate::hive::Hive::resume_store) method can be used to both resume and
//! submit any unprocessed tasks stored in the `Hive` for (re)processing.
//!
//! ## Hive poisoning
//!
//! The internal data structure shared between a `Hive`, its clones, and its worker threads is
//! considered thread-safe. However, there is no formal proof that it is incorruptible. A `Hive`
//! attempts to detect if it has become corrupted and, if so, sets the `poisoned` flag on the
//! shared data. A poisoned `Hive` will not accept or process any new tasks, and all worker threads
//! will terminate after finishing their current tasks. If a task is submitted to a poisoned `Hive`,
//! it will immediately be converted to an `Unprocessed` outcome and sent/stored. The only thing
//! that can be done with a poisoned `Hive` is to access its stored `Outcome`s or convert it to a
//! `Husk` (see below).
//!
//! # Disposing of a `Hive`
//!
//! When a `Hive` is no longer needed, it may simply be dropped. If there are extant clones of the
//! dropped `Hive`, then nothing further happens. When the last instance of a `Hive` referring to
//! its shared data is dropped, then the following steps happen:
//! * The `Hive` is poisoned to prevent any new tasks from being submitted or queued tasks from
//!   being processed.
//! * All of the `Hive`s queued tasks are coverted to `Unprocessed` outcomes and either sent to
//!   their outcome channel or stored in the `Hive`.
//! * If the `Hive` was in a suspended state, it is resumed. This is necessary to unblock the
//!   worker threads and allow them to terminate.
//! * Any worker thread that is actively processing a task will finish and send/store the outcome
//!   before terminating.
//! * The shared data structure is dropped.
//!
//! You may instead manually dispose of a `Hive` by converting it into a [`Husk`] using the
//! [`try_into_husk`](crate::hive::Hive::try_into_husk) method. A `Husk` retains the core
//! configuration of a `Hive`. This includes the queen, which means if your `Queen` type is
//! stateful, you can access its final state.
//!
//! The `Husk` also retains any stored `Outcome`s from the `Hive`, which can be accessed using the
//! [`OutcomeStore`] API.
//!
//! The `Husk` can be used to create a new `Builder`
//! ([`Husk::as_builder`](crate::hive::husk::Husk::as_builder)) or a new `Hive`
//! ([`Husk::into_hive`](crate::hive::husk::Husk::into_hive)).
mod builder;
mod context;
#[cfg(feature = "affinity")]
pub mod cores;
#[allow(clippy::module_inception)]
mod hive;
mod husk;
mod inner;
mod outcome;
mod sentinel;
mod util;
mod weighted;

pub use self::builder::{BeeBuilder, ChannelBuilder, FullBuilder, OpenBuilder, TaskQueuesBuilder};
pub use self::builder::{
    channel as channel_builder, open as open_builder, workstealing as workstealing_builder,
};
pub use self::hive::{DefaultHive, Hive, Poisoned};
pub use self::husk::Husk;
pub use self::inner::{
    Builder, ChannelTaskQueues, TaskInput, WorkstealingTaskQueues, set_config::*,
};
pub use self::outcome::{Outcome, OutcomeBatch, OutcomeIteratorExt, OutcomeStore};
pub use self::weighted::Weighted;

use self::context::HiveLocalContext;
use self::inner::{Config, Shared, Task, TaskQueues, WorkerQueues};
use self::outcome::{DerefOutcomes, OutcomeQueue, OwnedOutcomes};
use self::sentinel::Sentinel;
use crate::bee::Worker;
use crate::channel::{Receiver, Sender, channel};
use std::io::Error as SpawnError;

/// Sender type for channel used to send task outcomes.
pub type OutcomeSender<W> = Sender<Outcome<W>>;
/// Receiver type for channel used to receive task outcomes.
pub type OutcomeReceiver<W> = Receiver<Outcome<W>>;

/// Creates a channel (`Sender`, `Receiver`) pair for sending task outcomes from the `Hive` to the
/// task submitter.
#[inline]
pub fn outcome_channel<W: Worker>() -> (OutcomeSender<W>, OutcomeReceiver<W>) {
    channel()
}

pub mod prelude {
    pub use super::{
        Builder, Hive, Husk, Outcome, OutcomeBatch, OutcomeIteratorExt, OutcomeStore, Poisoned,
        TaskQueuesBuilder, channel_builder, open_builder, outcome_channel, workstealing_builder,
    };
}

#[cfg(test)]
mod tests {
    use super::inner::TaskQueues;
    use super::{
        Builder, ChannelTaskQueues, Hive, Outcome, OutcomeIteratorExt, OutcomeStore,
        TaskQueuesBuilder, WorkstealingTaskQueues, channel_builder, workstealing_builder,
    };
    use crate::barrier::IndexedBarrier;
    use crate::bee::stock::{Caller, OnceCaller, RefCaller, Thunk, ThunkWorker};
    use crate::bee::{
        ApplyError, ApplyRefError, Context, DefaultQueen, QueenMut, RefWorker, RefWorkerResult,
        TaskId, Worker, WorkerResult,
    };
    use crate::channel::{Message, ReceiverExt};
    use crate::hive::outcome::DerefOutcomes;
    use rstest::*;
    use std::fmt::Debug;
    use std::io::{self, BufRead, BufReader, Write};
    use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::thread;
    use std::time::Duration;

    const TEST_TASKS: usize = 4;
    const ONE_SEC: Duration = Duration::from_secs(1);
    const SHORT_TASK: Duration = Duration::from_secs(2);
    const LONG_TASK: Duration = Duration::from_secs(5);

    type TWrk<I> = ThunkWorker<I>;

    /// Convenience function that returns a `Hive` configured with the global defaults, and the
    /// specified number of workers that execute `Thunk<T>`s, i.e. closures that return `T`.
    pub fn thunk_hive<I, T, B>(num_threads: usize, builder: B) -> Hive<DefaultQueen<TWrk<I>>, T>
    where
        I: Send + Sync + Debug + 'static,
        T: TaskQueues<TWrk<I>>,
        B: TaskQueuesBuilder<TaskQueues<TWrk<I>> = T>,
    {
        builder
            .num_threads(num_threads)
            .with_queen_default()
            .build()
    }

    pub fn void_thunk_hive<T, B>(num_threads: usize, builder: B) -> Hive<DefaultQueen<TWrk<()>>, T>
    where
        T: TaskQueues<TWrk<()>>,
        B: TaskQueuesBuilder<TaskQueues<TWrk<()>> = T>,
    {
        thunk_hive(num_threads, builder)
    }

    #[rstest]
    fn test_works<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive(TEST_TASKS, builder_factory(true));
        let (tx, rx) = mpsc::channel();
        assert_eq!(hive.max_workers(), TEST_TASKS);
        assert_eq!(hive.alive_workers(), TEST_TASKS);
        assert!(!hive.has_dead_workers());
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            hive.apply_store(Thunk::of(move || {
                tx.send(1).unwrap();
            }));
        }
        assert_eq!(rx.iter().take(TEST_TASKS).sum::<usize>(), TEST_TASKS);
    }

    #[rstest]
    fn test_grow_from_zero<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive::<u8, _, _>(0, builder_factory(true));
        // check that with 0 threads no tasks are scheduled
        let (tx, rx) = super::outcome_channel();
        let _ = hive.apply_send(Thunk::of(|| 0), &tx);
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks().0, 1);
        assert!(matches!(rx.try_recv_msg(), Message::ChannelEmpty));
        hive.grow(1).expect("error spawning threads");
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks().0, 0);
        assert!(matches!(
            rx.try_recv_msg(),
            Message::Received(Outcome::Success { value: 0, .. })
        ));
    }

    #[rstest]
    fn test_grow_from_nonzero<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = void_thunk_hive(TEST_TASKS, builder_factory(false));
        // queue some long-running tasks
        for _ in 0..TEST_TASKS {
            hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks().1, TEST_TASKS as u64);
        // increase the number of threads
        let new_threads = 4;
        let total_threads = new_threads + TEST_TASKS;
        hive.grow(new_threads).expect("error spawning threads");
        // queue some more long-running tasks
        for _ in 0..new_threads {
            hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks().1, total_threads as u64);
        let husk = hive.try_into_husk(false).unwrap();
        assert_eq!(husk.iter_successes().count(), total_threads);
    }

    #[rstest]
    fn test_suspend<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = void_thunk_hive(TEST_TASKS, builder_factory(false));
        // queue some long-running tasks
        let total_tasks = 2 * TEST_TASKS;
        for _ in 0..total_tasks {
            hive.apply_store(Thunk::of(|| thread::sleep(SHORT_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks(), (TEST_TASKS as u64, TEST_TASKS as u64));
        hive.suspend();
        // active tasks should finish but no more tasks should be started
        thread::sleep(SHORT_TASK);
        assert_eq!(hive.num_tasks(), (TEST_TASKS as u64, 0));
        assert_eq!(hive.num_successes(), TEST_TASKS);
        hive.resume();
        // new tasks should start
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks(), (0, TEST_TASKS as u64));
        thread::sleep(SHORT_TASK);
        // all tasks should be completed
        assert_eq!(hive.num_tasks(), (0, 0));
        assert_eq!(hive.num_successes(), total_tasks);
    }

    #[derive(Debug, Default)]
    struct MyRefWorker;

    impl RefWorker for MyRefWorker {
        type Input = u8;
        type Output = u8;
        type Error = ();

        fn apply_ref(
            &mut self,
            input: &Self::Input,
            ctx: &Context<Self::Input>,
        ) -> RefWorkerResult<Self> {
            for _ in 0..3 {
                thread::sleep(Duration::from_secs(1));
                if ctx.is_cancelled() {
                    return Err(ApplyRefError::Cancelled);
                }
            }
            Ok(*input)
        }
    }

    #[rstest]
    fn test_suspend_with_cancelled_tasks<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive: Hive<_, _> = builder_factory(false)
            .num_threads(TEST_TASKS)
            .with_worker_default::<MyRefWorker>()
            .build();
        hive.swarm_store(0..TEST_TASKS as u8);
        hive.suspend();
        // wait for tasks to be cancelled
        thread::sleep(Duration::from_secs(2));
        hive.resume_store();
        thread::sleep(Duration::from_secs(1));
        // unprocessed tasks should be requeued
        assert_eq!(hive.num_tasks().1, TEST_TASKS as u64);
        thread::sleep(Duration::from_secs(3));
        assert_eq!(hive.num_successes(), TEST_TASKS);
    }

    #[rstest]
    fn test_num_tasks_active<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = void_thunk_hive(TEST_TASKS, builder_factory(false));
        for _ in 0..2 * TEST_TASKS {
            hive.apply_store(Thunk::of(|| {
                loop {
                    thread::sleep(LONG_TASK)
                }
            }));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks().1, TEST_TASKS as u64);
        let num_threads = hive.max_workers();
        assert_eq!(num_threads, TEST_TASKS);
    }

    #[rstest]
    fn test_all_threads<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive: Hive<DefaultQueen<TWrk<()>>, _> = builder_factory(false)
            .with_queen_default()
            .with_thread_per_core()
            .build();
        let num_threads = num_cpus::get();
        for _ in 0..num_threads {
            hive.apply_store(Thunk::of(|| {
                loop {
                    thread::sleep(LONG_TASK)
                }
            }));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks().1, num_threads as u64);
        let max_workers = hive.max_workers();
        assert_eq!(num_threads, max_workers);
    }

    #[rstest]
    fn test_panic<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive(TEST_TASKS, builder_factory(true));
        let (tx, _) = super::outcome_channel();
        // Panic all the existing threads.
        for _ in 0..TEST_TASKS {
            hive.apply_send(Thunk::of(|| panic!("intentional panic")), &tx);
        }
        hive.join();
        // Ensure that none of the threads have panicked
        assert_eq!(hive.num_panics(), TEST_TASKS);
        let husk = hive.try_into_husk(false).unwrap();
        assert_eq!(husk.num_panics(), TEST_TASKS);
    }

    #[rstest]
    fn test_catch_panic<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive: Hive<_, _> = builder_factory(false)
            .with_worker(RefCaller::of(|_: &u8| -> Result<u8, String> {
                panic!("intentional panic")
            }))
            .num_threads(TEST_TASKS)
            .build();
        let (tx, rx) = super::outcome_channel();
        // Panic all the existing threads.
        for i in 0..TEST_TASKS {
            hive.apply_send(i as u8, &tx);
        }
        hive.join();
        // Ensure that none of the threads have panicked
        assert_eq!(hive.num_panics(), 0);
        // Check that all the results are Outcome::Panic
        for outcome in rx.into_iter().take(TEST_TASKS) {
            assert!(matches!(outcome, Outcome::Panic { .. }));
        }
    }

    #[rstest]
    fn test_should_not_panic_on_drop_if_subtasks_panic_after_drop<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = void_thunk_hive(TEST_TASKS, builder_factory(false));
        let waiter = Arc::new(Barrier::new(TEST_TASKS + 1));
        let waiter_count = Arc::new(AtomicUsize::new(0));

        // panic all the existing threads in a bit
        for _ in 0..TEST_TASKS {
            let waiter = waiter.clone();
            let waiter_count = waiter_count.clone();
            hive.apply_store(Thunk::of(move || {
                waiter_count.fetch_add(1, Ordering::SeqCst);
                waiter.wait();
                panic!("intentional panic");
            }));
        }

        // queued tasks will not be processed after the hive is dropped, so we need to wait to make
        // sure that all tasks have started and are waiting on the barrier
        // TODO: find a Barrier implementation with try_wait() semantics
        thread::sleep(Duration::from_secs(1));
        assert_eq!(waiter_count.load(Ordering::SeqCst), TEST_TASKS);

        drop(hive);

        // unblock the tasks and allow them to panic
        waiter.wait();
    }

    #[rstest]
    fn test_massive_task_creation<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let test_tasks = 4_200_000;

        let hive = thunk_hive(TEST_TASKS, builder_factory(true));
        let b0 = IndexedBarrier::new(TEST_TASKS);
        let b1 = IndexedBarrier::new(TEST_TASKS);

        let (tx, rx) = mpsc::channel();

        for _ in 0..test_tasks {
            let tx = tx.clone();
            let (b0, b1) = (b0.clone(), b1.clone());

            hive.apply_store(Thunk::of(move || {
                // Wait until the pool has been filled once.
                b0.wait();
                // wait so the pool can be measured
                b1.wait();
                assert!(tx.send(1).is_ok());
            }));
        }

        b0.wait();
        assert_eq!(hive.num_tasks().1, TEST_TASKS as u64);
        b1.wait();

        assert_eq!(rx.iter().take(test_tasks).sum::<usize>(), test_tasks);
        hive.join();

        let atomic_num_tasks_active = hive.num_tasks().1;
        assert!(
            atomic_num_tasks_active == 0,
            "atomic_num_tasks_active: {}",
            atomic_num_tasks_active
        );
    }

    #[rstest]
    fn test_name<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let name = "test";
        let hive: Hive<DefaultQueen<TWrk<()>>, B::TaskQueues<_>> = builder_factory(false)
            .with_queen_default()
            .thread_name(name.to_owned())
            .num_threads(2)
            .build();
        let (tx, rx) = mpsc::channel();

        // initial thread should share the name "test"
        for _ in 0..2 {
            let tx = tx.clone();
            hive.apply_store(Thunk::of(move || {
                let name = thread::current().name().unwrap().to_owned();
                tx.send(name).unwrap();
            }));
        }

        // new spawn thread should share the name "test" too.
        hive.grow(3).expect("error spawning threads");
        let tx_clone = tx.clone();
        hive.apply_store(Thunk::of(move || {
            let name = thread::current().name().unwrap().to_owned();
            tx_clone.send(name).unwrap();
        }));

        for thread_name in rx.iter().take(3) {
            assert_eq!(name, thread_name);
        }
    }

    #[rstest]
    fn test_stack_size<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let stack_size = 4_000_000;

        let hive: Hive<DefaultQueen<TWrk<usize>>, B::TaskQueues<_>> = builder_factory(false)
            .with_queen_default()
            .num_threads(1)
            .thread_stack_size(stack_size)
            .build();

        let actual_stack_size = hive
            .apply(Thunk::of(|| {
                //println!("This thread has a 4 MB stack size!");
                stacker::remaining_stack().unwrap()
            }))
            .unwrap() as f64;

        // measured value should be within 1% of actual
        assert!(actual_stack_size > (stack_size as f64 * 0.99));
        assert!(actual_stack_size < (stack_size as f64 * 1.01));
    }

    #[rstest]
    fn test_debug<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = void_thunk_hive(4, builder_factory(true));
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { shared: Shared { name: None, num_threads: 4, num_tasks_queued: 0, num_tasks_active: 0 } }"
        );

        let hive: Hive<DefaultQueen<TWrk<usize>>, B::TaskQueues<_>> = builder_factory(false)
            .with_queen_default()
            .thread_name("hello")
            .num_threads(4)
            .build();
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { shared: Shared { name: \"hello\", num_threads: 4, num_tasks_queued: 0, num_tasks_active: 0 } }"
        );

        let hive = thunk_hive(4, builder_factory(true));
        hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        thread::sleep(ONE_SEC);
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { shared: Shared { name: None, num_threads: 4, num_tasks_queued: 0, num_tasks_active: 1 } }"
        );
    }

    #[rstest]
    fn test_repeated_join<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive: Hive<DefaultQueen<TWrk<()>>, B::TaskQueues<_>> = builder_factory(false)
            .with_queen_default()
            .thread_name("repeated join test")
            .num_threads(8)
            .build();

        let test_count = Arc::new(AtomicUsize::new(0));

        for _ in 0..42 {
            let test_count = test_count.clone();
            hive.apply_store(Thunk::of(move || {
                thread::sleep(SHORT_TASK);
                test_count.fetch_add(1, Ordering::Release);
            }));
        }

        hive.join();
        assert_eq!(42, test_count.load(Ordering::Acquire));

        for _ in 0..42 {
            let test_count = test_count.clone();
            hive.apply_store(Thunk::of(move || {
                thread::sleep(SHORT_TASK);
                test_count.fetch_add(1, Ordering::Relaxed);
            }));
        }
        hive.join();
        assert_eq!(84, test_count.load(Ordering::Relaxed));
    }

    #[rstest]
    fn test_multi_join<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        // Toggle the following lines to debug the deadlock
        // fn error(_s: String) {
        //     use ::std::io::Write;
        //     let stderr = ::std::io::stderr();
        //     let mut stderr = stderr.lock();
        //     stderr
        //         .write(&_s.as_bytes())
        //         .expect("Failed to write to stderr");
        // }

        let hive0: Hive<DefaultQueen<TWrk<()>>, B::TaskQueues<_>> = builder_factory(false)
            .with_queen_default()
            .thread_name("multi join pool0")
            .num_threads(4)
            .build();
        let hive1: Hive<DefaultQueen<TWrk<()>>, B::TaskQueues<_>> = builder_factory(false)
            .with_queen_default()
            .thread_name("multi join pool1")
            .num_threads(4)
            .build();
        let (tx, rx) = crate::channel::channel();

        for i in 0..8 {
            let hive1_clone = hive1.clone();
            let hive0_clone = hive0.clone();
            let tx = tx.clone();
            hive0.apply_store(Thunk::of(move || {
                hive1_clone.apply_store(Thunk::of(move || {
                    //error(format!("p1: {} -=- {:?}\n", i, hive0_clone));
                    hive0_clone.join();
                    // ensure that the main thread has a chance to execute
                    thread::sleep(Duration::from_millis(10));
                    //error(format!("p1: send({})\n", i));
                    tx.send(i).expect("send failed from hive1_clone to main");
                }));
                //error(format!("p0: {}\n", i));
            }));
        }
        drop(tx);

        // no hive1 task should be completed yet, so the channel should be empty
        let before_any_send = rx.try_recv_msg();
        assert!(matches!(before_any_send, Message::ChannelEmpty));
        //error(format!("{:?}\n{:?}\n", hive0, hive1));
        hive0.join();
        //error(format!("pool0.join() complete =-= {:?}", hive1));
        hive1.join();
        //error("pool1.join() complete\n".into());
        assert_eq!(rx.into_iter().sum::<u32>(), (0..8).sum());
    }

    #[rstest]
    fn test_empty_hive<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        // Joining an empty hive must return imminently
        // TODO: run this in a thread and kill it after a timeout to prevent hanging the tests
        let hive = void_thunk_hive(4, builder_factory(true));
        hive.join();
    }

    #[rstest]
    fn test_no_fun_or_joy<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        // What happens when you keep adding tasks after a join

        fn sleepy_function() {
            thread::sleep(LONG_TASK);
        }

        let hive: Hive<DefaultQueen<TWrk<()>>, B::TaskQueues<_>> = builder_factory(false)
            .with_queen_default()
            .thread_name("no fun or joy")
            .num_threads(8)
            .build();

        hive.apply_store(Thunk::of(sleepy_function));

        let p_t = hive.clone();
        thread::spawn(move || {
            (0..23)
                .inspect(|_| {
                    p_t.apply_store(Thunk::of(sleepy_function));
                })
                .count();
        });

        hive.join();
    }

    #[rstest]
    fn test_map<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive::<u8, _, _>(2, builder_factory(false));
        let outputs: Vec<_> = hive
            .map((0..10u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((10 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>())
    }

    #[rstest]
    fn test_map_unordered<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive::<u8, _, _>(8, builder_factory(false));
        let mut outputs: Vec<_> = hive
            .map_unordered((0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        outputs.sort();
        assert_eq!(outputs, (0..8).collect::<Vec<_>>())
    }

    #[rstest]
    fn test_map_send<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive::<u8, _, _>(8, builder_factory(false));
        let (tx, rx) = super::outcome_channel();
        let mut task_ids = hive.map_send(
            (0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }),
            tx,
        );
        let (mut outcome_task_ids, mut values): (Vec<TaskId>, Vec<u8>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, task_id } => (task_id, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
        values.sort();
        assert_eq!(values, (0..8).collect::<Vec<_>>());
    }

    #[rstest]
    fn test_map_store<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let mut hive = thunk_hive::<u8, _, _>(8, builder_factory(false));
        let mut task_ids = hive.map_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive.join();
        for i in task_ids.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_task_ids, values): (Vec<TaskId>, Vec<u8>) = task_ids
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .collect();
        assert_eq!(values, (0..8).collect::<Vec<_>>());
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
    }

    #[rstest]
    fn test_swarm<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive::<u8, _, _>(2, builder_factory(false));
        let outputs: Vec<_> = hive
            .swarm((0..10u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((10 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>())
    }

    #[rstest]
    fn test_swarm_unordered<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive::<u8, _, _>(8, builder_factory(false));
        let mut outputs: Vec<_> = hive
            .swarm_unordered((0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        outputs.sort();
        assert_eq!(outputs, (0..8).collect::<Vec<_>>())
    }

    #[rstest]
    fn test_swarm_send<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = thunk_hive::<u8, _, _>(8, builder_factory(false));
        #[cfg(feature = "local-batch")]
        assert_eq!(hive.worker_batch_limit(), 0);
        let (tx, rx) = super::outcome_channel();
        let mut task_ids = hive.swarm_send(
            (0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 200));
                    i
                })
            }),
            tx,
        );
        let (mut outcome_task_ids, values): (Vec<TaskId>, Vec<u8>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, task_id } => (task_id, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        assert_eq!(values, (0..8).rev().collect::<Vec<_>>());
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
    }

    #[rstest]
    fn test_swarm_store<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let mut hive = thunk_hive::<u8, _, _>(8, builder_factory(false));
        let mut task_ids = hive.swarm_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive.join();
        for i in task_ids.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_task_ids, values): (Vec<TaskId>, Vec<u8>) = task_ids
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .collect();
        assert_eq!(values, (0..8).collect::<Vec<_>>());
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
    }

    #[rstest]
    fn test_scan<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .with_worker(Caller::of(|i: usize| i * i))
            .num_threads(4)
            .build();
        let (outputs, state) = hive.scan(0..10usize, 0, |acc, i| {
            *acc += i;
            *acc
        });
        let mut outputs = outputs.unwrap();
        outputs.sort();
        assert_eq!(outputs.len(), 10);
        assert_eq!(state, 45);
        assert_eq!(
            outputs,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
    }

    #[rstest]
    fn test_scan_send<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .with_worker(Caller::of(|i: i32| i * i))
            .num_threads(4)
            .build();
        let (tx, rx) = super::outcome_channel();
        let (mut task_ids, state) = hive.scan_send(0..10, tx, 0, |acc, i| {
            *acc += i;
            *acc
        });
        assert_eq!(task_ids.len(), 10);
        assert_eq!(state, 45);
        let (mut outcome_task_ids, mut values): (Vec<TaskId>, Vec<i32>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, task_id } => (task_id, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        values.sort();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
    }

    #[rstest]
    fn test_try_scan_send<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .with_worker(Caller::of(|i: i32| i * i))
            .num_threads(4)
            .build();
        let (tx, rx) = super::outcome_channel();
        let (results, state) = hive.try_scan_send(0..10, tx, 0, |acc, i| {
            *acc += i;
            Ok::<_, String>(*acc)
        });
        let mut task_ids: Vec<_> = results.into_iter().map(Result::unwrap).collect();
        assert_eq!(task_ids.len(), 10);
        assert_eq!(state, 45);
        let (mut outcome_task_ids, mut values): (Vec<TaskId>, Vec<i32>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, task_id } => (task_id, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        values.sort();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
    }

    #[rstest]
    #[should_panic]
    fn test_try_scan_send_fail<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .with_worker(OnceCaller::of(|i: i32| Ok::<_, String>(i * i)))
            .num_threads(4)
            .build();
        let (tx, _) = super::outcome_channel();
        let _ = hive
            .try_scan_send(0..10, &tx, 0, |_, _| Err::<i32, _>("fail"))
            .0
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
    }

    #[rstest]
    fn test_scan_store<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let mut hive = builder_factory(false)
            .with_worker(Caller::of(|i: i32| i * i))
            .num_threads(4)
            .build();
        let (mut task_ids, state) = hive.scan_store(0..10, 0, |acc, i| {
            *acc += i;
            *acc
        });
        assert_eq!(task_ids.len(), 10);
        assert_eq!(state, 45);
        hive.join();
        for i in task_ids.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_task_ids, values): (Vec<TaskId>, Vec<i32>) = task_ids
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .unzip();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
    }

    #[rstest]
    fn test_try_scan_store<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let mut hive = builder_factory(false)
            .with_worker(Caller::of(|i: i32| i * i))
            .num_threads(4)
            .build();
        let (results, state) = hive.try_scan_store(0..10, 0, |acc, i| {
            *acc += i;
            Ok::<i32, String>(*acc)
        });
        let mut task_ids: Vec<_> = results.into_iter().map(Result::unwrap).collect();
        assert_eq!(task_ids.len(), 10);
        assert_eq!(state, 45);
        hive.join();
        for i in task_ids.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_task_ids, values): (Vec<TaskId>, Vec<i32>) = task_ids
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .unzip();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        task_ids.sort();
        outcome_task_ids.sort();
        assert_eq!(task_ids, outcome_task_ids);
    }

    #[rstest]
    #[should_panic]
    fn test_try_scan_store_fail<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .with_worker(OnceCaller::of(|i: i32| Ok::<i32, String>(i * i)))
            .num_threads(4)
            .build();
        let _ = hive
            .try_scan_store(0..10, 0, |_, _| Err::<i32, _>("fail"))
            .0
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
    }

    const NUM_FIRST_TASKS: usize = 4;

    #[derive(Debug, Default)]
    struct SendWorker;

    impl Worker for SendWorker {
        type Input = usize;
        type Output = usize;
        type Error = ();

        fn apply(&mut self, input: Self::Input, ctx: &Context<Self::Input>) -> WorkerResult<Self> {
            if input < NUM_FIRST_TASKS {
                ctx.submit(input + NUM_FIRST_TASKS)
                    .map_err(|input| ApplyError::Retryable { input, error: () })?;
            }
            Ok(input)
        }
    }

    #[rstest]
    fn test_send_from_task<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .num_threads(2)
            .with_worker_default::<SendWorker>()
            .build();
        let (tx, rx) = super::outcome_channel();
        let task_ids = hive.map_send(0..NUM_FIRST_TASKS, tx);
        hive.join();
        // each task submits another task
        assert_eq!(task_ids.len(), NUM_FIRST_TASKS);
        let outputs: Vec<_> = rx.select_ordered_outputs(task_ids).collect();
        assert_eq!(outputs.len(), NUM_FIRST_TASKS * 2);
        assert_eq!(outputs, (0..NUM_FIRST_TASKS * 2).collect::<Vec<_>>());
    }

    #[rstest]
    fn test_husk<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive1 = thunk_hive::<u8, _, _>(8, builder_factory(false));
        let task_ids = hive1.map_store((0..8u8).map(|i| Thunk::of(move || i)));
        hive1.join();
        let mut husk1 = hive1.try_into_husk(false).unwrap();
        for i in task_ids.iter() {
            assert!(husk1.outcomes_deref().get(i).unwrap().is_success());
            assert!(matches!(husk1.get(*i), Some(Outcome::Success { .. })));
        }

        let builder = husk1.as_builder();
        let hive2 = builder
            .num_threads(4)
            .with_worker_default::<ThunkWorker<u8>>()
            .with_channel_queues()
            .build();
        hive2.map_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive2.join();
        let mut husk2 = hive2.try_into_husk(false).unwrap();

        let mut outputs1 = husk1
            .remove_all()
            .into_iter()
            .map(Outcome::unwrap)
            .collect::<Vec<_>>();
        outputs1.sort();
        let mut outputs2 = husk2
            .remove_all()
            .into_iter()
            .map(Outcome::unwrap)
            .collect::<Vec<_>>();
        outputs2.sort();
        assert_eq!(outputs1, outputs2);

        let hive3 = husk1.into_hive::<ChannelTaskQueues<_>>();
        hive3.map_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive3.join();
        let husk3 = hive3.try_into_husk(false).unwrap();
        let (_, outcomes3) = husk3.into_parts();
        let mut outputs3 = outcomes3
            .into_iter()
            .map(Outcome::unwrap)
            .collect::<Vec<_>>();
        outputs3.sort();
        assert_eq!(outputs1, outputs3);
    }

    #[rstest]
    fn test_clone<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive: Hive<DefaultQueen<TWrk<()>>, B::TaskQueues<_>> = builder_factory(false)
            .with_worker_default()
            .thread_name("clone example")
            .num_threads(2)
            .build();

        // This batch of tasks will occupy the pool for some time
        for _ in 0..6 {
            hive.apply_store(Thunk::of(|| {
                thread::sleep(SHORT_TASK);
            }));
        }

        // The following tasks will be inserted into the pool in a random fashion
        let t0 = {
            let hive = hive.clone();
            thread::spawn(move || {
                // wait for the first batch of tasks to finish
                hive.join();

                let (tx, rx) = mpsc::channel();
                for i in 0..42 {
                    let tx = tx.clone();
                    hive.apply_store(Thunk::of(move || {
                        tx.send(i).expect("channel will be waiting");
                    }));
                }
                drop(tx);
                rx.iter().sum::<i32>()
            })
        };
        let t1 = {
            let pool = hive.clone();
            thread::spawn(move || {
                // wait for the first batch of tasks to finish
                pool.join();

                let (tx, rx) = mpsc::channel();
                for i in 1..12 {
                    let tx = tx.clone();
                    pool.apply_store(Thunk::of(move || {
                        tx.send(i).expect("channel will be waiting");
                    }));
                }
                drop(tx);
                rx.iter().product::<i32>()
            })
        };

        assert_eq!(
            861,
            t0.join()
                .expect("thread 0 will return after calculating additions",)
        );
        assert_eq!(
            39916800,
            t1.join()
                .expect("thread 1 will return after calculating multiplications",)
        );
    }

    #[rstest]
    fn test_clone_into_husk_fails<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive1: Hive<DefaultQueen<TWrk<()>>, B::TaskQueues<_>> = builder_factory(false)
            .with_worker_default()
            .num_threads(2)
            .build();
        let hive2 = hive1.clone();
        // should return None the first time since there is more than one reference
        assert!(hive1.try_into_husk(false).is_none());
        // hive1 has been dropped, so we're down to 1 reference and it should succeed
        assert!(hive2.try_into_husk(false).is_some());
    }

    #[rstest]
    fn test_channel_hive_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Hive<DefaultQueen<TWrk<()>>, ChannelTaskQueues<_>>>();
    }

    #[rstest]
    fn test_workstealing_hive_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Hive<DefaultQueen<TWrk<()>>, WorkstealingTaskQueues<_>>>();
    }

    #[rstest]
    fn test_cloned_eq<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let a = thunk_hive::<(), _, _>(2, builder_factory(true));
        assert_eq!(a, a.clone());
    }

    #[rstest]
    /// When a thread joins on a pool, it blocks until all tasks have completed. If a second thread
    /// adds tasks to the pool and then joins before all the tasks have completed, both threads
    /// will wait for all tasks to complete. However, as soon as all tasks have completed, all
    /// joining threads are notified, and the first one to wake will exit the join and increment
    /// the phase of the condvar. Subsequent notified threads will then see that the phase has been
    /// changed and will wake, even if new tasks have been added in the meantime.
    ///
    /// In this example, this means the waiting threads will exit the join in groups of four
    /// because the waiter pool has four processes
    ///
    /// TODO: make this test work with WorkstealingTaskQueues.
    fn test_join_wavesurfer() {
        let n_waves = 4;
        let n_workers = 4;
        let (tx, rx) = mpsc::channel();
        let builder = channel_builder(false)
            .num_threads(n_workers)
            .thread_name("join wavesurfer");
        let waiter_hive = builder
            .clone()
            .with_worker_default::<ThunkWorker<()>>()
            .build();
        let clock_hive = builder.with_worker_default::<ThunkWorker<()>>().build();

        let barrier = Arc::new(Barrier::new(3));
        let wave_counter = Arc::new(AtomicUsize::new(0));
        let clock_thread = {
            let barrier = barrier.clone();
            let wave_counter = wave_counter.clone();
            thread::spawn(move || {
                barrier.wait();
                for wave_num in 0..n_waves {
                    let _ = wave_counter.swap(wave_num, Ordering::SeqCst);
                    thread::sleep(ONE_SEC);
                }
            })
        };

        {
            let barrier = barrier.clone();
            clock_hive.apply_store(Thunk::of(move || {
                barrier.wait();
                // this sleep is for stabilisation on weaker platforms
                thread::sleep(Duration::from_millis(100));
            }));
        }

        // prepare three waves of tasks (0..=11)
        for worker in 0..(3 * n_workers) {
            let tx = tx.clone();
            let clock_hive = clock_hive.clone();
            let wave_counter = wave_counter.clone();
            waiter_hive.apply_store(Thunk::of(move || {
                let wave_before = wave_counter.load(Ordering::SeqCst);
                clock_hive.join();
                // submit tasks for the next wave
                clock_hive.apply_store(Thunk::of(|| thread::sleep(ONE_SEC)));
                let wave_after = wave_counter.load(Ordering::SeqCst);
                tx.send((wave_before, wave_after, worker)).unwrap();
            }));
        }
        barrier.wait();

        clock_hive.join();

        drop(tx);
        let mut hist = vec![0; n_waves];
        let mut data = vec![];
        for (before, after, worker) in rx.iter() {
            let mut dur = after - before;
            if dur >= n_waves - 1 {
                dur = n_waves - 1;
            }
            hist[dur] += 1;
            data.push((before, after, worker));
        }

        println!("Histogram of wave duration:");
        for (i, n) in hist.iter().enumerate() {
            println!(
                "\t{}: {} {}",
                i,
                n,
                &*(0..*n).fold("".to_owned(), |s, _| s + "*")
            );
        }

        for (wave_before, wave_after, worker) in data.iter() {
            if *worker < n_workers {
                assert_eq!(wave_before, wave_after);
            } else {
                assert!(wave_before < wave_after);
            }
        }
        clock_thread.join().unwrap();
    }

    // cargo-llvm-cov doesn't yet support doctests in stable, so we need to duplicate them in
    // unit tests to get coverage

    #[rstest]
    fn doctest_lib_2<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        // create a hive to process `Thunk`s - no-argument closures with the same return type (`i32`)
        let hive: Hive<DefaultQueen<TWrk<i32>>, B::TaskQueues<_>> = builder_factory(false)
            .with_worker_default()
            .num_threads(4)
            .thread_name("thunk_hive")
            .build();

        // return results to your own channel...
        let (tx, rx) = crate::hive::outcome_channel();
        let task_ids = hive.swarm_send((0..10).map(|i: i32| Thunk::of(move || i * i)), &tx);
        let outputs: Vec<_> = rx.select_unordered_outputs(task_ids).collect();
        assert_eq!(285, outputs.into_iter().sum());

        // return results as an iterator...
        let outputs2: Vec<_> = hive
            .swarm((0..10).map(|i: i32| Thunk::of(move || i * -i)))
            .into_outputs()
            .collect();
        assert_eq!(-285, outputs2.into_iter().sum());
    }

    #[rstest]
    fn doctest_lib_3<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        #[derive(Debug)]
        struct CatWorker {
            stdin: ChildStdin,
            stdout: BufReader<ChildStdout>,
        }

        impl CatWorker {
            fn new(stdin: ChildStdin, stdout: ChildStdout) -> Self {
                Self {
                    stdin,
                    stdout: BufReader::new(stdout),
                }
            }

            fn write_char(&mut self, c: u8) -> io::Result<String> {
                self.stdin.write_all(&[c])?;
                self.stdin.write_all(b"\n")?;
                self.stdin.flush()?;
                let mut s = String::new();
                self.stdout.read_line(&mut s)?;
                s.pop(); // exclude newline
                Ok(s)
            }
        }

        impl Worker for CatWorker {
            type Input = u8;
            type Output = String;
            type Error = io::Error;

            fn apply(&mut self, input: Self::Input, _: &Context<u8>) -> WorkerResult<Self> {
                self.write_char(input).map_err(|error| ApplyError::Fatal {
                    input: Some(input),
                    error,
                })
            }
        }

        #[derive(Default)]
        struct CatQueen {
            children: Vec<Child>,
        }

        impl CatQueen {
            fn wait_for_all(&mut self) -> Vec<io::Result<ExitStatus>> {
                self.children
                    .drain(..)
                    .map(|mut child| child.wait())
                    .collect()
            }
        }

        impl QueenMut for CatQueen {
            type Kind = CatWorker;

            fn create(&mut self) -> Self::Kind {
                let mut child = Command::new("cat")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap();
                let stdin = child.stdin.take().unwrap();
                let stdout = child.stdout.take().unwrap();
                self.children.push(child);
                CatWorker::new(stdin, stdout)
            }
        }

        impl Drop for CatQueen {
            fn drop(&mut self) {
                self.wait_for_all()
                    .into_iter()
                    .for_each(|result| match result {
                        Ok(status) if status.success() => (),
                        Ok(status) => eprintln!("Child process failed: {}", status),
                        Err(e) => eprintln!("Error waiting for child process: {}", e),
                    })
            }
        }

        // build the Hive
        let hive = builder_factory(false)
            .with_queen_mut_default::<CatQueen>()
            .num_threads(4)
            .build();

        // prepare inputs
        let inputs: Vec<u8> = (0..8).map(|i| 97 + i).collect();

        // execute tasks and collect outputs
        let output = hive
            .swarm(inputs)
            .into_outputs()
            .fold(String::new(), |mut a, b| {
                a.push_str(&b);
                a
            })
            .into_bytes();

        // verify the output - note that `swarm` ensures the outputs are in the same order
        // as the inputs
        assert_eq!(output, b"abcdefgh");

        // shutdown the hive, use the Queen to wait on child processes, and report errors
        let mut queen = hive
            .try_into_husk(false)
            .unwrap()
            .into_parts()
            .0
            .into_inner();
        let (wait_ok, wait_err): (Vec<_>, Vec<_>) =
            queen.wait_for_all().into_iter().partition(Result::is_ok);
        if !wait_err.is_empty() {
            panic!(
                "Error(s) occurred while waiting for child processes: {:?}",
                wait_err
            );
        }
        let exec_err_codes: Vec<_> = wait_ok
            .into_iter()
            .map(Result::unwrap)
            .filter(|status| !status.success())
            .filter_map(|status| status.code())
            .collect();
        if !exec_err_codes.is_empty() {
            panic!(
                "Child process(es) failed with exit codes: {:?}",
                exec_err_codes
            );
        }
    }
}

#[cfg(all(test, feature = "affinity"))]
mod affinity_tests {
    use crate::bee::stock::{Thunk, ThunkWorker};
    use crate::hive::{Builder, TaskQueuesBuilder, channel_builder, workstealing_builder};
    use rstest::*;

    #[rstest]
    fn test_affinity<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .thread_name("affinity example")
            .num_threads(2)
            .core_affinity(0..2)
            .with_worker_default::<ThunkWorker<()>>()
            .build();

        hive.map_store((0..10).map(move |i| {
            Thunk::of(move || {
                if let Some(affininty) = core_affinity::get_core_ids() {
                    eprintln!("task {} on thread with affinity {:?}", i, affininty);
                }
            })
        }));
    }

    #[rstest]
    fn test_use_all_cores() {
        let hive = crate::hive::channel_builder(false)
            .thread_name("affinity example")
            .with_thread_per_core()
            .with_default_core_affinity()
            .with_worker_default::<ThunkWorker<()>>()
            .build();

        hive.map_store((0..num_cpus::get()).map(move |i| {
            Thunk::of(move || {
                if let Some(affininty) = core_affinity::get_core_ids() {
                    eprintln!("task {} on thread with affinity {:?}", i, affininty);
                }
            })
        }));
    }
}

#[cfg(all(test, feature = "local-batch"))]
mod local_batch_tests {
    use crate::barrier::IndexedBarrier;
    use crate::bee::DefaultQueen;
    use crate::bee::stock::{Thunk, ThunkWorker};
    use crate::hive::{
        Builder, Hive, OutcomeIteratorExt, OutcomeReceiver, OutcomeSender, TaskQueues,
        TaskQueuesBuilder, channel_builder, workstealing_builder,
    };
    use rstest::*;
    use std::collections::HashMap;
    use std::thread::{self, ThreadId};
    use std::time::Duration;

    fn launch_tasks<T: TaskQueues<ThunkWorker<ThreadId>>>(
        hive: &Hive<DefaultQueen<ThunkWorker<ThreadId>>, T>,
        num_threads: usize,
        num_tasks_per_thread: usize,
        barrier: &IndexedBarrier,
        tx: &OutcomeSender<ThunkWorker<ThreadId>>,
    ) -> Vec<usize> {
        let total_tasks = num_threads * num_tasks_per_thread;
        // send the first `num_threads` tasks widely spaced, so each worker thread only gets one
        let init_task_ids: Vec<_> = (0..num_threads)
            .map(|_| {
                let barrier = barrier.clone();
                let task_id = hive.apply_send(
                    Thunk::of(move || {
                        barrier.wait();
                        thread::sleep(Duration::from_millis(100));
                        thread::current().id()
                    }),
                    tx,
                );
                thread::sleep(Duration::from_millis(100));
                task_id
            })
            .collect();
        // send the rest all at once
        let rest_task_ids = hive.map_send(
            (num_threads..total_tasks).map(|_| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis(1));
                    thread::current().id()
                })
            }),
            tx,
        );
        init_task_ids.into_iter().chain(rest_task_ids).collect()
    }

    fn count_thread_ids(
        rx: OutcomeReceiver<ThunkWorker<ThreadId>>,
        task_ids: Vec<usize>,
    ) -> HashMap<ThreadId, usize> {
        rx.select_unordered_outputs(task_ids)
            .fold(HashMap::new(), |mut counter, id| {
                *counter.entry(id).or_insert(0) += 1;
                counter
            })
    }

    fn run_test<T: TaskQueues<ThunkWorker<ThreadId>>>(
        hive: &Hive<DefaultQueen<ThunkWorker<ThreadId>>, T>,
        num_threads: usize,
        batch_limit: usize,
        assert_exact: bool,
    ) {
        let tasks_per_thread = batch_limit + 2;
        let (tx, rx) = crate::hive::outcome_channel();
        // each worker should take `batch_limit` tasks for its queue + 1 to work on immediately,
        // meaning there should be `batch_limit + 1` tasks associated with each thread ID
        let barrier = IndexedBarrier::new(num_threads);
        let task_ids = launch_tasks(hive, num_threads, tasks_per_thread, &barrier, &tx);
        // start the first tasks
        barrier.wait();
        // wait for all tasks to complete
        hive.join();
        let thread_counts = count_thread_ids(rx, task_ids);
        assert_eq!(thread_counts.len(), num_threads);
        assert_eq!(
            thread_counts.values().sum::<usize>(),
            tasks_per_thread * num_threads
        );
        if assert_exact {
            assert!(
                thread_counts
                    .values()
                    .all(|&count| count == tasks_per_thread)
            );
        } else {
            assert!(thread_counts.values().all(|&count| count > 0));
        }
    }

    #[rstest]
    fn test_local_batch_channel() {
        const NUM_THREADS: usize = 4;
        const BATCH_LIMIT: usize = 24;
        let hive = channel_builder(false)
            .with_worker_default()
            .num_threads(NUM_THREADS)
            .batch_limit(BATCH_LIMIT)
            .build();
        run_test(&hive, NUM_THREADS, BATCH_LIMIT, true);
    }

    #[rstest]
    fn test_local_batch_workstealing() {
        const NUM_THREADS: usize = 4;
        const BATCH_LIMIT: usize = 24;
        let hive = workstealing_builder(false)
            .with_worker_default()
            .num_threads(NUM_THREADS)
            .batch_limit(BATCH_LIMIT)
            .build();
        run_test(&hive, NUM_THREADS, BATCH_LIMIT, false);
    }

    #[rstest]
    fn test_set_batch_limit_channel() {
        const NUM_THREADS: usize = 4;
        const BATCH_LIMIT_0: usize = 10;
        const BATCH_LIMIT_1: usize = 50;
        const BATCH_LIMIT_2: usize = 20;
        let hive = channel_builder(false)
            .with_worker_default()
            .num_threads(NUM_THREADS)
            .batch_limit(BATCH_LIMIT_0)
            .build();
        run_test(&hive, NUM_THREADS, BATCH_LIMIT_0, true);
        // increase batch size
        hive.set_worker_batch_limit(BATCH_LIMIT_1);
        run_test(&hive, NUM_THREADS, BATCH_LIMIT_1, true);
        // decrease batch size
        hive.set_worker_batch_limit(BATCH_LIMIT_2);
        run_test(&hive, NUM_THREADS, BATCH_LIMIT_2, true);
    }

    #[rstest]
    fn test_set_batch_limit_workstealing() {
        const NUM_THREADS: usize = 4;
        const BATCH_LIMIT_0: usize = 10;
        const BATCH_LIMIT_1: usize = 50;
        const BATCH_LIMIT_2: usize = 20;
        let hive = workstealing_builder(false)
            .with_worker_default()
            .num_threads(NUM_THREADS)
            .batch_limit(BATCH_LIMIT_0)
            .build();
        run_test(&hive, NUM_THREADS, BATCH_LIMIT_0, false);
        // increase batch size
        hive.set_worker_batch_limit(BATCH_LIMIT_1);
        run_test(&hive, NUM_THREADS, BATCH_LIMIT_1, false);
        // decrease batch size
        hive.set_worker_batch_limit(BATCH_LIMIT_2);
        run_test(&hive, NUM_THREADS, BATCH_LIMIT_2, false);
    }

    // TODO: make this work with WorkstealingTaskQueues
    #[rstest]
    fn test_shrink_batch_limit() {
        const NUM_THREADS: usize = 4;
        const NUM_TASKS_PER_THREAD: usize = 125;
        const BATCH_LIMIT_0: usize = 100;
        const BATCH_LIMIT_1: usize = 10;
        let hive = channel_builder(false)
            .with_worker_default()
            .num_threads(NUM_THREADS)
            .batch_limit(BATCH_LIMIT_0)
            .build();
        let (tx, rx) = crate::hive::outcome_channel();
        let barrier = IndexedBarrier::new(NUM_THREADS);
        let task_ids = launch_tasks(&hive, NUM_THREADS, NUM_TASKS_PER_THREAD, &barrier, &tx);
        let total_tasks = NUM_THREADS * NUM_TASKS_PER_THREAD;
        assert_eq!(task_ids.len(), total_tasks);
        barrier.wait();
        hive.set_worker_batch_limit(BATCH_LIMIT_1);
        // The number of tasks completed by each thread could be variable, so we want to ensure
        // that a) each processed at least `BATCH_LIMIT_0` tasks, and b) there are a total of
        // `NUM_TASKS` outputs with no errors
        hive.join();
        let thread_counts = count_thread_ids(rx, task_ids);
        assert!(thread_counts.values().all(|count| *count > BATCH_LIMIT_0));
        assert_eq!(thread_counts.values().sum::<usize>(), total_tasks);
    }
}

#[cfg(all(test, feature = "retry"))]
mod retry_tests {
    use crate::bee::stock::RetryCaller;
    use crate::bee::{ApplyError, Context};
    use crate::hive::{
        Builder, Outcome, OutcomeIteratorExt, TaskQueuesBuilder, channel_builder,
        workstealing_builder,
    };
    use rstest::*;
    use std::time::{Duration, SystemTime};

    fn echo_time(i: usize, ctx: &Context<usize>) -> Result<String, ApplyError<usize, String>> {
        let attempt = ctx.attempt();
        if attempt == 3 {
            Ok("Success".into())
        } else {
            // the delay between each message should be exponential
            eprintln!("Task {} attempt {}: {:?}", i, attempt, SystemTime::now());
            Err(ApplyError::Retryable {
                input: i,
                error: "Retryable".into(),
            })
        }
    }

    #[rstest]
    fn test_retries<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .with_worker(RetryCaller::of(echo_time))
            .with_thread_per_core()
            .max_retries(3)
            .retry_factor(Duration::from_secs(1))
            .build();

        let v: Result<Vec<_>, _> = hive.swarm(0..10usize).into_results().collect();
        assert_eq!(v.unwrap().len(), 10);
    }

    #[rstest]
    fn test_retries_fail<B, F>(#[values(channel_builder, workstealing_builder)] builder_factory: F)
    where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        fn sometimes_fail(
            i: usize,
            _: &Context<usize>,
        ) -> Result<String, ApplyError<usize, String>> {
            match i % 3 {
                0 => Ok("Success".into()),
                1 => Err(ApplyError::Retryable {
                    input: i,
                    error: "Retryable".into(),
                }),
                2 => Err(ApplyError::Fatal {
                    input: Some(i),
                    error: "Fatal".into(),
                }),
                _ => unreachable!(),
            }
        }

        let hive = builder_factory(false)
            .with_worker(RetryCaller::of(sometimes_fail))
            .with_thread_per_core()
            .max_retries(3)
            .build();

        let (success, retry_failed, not_retried) = hive.swarm(0..10usize).fold(
            (0, 0, 0),
            |(success, retry_failed, not_retried), outcome| match outcome {
                Outcome::Success { .. } => (success + 1, retry_failed, not_retried),
                Outcome::MaxRetriesAttempted { .. } => (success, retry_failed + 1, not_retried),
                Outcome::Failure { .. } => (success, retry_failed, not_retried + 1),
                _ => unreachable!(),
            },
        );

        assert_eq!(success, 4);
        assert_eq!(retry_failed, 3);
        assert_eq!(not_retried, 3);
    }

    #[rstest]
    fn test_disable_retries<B, F>(
        #[values(channel_builder, workstealing_builder)] builder_factory: F,
    ) where
        B: TaskQueuesBuilder,
        F: Fn(bool) -> B,
    {
        let hive = builder_factory(false)
            .with_worker(RetryCaller::of(echo_time))
            .with_thread_per_core()
            .with_no_retries()
            .build();
        let v: Result<Vec<_>, _> = hive.swarm(0..10usize).into_results().collect();
        assert!(v.is_err());
    }
}
