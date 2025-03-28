#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
//! A Rust library that provides a [thread pool](https://en.wikipedia.org/wiki/Thread_pool)
//! implementation designed to execute the same operation in parallel on any number of inputs (this
//! is sometimes called a "worker pool").
//!
//! ## Overview
//!
//! * Operations are defined by implementing the [`Worker`](crate::bee::Worker) trait.
//! * A [`Builder`](crate::hive::Builder) is used to configure and create a worker pool
//!   called a [`Hive`](crate::hive::Hive).
//! * `Hive` is generic over:
//!     * The type of [`Queen`](crate::bee::Queen), which creates `Worker` instances
//!     * The type of [`TaskQueues`](crate::hive::TaskQueues), which provides the global and
//!       worker thread-local queues for managing tasks; there are currently two implementations:
//!         * Channel: A
//!           [`crossbeam` channel](https://docs.rs/crossbeam-channel/latest/crossbeam_channel/)
//!           is used to send tasks from the `Hive` to the worker threads. *This is a good choice
//!           for most workloads*.
//!         * Workstealing: A
//!           [`crossbeam_dequeue::Injector`](https://docs.rs/crossbeam-deque/latest/crossbeam_deque/struct.Injector.html)
//!           is used to submit tasks and serves as a global queue. Worker threads each have their
//!           own local queue and can take tasks either from the global queue or steal from other
//!           workers' local queues if their own queue is empty. This is a good choice for workloads
//!           that are either highly variable from task to task (in terms of processing time), or
//!           are fork-join in nature (i.e., tasks that submit sub-tasks).
//! * The `Hive` creates a `Worker` instance for each thread in the pool.
//! * Each thread in the pool continually:
//!     * Receives a task from an input queue,
//!     * Calls its `Worker`'s [`apply`](crate::bee::Worker::apply) method on the input, and
//!     * Produces an [`Outcome`](crate::hive::Outcome).
//! * Depending on which of `Hive`'s methods are called to submit a task (or batch of tasks), the
//!   `Outcome`(s) may be returned as an [`Iterator`], sent to an output `channel`, or stored in the
//!   `Hive` for later retrieval.
//! * A `Hive` may create `Worker`s may in one of three ways:
//!     * Call the `default()` function on a `Worker` type that implements [`Default`]
//!     * Clone an instance of a `Worker` that implements [`Clone`]
//!     * Call the [`create()`](crate::bee::Queen::create) method on a worker factory that
//!       implements the [`Queen`](crate::bee::Queen) trait.
//! * A `Worker`s may be stateful, i.e., `Worker::apply()` takes `&mut self`.
//! * While `Queen` is not stateful, [`QueenMut`](crate::bee::QueenMut) may be (i.e., it's
//!   `create()` method takes `&mut self`)
//! * Although it is strongly recommended to avoid `panic`s in worker threads (and thus, within
//!   `Worker` implementations), the `Hive` does automatically restart any threads that panic.
//! * A `Hive` may be [`suspend`](crate::hive::Hive::suspend)ed and
//!   [`resume`](crate::hive::Hive::resume)d at any time. When a `Hive` is suspended, worker threads
//!   do no work and tasks accumulate in the input `channel`.
//! * Several utility functions are provided in the [`util`] module. Notably, the `map`
//!   and `try_map` functions enable simple parallel processing of a single batch of tasks.
//! * Several useful `Worker` implementations are provided in the [stock](crate::bee::stock) module.
//!   Most notable are the `Caller*` types, which provide different ways of wrapping `callable`s,
//!   i.e., closures and function pointers.
//! * The following optional features are provided via feature flags:
//!     * `affinity`: worker threads may be pinned to CPU cores to minimize the overhead of
//!       context-switching.
//!     * `local-batch` (>=0.3.0): worker threads take batches of tasks from the global input queue
//!       and add them to a local queue, which may alleviate thread contention, especially when
//!       there are many short-lived tasks.
//!         * Tasks may be [`Weighted`](crate::hive::Weighted) to enable balancing unevenly sized
//!           tasks between worker threads.
//!     * `retry`: Tasks that fail due to transient errors (e.g., temporarily unavailable resources)
//!       may be retried up to a set number of times, with an optional, exponentially increasing
//!       delay between retries.
//!     * Several alternative `channel` implementations are supported:
//!         * [`crossbeam`](https://docs.rs/crossbeam/latest/crossbeam/)
//!         * [`flume`](https://github.com/zesterer/flume)
//!         * [`loole`](https://github.com/mahdi-shojaee/loole)
//!
//! ## Usage
//!
//! To parallelize a task, you'll need two things:
//! 1. A [`Worker`](crate::bee::Worker) implementation. Your options are:
//!     * Use an existing implementation from the [`stock`](crate::bee::stock) module (see Example 2 below)
//!     * Implement your own (See Example 3 below)
//!         * `use` the necessary traits (e.g., `use beekeeper::bee::prelude::*`)
//!         * Define a `struct` for your worker
//!         * Implement the `Worker` trait on your struct and define the
//!           [`apply`](crate::bee::Worker::apply) method with the logic of your task
//!         * Do at least one of the following:
//!             * Implement [`Default`] for your worker
//!             * Implement [`Clone`] for your worker
//!             * Create a custom worker factory that implements the [`Queen`](crate::bee::Queen)
//!               or [`QueenMut`](crate::bee::QueenMut) trait
//! 2. A [`Hive`](crate::hive::Hive) to execute your tasks. Your options are:
//!     * Use one of the convenience methods in the [`util`] module (see Example 1 below)
//!     * Create a `Hive` manually using a [`Builder`](crate::hive::Builder) (see Examples 2
//!       and 3 below)
//!         * [`OpenBuilder`](crate::hive::OpenBuilder) is the most general builder
//!         * [`OpenBuilder::empty()`](crate::hive::OpenBuilder::empty) creates an empty `OpenBuilder`
//!         * [`OpenBuilder::default()`](crate::hive::OpenBuilder::default) creates a `OpenBuilder`
//!           with the global default settings (which may be changed using the functions in the
//!           [`hive`] module, e.g., `beekeeper::hive::set_num_threads_default(4)`).
//!         * The builder must be specialized for the `Queen` and `TaskQueues` types:
//!             * If you have a `Worker` that implements `Default`, use
//!               [`with_worker_default::<MyWorker>()`](crate::hive::OpenBuilder::with_worker_default)
//!             * If you have a `Worker` that implements `Clone`, use
//!               [`with_worker(MyWorker::new())`](crate::hive::OpenBuilder::with_worker)
//!             * If you have a custom `Queen`, use
//!               [`with_queen_default::<MyQueen>()`](crate::hive::OpenBuilder::with_queen_default)
//!               if it implements `Default`, otherwise use
//!               [`with_queen(MyQueen::new())`](crate::hive::OpenBuilder::with_queen)
//!             * If instead your queen implements `QueenMut`, use
//!               [`with_queen_mut_default::<MyQueenMut>()`](crate::hive::OpenBuilder::with_queen_mut_default)
//!               or [`with_queen_mut(MyQueenMut::new())`](crate::hive::OpenBuilder::with_queen_mut)
//!             * Use [`with_channel_queues`](crate::hive::OpenBuilder::with_channel_queues) or
//!               [`with_workstealing_queues`](crate::hive::OpenBuilder::with_workstealing_queues)
//!               to specify the `TaskQueues` type.
//!         * Note that [`Builder::num_threads()`](crate::hive::Builder::num_threads) must be set
//!           to a non-zero value, otherwise the built `Hive` will not start any worker threads
//!           until you call the [`Hive::grow()`](crate::hive::Hive::grow) method.
//!
//! Once you've created a `Hive`, use its methods to submit tasks for processing. There are
//! four groups of methods available:
//! * `apply`: submits a single task
//! * `swarm`: submits a batch of tasks given a collection of inputs with known size (i.e., anything
//!   that implements [`IntoIterator<IntoIter: ExactSizeIterator>`])
//! * `map`: submits an arbitrary batch of tasks (i.e., anything that implements `IntoIterator`)
//! * `scan`: Similar to `map`, but you also provide 1) an initial value for a state variable, and
//!   2) a function that transforms each item in the input iterator into the input type required by
//!   the `Worker`, and also has access to (and may modify) the state variable.
//!
//! There are multiple methods in each group that differ by how the task results (called
//! [`Outcome`](crate::hive::Outcome)s) are handled:
//! * The unsuffixed methods return an `Iterator` over the `Outcome`s in the same order as the
//!   inputs (or, in the case of `apply`, a single `Outcome`)
//! * The methods with the `_unordered` suffix instead return an unordered iterator, which may be
//!   more performant than the ordered iterator
//! * The methods with the `_send` suffix accept a channel [`Sender`](crate::channel::Sender) and
//!   send the `Outcome`s to that channel as they are completed
//! * The methods with the `_store` suffix store the `Outcome`s in the `Hive`; these may be
//!   retrieved later using one of the `remove*` methods (which requires
//!   [`OutcomeStore`](crate::hive::OutcomeStore) to be in scope), or by
//!   using one of the methods on [`Husk`](crate::hive::Husk) after shutting down the `Hive` using
//!   [`Hive::try_into_husk()`](crate::hive::Hive::try_into_husk).
//!
//! When using one of the `_send` methods, you should ensure that the `Sender` is dropped after
//! all tasks have been submitted, otherwise calling `recv()` on (or iterating over) the `Receiver`
//! will block indefinitely.
//!
//! Within a `Hive`, each submitted task is assinged a unique ID. The `_send` and `_store`
//! methods return the task IDs of the submitted tasks, which can be used to retrieve them later
//! (e.g., using [`Hive::remove()`](crate::hive::OutcomeStore::remove)).
//!
//! After submitting tasks, you may use the [`Hive::join()`](crate::hive::Hive::join) method to wait
//! for all tasks to complete. Using `join` is strongly recommended when using one of the `_store`
//! methods, otherwise you'll need to continually poll the `Hive` to check for completed tasks.
//!
//! When you are finished with a `Hive`, you may simply drop it (either explicitly, or by letting
//! it go out of scope) - the worker threads will be terminated automatically. If you used the
//! `_store` methods and would like to have access to the stored task `Outcome`s after the `Hive`
//! has been dropped, and/or you'd like to re-use the `Hive's` `Queen` or other configuration
//! parameters, you can use the [`Hive::try_into_husk()`](crate::hive::Hive::try_into_husk) method to extract
//! the relevant data from the `Hive` into a [`Husk`](crate::hive::Husk) object.
//!
//! ## Examples
//!
//! ### 1. Parallelize an existing function
//!
//! ```
//! pub fn double(i: usize) -> usize {
//!   i * 2
//! }
//!
//! # fn main() {
//! // parallelize the computation of `double` on a range of numbers
//! // over 4 threads, and sum the results
//! const N: usize = 100;
//! let sum_doubles: usize = beekeeper::util::map(4, 0..N, double)
//!     .into_iter()
//!     .sum();
//! println!("Sum of {} doubles: {}", N, sum_doubles);
//! # }
//! ```
//!
//! ### 2. Parallelize arbitrary tasks with the same output type
//!
//! ```
//! use beekeeper::bee::stock::{Thunk, ThunkWorker};
//! use beekeeper::hive::prelude::*;
//!
//! # fn main() {
//! // create a hive to process `Thunk`s - no-argument closures with the
//! // same return type (`i32`)
//! let hive = channel_builder(false)
//!     .num_threads(4)
//!     .thread_name("thunk_hive")
//!     .with_worker_default::<ThunkWorker<i32>>()
//!     .build();
//!
//! // return results to your own channel...
//! let (tx, rx) = outcome_channel();
//! let _ = hive.swarm_send(
//!     (0..10).map(|i: i32| Thunk::from(move || i * i)),
//!     tx
//! );
//! assert_eq!(285, rx.into_outputs().take(10).sum());
//!
//! // return results as an iterator...
//! let total = hive
//!     .swarm_unordered((0..10).map(|i: i32| Thunk::from(move || i * -i)))
//!     .into_outputs()
//!     .sum();
//! assert_eq!(-285, total);
//! # }
//! ```
//!
//! ### 3. Parallelize a complex task using a stateful `Worker`
//!
//! Suppose you'd like to parallelize executions of a line-delimited process, such as `cat`.
//! This requires defining a `struct` to hold the process `stdin` and `stdout`, and
//! implementing the `Worker` trait for this struct. We'll also use a custom `Queen` to keep track
//! of the [`Child`](::std::process::Child) processes and make sure they're terminated properly.
//!
//! ```
//! use beekeeper::bee::prelude::*;
//! use beekeeper::hive::prelude::*;
//! use std::io::prelude::*;
//! use std::io::{self, BufReader};
//! use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
//!
//! #[derive(Debug)]
//! struct CatWorker {
//!     stdin: ChildStdin,
//!     stdout: BufReader<ChildStdout>,
//! }
//!
//! impl CatWorker {
//!     fn new(stdin: ChildStdin, stdout: ChildStdout) -> Self {
//!         Self {
//!             stdin,
//!             stdout: BufReader::new(stdout),
//!         }
//!     }
//!
//!     fn write_char(&mut self, c: u8) -> io::Result<String> {
//!         self.stdin.write_all(&[c])?;
//!         self.stdin.write_all(b"\n")?;
//!         self.stdin.flush()?;
//!         let mut s = String::new();
//!         self.stdout.read_line(&mut s)?;
//!         s.pop(); // exclude newline
//!         Ok(s)
//!     }
//! }
//!
//! impl Worker for CatWorker {
//!     type Input = u8;
//!     type Output = String;
//!     type Error = io::Error;
//!
//!     fn apply(
//!         &mut self,
//!         input: Self::Input,
//!         _: &Context<u8>
//!     ) -> WorkerResult<Self> {
//!         self.write_char(input).map_err(|error| {
//!             ApplyError::Fatal { input: Some(input), error }
//!         })
//!     }
//! }
//!
//! #[derive(Default)]
//! struct CatQueen {
//!     children: Vec<Child>,
//! }
//!
//! impl CatQueen {
//!     fn wait_for_all(&mut self) -> Vec<io::Result<ExitStatus>> {
//!         self.children
//!             .drain(..)
//!             .map(|mut child| child.wait())
//!             .collect()
//!     }
//! }
//!
//! impl QueenMut for CatQueen {
//!     type Kind = CatWorker;
//!
//!     fn create(&mut self) -> Self::Kind {
//!         let mut child = Command::new("cat")
//!             .stdin(Stdio::piped())
//!             .stdout(Stdio::piped())
//!             .stderr(Stdio::inherit())
//!             .spawn()
//!             .unwrap();
//!         let stdin = child.stdin.take().unwrap();
//!         let stdout = child.stdout.take().unwrap();
//!         self.children.push(child);
//!         CatWorker::new(stdin, stdout)
//!     }
//! }
//!
//! impl Drop for CatQueen {
//!     fn drop(&mut self) {
//!         self.wait_for_all().into_iter().for_each(|result| {
//!             match result {
//!                 Ok(status) if status.success() => (),
//!                 Ok(status) => {
//!                     eprintln!("Child process failed: {}", status);
//!                 }
//!                 Err(e) => {
//!                     eprintln!("Error waiting for child process: {}", e);
//!                 }
//!             }
//!         })
//!     }
//! }
//!
//! # fn main() {
//! // build the Hive
//! let hive = channel_builder(false)
//!     .num_threads(4)
//!     .with_queen_mut_default::<CatQueen>()
//!     .build();
//!
//! // prepare inputs
//! let inputs = (0..8).map(|i| 97 + i);
//!
//! // execute tasks and collect outputs
//! let output = hive
//!     .swarm(inputs)
//!     .into_outputs()
//!     .fold(String::new(), |mut a, b| {
//!         a.push_str(&b);
//!         a
//!     })
//!     .into_bytes();
//!
//! // verify the output - note that `swarm` ensures the outputs are in the same order
//! // as the inputs
//! assert_eq!(output, b"abcdefgh");
//!
//! // shutdown the hive, use the Queen to wait on child processes, and report errors;
//! // the `_outcomes` will be empty as we did not use any `_store` methods
//! let (queen, _outcomes) = hive.try_into_husk(false).unwrap().into_parts();
//! let (wait_ok, wait_err): (Vec<_>, Vec<_>) =
//!     queen.into_inner().wait_for_all().into_iter().partition(Result::is_ok);
//! if !wait_err.is_empty() {
//!     panic!(
//!         "Error(s) occurred while waiting for child processes: {:?}",
//!         wait_err
//!     );
//! }
//! let exec_err_codes: Vec<_> = wait_ok
//!     .into_iter()
//!     .map(Result::unwrap)
//!     .filter_map(|status| (!status.success()).then(|| status.code()))
//!     .flatten()
//!     .collect();
//! if !exec_err_codes.is_empty() {
//!     panic!(
//!         "Child process(es) failed with exit codes: {:?}",
//!         exec_err_codes
//!     );
//! }
//! # }
//! ```
mod atomic;
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod barrier;
pub mod bee;
mod boxed;
pub mod channel;
pub mod hive;
pub mod panic;
pub mod util;
