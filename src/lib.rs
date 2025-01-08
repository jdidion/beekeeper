//! A Rust library that provides a [thread pool](https://en.wikipedia.org/wiki/Thread_pool)
//! implementation designed to execute the same operation in parallel on any number of inputs (this
//! is sometimes called a "worker pool").
//!
//! ## Overview
//!
//! * Operations are defined by implementing the [`Worker`](crate::bee::Worker) trait.
//! * A [`Builder`](crate::hive::Builder) is used to configure and create a worker pool, called a
//!   [`Hive`](crate::hive::Hive).
//! * The `Hive` creates a `Worker` instance for each thread in the pool.
//! * Each thread in the pool continually:
//!     * Recieves a task from an input
//!       [`channel`](https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html),
//!     * Calls its `Worker`'s [`apply`](crate::bee::Worker::apply) method on the input, and
//!     * Produces an [`Outcome`](crate::hive::Outcome).
//! * Depending on which of `Hive`'s methods are called to submit a task (or batch of tasks), the
//!   `Outcome`(s) may be sent to an output `channel` or stored in the `Hive` for later retrieval.
//! * A `Hive` may create `Worker`s may in one of three ways:
//!     * Call the `default()` function on a `Worker` type that implements
//!       [`Default`](https://doc.rust-lang.org/std/default/trait.Default.html)
//!     * Clone an instance of a `Worker` that implements
//!       [`Clone`](https://doc.rust-lang.org/std/clone/trait.Clone.html)
//!     * Call the [`create()`](crate::bee::Queen::create) method on an a worker factory that
//!       implements the [`Queen`](crate::bee::Queen) trait.
//! * Both `Worker`s and `Queen`s may be stateful - i.e., `Worker::apply()` and `Queen::create()`
//!   both take `&mut self`.
//! * Although it is strongly recommended to avoid `panic`s in worker threads (and thus, within
//!   `Worker` implementations), the `Hive` does automatically restart any threads that panic.
//! * A `Hive` may be [`suspend`](crate::hive::Hive::suspend)ed and
//!   [`resume`](crate::hive::Hive::resume)d at any time. When a `Hive` is suspended, worker threads
//!   do no work and tasks accumulate in the input `channel`.
//! * Several utility functions are provided in the [util](crate::util) module. Notably, the `map`
//!   and `try_map` functions enable simple parallel processing of a single batch of tasks.
//! * Several useful `Worker` implementations are provided in the [stock](create::bee::stock) module.
//!   Most notable are those in the [`call`](crate::bee::stock::call) submodule, which provide
//!   different ways of wrapping `callable`s - i.e., closures and function pointers.
//! * The following optional features are provided via feature flags:
//!     * `affinity`: worker threads may be pinned to CPU cores to minimize the overhead of
//!       context-switching.
//!     * `retry`: Tasks that fail due to transient errors (e.g., temporarily unavailable resources)
//!       may be retried a set number of times, with an optional exponentially increasing delay
//!       between retries.
//! * Several alternative `channel` implementations are supported:
//!     * [`crossbeam`](https://docs.rs/crossbeam/latest/crossbeam/)
//!     * [`flume`](https://github.com/zesterer/flume)
//!     * [`kanal`](https://github.com/fereidani/kanal)
//!     * [`loole`](https://github.com/mahdi-shojaee/loole)
//!
//! ## Examples
//!
//! ### Parallelize an existing function
//!
//! ```
//! fn double(i: usize) -> usize {
//!   i * 2
//! }
//!
//! # fn main() {
//! // parallelize the computation of `double` on a range of numbers over
//! // 4 threads, and sum the results
//! const N: usize = 100;
//! let sum_doubles: usize = drudge::util::map(4, 0..N, double).into_iter().sum();
//! println!("Sum of {} doubles: {}", N, sum_doubles);
//! # }
//! ```
//!
//! ### Parallelize arbitrary tasks with the same output type
//!
//! ```
//! use drudge::bee::stock::{Thunk, ThunkWorker};
//! use drudge::hive::prelude::*;
//!
//! # fn main() {
//! // create a hive to process `Thunk`s - no-argument closures with the same return type (`i32`)
//! let hive = Builder::new()
//!     .num_threads(4)
//!     .thread_name("thunk_hive")
//!     .build_with_default::<ThunkWorker<i32>>();
//!
//! // return results to your own channel...
//! let (tx, rx) = outcome_channel();
//! let _ = hive.swarm_send((0..10).map(|i: i32| Thunk::of(move || i * i)), tx);
//! assert_eq!(285, rx.into_outputs().take(10).sum());
//!
//! // return results as an iterator...
//! let total = hive
//!     .swarm((0..10).map(|i: i32| Thunk::of(move || i * -i)))
//!     .into_outputs()
//!     .sum();
//! assert_eq!(-285, total);
//! # }
//! ```
//!
//! ### Parallelize a complex task using a stateful `Worker`
//!
//! Suppose you'd like to parallize executions of a line-delimited process, such as `cat`.
//! This requires defining a `struct` to hold the process `stdin` and `stdout`, and
//! implementing the `Worker` trait for this struct. We'll also use a custom `Queen` to keep track
//! of the [`Child`](https://doc.rust-lang.org/std/process/struct.Child.html) processes and make
//! sure they're terminated properly.
//!
//! ```
//! use drudge::hive::prelude::*;
//! use drudge::bee::prelude::*;
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
//!     fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
//!         self.write_char(input).map_err(|error| ApplyError::Fatal {input: Some(input), error})
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
//!         self.children.drain(..).map(|mut child| child.wait()).collect()
//!     }
//! }
//!
//! impl Queen for CatQueen {
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
//!                 Ok(status) => eprintln!("Child process failed: {}", status),
//!                 Err(e) => eprintln!("Error waiting for child process: {}", e),
//!             }
//!         })
//!     }
//! }
//!
//! # fn main() {
//! // build the Hive
//! let hive = Builder::new()
//!     .num_threads(4)
//!     .build_default::<CatQueen>();
//!
//! // prepare inputs
//! let inputs: Vec<u8> = (0..8).map(|i| 97 + i).collect();
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
//! // shutdown the hive, use the Queen to wait on child processes, and report errors
//! let (mut queen, _) = hive.into_husk().into_parts();
//! let (wait_ok, wait_err): (Vec<_>, Vec<_>) =
//!     queen.wait_for_all().into_iter().partition(Result::is_ok);
//! if !wait_err.is_empty() {
//!     panic!(
//!         "Error(s) occurred while waiting for child processes: {:?}",
//!         wait_err
//!     );
//! }
//! let exec_err_codes: Vec<_> = wait_ok
//!     .into_iter()
//!     .map(Result::unwrap)
//!     .filter(|status| !status.success())
//!     .map(|status| status.code())
//!     .flatten()
//!     .collect();
//! if !exec_err_codes.is_empty() {
//!     panic!("Child process(es) failed with exit codes: {:?}", exec_err_codes);
//! }
//! # }
//! ```

mod atomic;
pub mod bee;
mod boxed;
pub mod channel;
pub mod hive;
pub mod panic;
pub mod util;
