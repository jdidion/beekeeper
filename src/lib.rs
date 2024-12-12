//! A [`Hive`] is a pool of threads used to execute a (possibly) stateful function in parallel on
//! any number of inputs.
//!
//! The [`Worker`] trait is used to define a function that can be executed in a `Hive`. A `Hive` is
//! created with a [`Queen`], which creates a `Worker` instance for each worker thread. The `Hive`
//! can be grown or shrunk dynamically. If a worker thread is terminated due to a panic, the `Hive`
//! automatically replenishes the pool.
//!
//! There are several optional features available:
//! * `affinity`: enable pinning worker threads to specific CPU cores.
//! * `crossbeam`: use `crossbeam`'s channels rather than [`std::sync::mpsc`] for the pool's task
//! management.
//! * `flume`: use `flume`'s channels rather than [`std::sync::mpsc`] for the pool's task management.
//! * `kanal`: use `kanal`'s channels rather than [`std::sync::mpsc`] for the pool's task management.
//! * `loole`: use `loole`'s channels rather than [`std::sync::mpsc`] for the pool's task management.
//!
//! # Examples
//!
//! Some `Worker` implementations and utility functions are provided in [`drudge::util`].
//! [`thunk_hive`] creates a `Hive` that executes [`Thunk<T>`]s, which are argumentless functions
//! that are `Sized + Send`. These `thunk`s are created by wrapping closures that return `T`
//! with [`Thunk::of`].
//!
//! ```
//! use drudge::hive::{Hive, Builder, OutcomeIteratorExt, outcome_channel};
//! use drudge::util::{self, Thunk, ThunkWorker};
//!
//! fn main() {
//!     let n_workers = 4;
//!     let n_tasks: usize = 8;
//!     let hive = Builder::default()
//!         .num_threads(n_workers)
//!         .build_with_default::<ThunkWorker<usize>>();
//!
//!     let (tx, rx) = outcome_channel();
//!     for i in 0..n_tasks {
//!         hive.apply_send(Thunk::of(move || i * i), tx.clone());
//!     }
//!
//!     let sum: usize = rx.take_outputs(n_tasks).sum();
//!     assert_eq!(140, sum);
//! }
//! ```
//!
//! For stateful workers, you have to implement [`Worker`] and [`Queen`] yourself, and use a
//! [`Builder`] to build the `Hive`. If your `Worker` implements `Default`, then you can use
//! `Builder::build_with_default` rather than manually implementing `Queen`. Alternatively, if your
//! `Worker` implements `Clone`, then you can use `Builder::build_with` with a `Worker` instance,
//! and new `Worker`s will be created by cloning it.
//!
//! Suppose there's a line-delimited process, such as `cat` or `tr`, which you'd like running on
//! many threads for use in a pool-like manner. You may create and use a `Worker`, with maintained
//! state of the stdin/stdout for the process, as follows:
//!
//! ```
//! use drudge::hive::Builder;
//! use drudge::task::{Context, RefWorker, RefWorkerResult};
//! use std::process::{Command, ChildStdin, ChildStdout, Stdio};
//! use std::io::prelude::*;
//! use std::io::{self, BufReader};
//!
//! #[derive(Debug)]
//! struct LineDelimitedWorker {
//!     stdin: ChildStdin,
//!     stdout: BufReader<ChildStdout>,
//! }
//!
//! impl Default for LineDelimitedWorker {
//!     fn default() -> Self {
//!         let child = Command::new("cat")
//!             .stdin(Stdio::piped())
//!             .stdout(Stdio::piped())
//!             .stderr(Stdio::inherit())
//!             .spawn()
//!             .unwrap();
//!         Self {
//!             stdin: child.stdin.unwrap(),
//!             stdout: BufReader::new(child.stdout.unwrap()),
//!         }
//!     }
//! }
//!
//! impl RefWorker for LineDelimitedWorker {
//!     type Input = u8;
//!     type Output = String;
//!     type Error = io::Error;
//!
//!     fn apply_ref(&mut self, input: &Self::Input, _: &Context) -> RefWorkerResult<Self> {
//!         self.stdin.write_all(&[*input])?;
//!         self.stdin.write_all(b"\n")?;
//!         self.stdin.flush()?;
//!         let mut s = String::new();
//!         self.stdout.read_line(&mut s)?;
//!         s.pop(); // exclude newline
//!         Ok(s)
//!     }
//! }
//!
//! # fn main() {
//! let n_workers = 4;
//! let n_tasks = 8;
//! let hive = Builder::new()
//!     .num_threads(n_workers)
//!     .build_with_default::<LineDelimitedWorker>();
//!
//! let inputs: Vec<u8> = (0..n_tasks).map(|i| 97 + i).collect();
//! let output = hive
//!     .swarm(inputs)
//!     .fold(String::new(), |mut a, b| {
//!         a.push_str(&b.unwrap());
//!         a
//!     })
//!     .into_bytes();
//! assert_eq!(output, b"abcdefgh");
//! # }
//! ```
//! [`Worker`]: task/trait.Worker.html
//! [`Queen`]: task/trait.Queen.html
//! [`Hive`]: hive/struct.Hive.html
//! [`Builder`]: hive/struct.Builder.html
//! [`drudge::util`]: util/
//! [`thunk_hive`]: util/fn.thunk_hive.html
//! [`ThunkWorker<T>`]: util/struct.ThunkWorker.html
//! [`Thunk::of`]: util/struct.Thunk.html#method.of
//! [`Thunk<T>`]: util/struct.Thunk.html
//! [`std::sync::mpsc`]: https://doc.rust-lang.org/stable/std/sync/mpsc/

mod boxed;
pub mod channel;
pub mod hive;
mod panic;
pub mod task;
pub mod util;

pub use panic::Panic;
