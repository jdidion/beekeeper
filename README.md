<img src="assets/logo.png" alt="logo" width="200"/>

[![CI](https://github.com/jdidion/beekeeper/actions/workflows/ci.yml/badge.svg)](https://github.com/jdidion/beekeeper/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/jdidion/beekeeper/graph/badge.svg?token=GKFXRVO6WS)](https://codecov.io/github/jdidion/beekeeper)
[![crates.io](https://img.shields.io/crates/v/beekeeper.svg)](https://crates.io/crates/beekeeper)
[![docs.rs](https://docs.rs/beekeeper/badge.svg)](https://docs.rs/beekeeper)

# Beekeeper

<!-- cargo-rdme start -->

A Rust library that provides a [thread pool](https://en.wikipedia.org/wiki/Thread_pool)
implementation designed to execute the same operation in parallel on any number of inputs (this
is sometimes called a "worker pool").

### Overview

* Operations are defined by implementing the [`Worker`](https://docs.rs/beekeeper/latest/beekeeper/bee/worker/trait.Worker.html) trait.
* A [`Builder`](https://docs.rs/beekeeper/latest/beekeeper/hive/builder/trait.Builder.html) is used to configure and create a worker pool called a [`Hive`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html).
* `Hive` is generic over
    * The type of [`Queen`](https://docs.rs/beekeeper/latest/beekeeper/bee/queen/trait.Queen.html) which creates `Worker` instances
    * The type of [`TaskQueues`](https://docs.rs/beekeeper/latest/beekeeper/hive/trait.TaskQueues.html), which provides the global and worker thread-local queues for managing tasks
* Currently, two `TaskQueues` implementations are available:
    * Channel: uses a [`crossbeam`](https://github.com/crossbeam-rs/crossbeam) channel to send tasks from the `Hive` to worker threads
        * When the `local-batch` feature is enabled, local batch queues are implemented using [`crossbeam_queue::ArrayQueue`](https://docs.rs/crossbeam/latest/crossbeam/queue/struct.ArrayQueue.html)
    * Workstealing: A [`crossbeam_dequeue::Injector`](https://docs.rs/crossbeam-deque/latest/crossbeam_deque/struct.Injector.html) is used to submit tasks and serves as a global queue. Worker threads each have their own local queue and can take tasks either from the global queue or steal from other workers' local queues if their own queue is empty. This is a good choice for workloads that are either highly variable from task to task (in terms of processing time), or are fork-join in nature (i.e., tasks that submit sub-tasks).
* The `Hive` creates a `Worker` instance for each thread in the pool.
* Each thread in the pool continually:
    * Receives a task from an input queue,
    * Calls its `Worker`'s [`apply`](https://docs.rs/beekeeper/latest/beekeeper/bee/worker/trait.Worker.html#method.apply) method on the input, and
    * Produces an [`Outcome`](https://docs.rs/beekeeper/latest/beekeeper/hive/outcome/outcome/enum.Outcome.html).
* Depending on which of `Hive`'s methods are called to submit a task (or batch of tasks), the
  `Outcome`(s) may be returned as an `Iterator`, sent to an output `channel`, or stored in the
  `Hive` for later retrieval.
* A `Hive` may create `Worker`s may in one of three ways:
    * Call the `default()` function on a `Worker` type that implements [`Default`](std::default::Default)
    * Clone an instance of a `Worker` that implements [`Clone`](std::clone::Clone)
    * Call the [`create()`](https://docs.rs/beekeeper/latest/beekeeper/bee/queen/trait.Queen.html#method.create) method on a worker factory that implements the [`Queen`](https://docs.rs/beekeeper/latest/beekeeper/bee/queen/trait.Queen.html) trait.
* A `Worker`s may be stateful, i.e., `Worker::apply()` takes a `&mut self`
* While `Queen` is not stateful, [`QueenMut`](https://docs.rs/beekeeper/latest/beekeeper/bee/queen/trait.QueenMut.html) may be (i.e., it's `create()` method takes a `&mut self`)
* Although it is strongly recommended to avoid `panic`s in worker threads (and thus, within
  `Worker` implementations), the `Hive` does automatically restart any threads that panic.
* A `Hive` may be [`suspend`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.suspend)ed and [`resume`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.resume)d at any time. When a `Hive` is suspended, worker threads do no work and tasks accumulate in the input queue.
* Several utility functions are provided in the [util](https://docs.rs/beekeeper/latest/beekeeper/util/) module. Notably, the `map` and `try_map` functions enable simple parallel processing of a single batch of tasks.
* Several useful `Worker` implementations are provided in the [stock](https://docs.rs/beekeeper/latest/beekeeper/bee/stock/) module. Most notable are those in the [`call`](https://docs.rs/beekeeper/latest/beekeeper/bee/stock/call/) submodule, which provide different ways of wrapping `callable`s, i.e., closures and function pointers.
* The following optional features are provided via feature flags:
    * `affinity`: worker threads may be pinned to CPU cores to minimize the overhead of context-switching.
    * `local-batch` (>=0.3.0): worker threads take batches of tasks from the global input queue and add them to a local queue, which may alleviate thread contention, especially when there are many short-lived tasks.
        * Tasks may be [`Weighted`](https://docs.rs/beekeeper/latest/beekeeper/hive/weighted/struct.Weighted.html) to enable balancing unevenly sized tasks between worker threads.
    * `retry`: Tasks that fail due to transient errors (e.g., temporarily unavailable resources)
      may be retried a set number of times, with an optional, exponentially increasing delay
      between retries.
    * Several alternative `channel` implementations are supported:
        * [`crossbeam`](https://docs.rs/crossbeam/latest/crossbeam/)
        * [`flume`](https://github.com/zesterer/flume)
        * [`loole`](https://github.com/mahdi-shojaee/loole)

### Usage

To parallelize a task, you'll need two things:
1. A `Worker` implementation. Your options are:
    * Use an existing implementation from the [stock](https://docs.rs/beekeeper/latest/beekeeper/bee/stock/) module (see Example 2 below)
    * Implement your own (See Example 3 below)
        * `use` the necessary traits (e.g., `use beekeeper::bee::prelude::*`)
        * Define a `struct` for your worker
        * Implement the `Worker` trait on your struct and define the `apply` method with the logic of your task
        * Do at least one of the following:
            * Implement `Default` for your worker
            * Implement `Clone` for your worker
            * Create a custom worker fatory that implements the `Queen` or `QueenMut` trait
2. A `Hive` to execute your tasks. Your options are:
    * Use one of the convenience methods in the [util](https://docs.rs/beekeeper/latest/beekeeper/util/) module (see Example 1 below)
    * Create a `Hive` manually using a [`Builder`](https://docs.rs/beekeeper/latest/beekeeper/hive/builder/trait.Builder.html) (see Examples 2 and 3 below)
        * [`OpenBuilder`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html) is the most general builder
        * [`OpenBuilder::new()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.new) creates an empty `OpenBuilder`
        * [`Builder::default()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.default) creates a `OpenBuilder`
          with the global default settings (which may be changed using the functions in the
          [`hive`](https://docs.rs/beekeeper/latest/beekeeper/hive/) module, e.g., `beekeeper::hive::set_num_threads_default(4)`).
        * The builder must be specialized for the `Queen` and `TaskQueues` types:
            * If you have a `Worker` that implements `Default`, use [`with_worker_default::<MyWorker>()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_worker_default)
            * If you have a `Worker` that implements `Clone`, use [`with_worker(MyWorker::new())`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_worker)
            * If you have a custom `Queen`, use [`with_queen_default::<MyQueen>()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_queen_default) if it implements `Default`, otherwise use [`with_queen(MyQueen::new())`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_queen)
            * If you have a custom `QueenMut`, use [`with_queen_mut_default::<MyQueenMut>()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_queen_mut_default) if it implements `Default`, otherwise use [`with_queen_mut(MyQueenMut::new())`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_queen_mut)
            * Use the [`with_channel_queues`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_channel_queues.html) or [`with_workstealing_queues`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.OpenBuilder.html#method.with_workstealing_queues.html) to configure the `TaskQueues` implementation
        * Use the `build()` methods to build the `Hive`
        * Note that [`Builder::num_threads()`](https://docs.rs/beekeeper/latest/beekeeper/hive/builder/struct.Builder.html#method.num_threads) must be set to a non-zero value, otherwise the built `Hive` will not start any worker threads until you call the [`Hive::grow()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.grow) method.

Once you've created a `Hive`, use its methods to submit tasks for processing. There are
four groups of methods available:
* `apply`: submits a single task
* `swarm`: submits a batch of tasks given a collection of inputs with known size (i.e., anything
  that implements `IntoIterator<IntoIter: ExactSizeIterator>`)
* `map`: submits an arbitrary batch of tasks (i.e., anything that implements `IntoIterator`)
* `scan`: Similar to `map`, but you also provide 1) an initial value for a state variable, and
  1) a function that transforms each item in the input iterator into the input type required by
  the `Worker`, and also has access to (and may modify) the state variable.

There are multiple methods in each group that differ by how the task results (called
`Outcome`s) are handled:
* The unsuffixed methods return an `Iterator` over the `Outcome`s in the same order as the inputs
  (or, in the case of `apply`, a single `Outcome`)
* The methods with the `_unordered` suffix instead return an unordered iterator, which may be
  more performant than the ordered iterator
* The methods with the `_send` suffix accept a channel `Sender` and send the `Outcome`s to that
  channel as they are completed (see this [note](https://docs.rs/beekeeper/latest/beekeeper/hive/index.html#outcome-channels)).
* The methods with the `_store` suffix store the `Outcome`s in the `Hive`; these may be
  retrieved later using the [`Hive::take_stored()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.take_stored) method, using
  one of the `remove*` methods (which requires
  [`OutcomeStore`](https://docs.rs/beekeeper/latest/beekeeper/hive/outcome/store/trait.OutcomeStore.html) to be in scope), or by
  using one of the methods on `Husk` after shutting down the `Hive` using
  [`Hive::try_into_husk()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.try_into_husk).

When using one of the `_send` methods, you should ensure that the `Sender` is dropped after
all tasks have been submitted, otherwise calling `recv()` on (or iterating over) the `Receiver`
will block indefinitely.

Within a `Hive`, each submitted task is assinged a unique ID. The `_send` and `_store`
methods return the task_ids of the submitted tasks, which can be used to retrieve them later
(e.g., using [`Hive::remove()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.remove)).

After submitting tasks, you may use the [`Hive::join()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.join) method to wait
for all tasks to complete. Using `join` is strongly recommended when using one of the `_store`
methods, otherwise you'll need to continually poll the `Hive` to check for completed tasks.

When you are finished with a `Hive`, you may simply drop it (either explicitly, or by letting
it go out of scope) - the worker threads will be terminated automatically. If you used the
`_store` methods and would like to have access to the stored task `Outcome`s after the `Hive`
has been dropped, and/or you'd like to re-use the `Hive's` `Queen` or other configuration
parameters, you can use the [`Hive::try_into_husk()`](https://docs.rs/beekeeper/latest/beekeeper/hive/struct.Hive.html#method.try_into_husk) method to extract
the relevant data from the `Hive` into a [`Husk`](https://docs.rs/beekeeper/latest/beekeeper/hive/husk/struct.Husk.html) object.

### Examples

#### 1. Parallelize an existing function

```rust
pub fn double(i: usize) -> usize {
  i * 2
}

// parallelize the computation of `double` on a range of numbers
// over 4 threads, and sum the results
const N: usize = 100;
let sum_doubles: usize = beekeeper::util::map(4, 0..N, double)
    .into_iter()
    .sum();
println!("Sum of {} doubles: {}", N, sum_doubles);
```

#### 2. Parallelize arbitrary tasks with the same output type

```rust
use beekeeper::bee::stock::{Thunk, ThunkWorker};
use beekeeper::hive::prelude::*;

// create a hive to process `Thunk`s - no-argument closures with the
// same return type (`i32`)
let hive = Builder::new()
    .num_threads(4)
    .thread_name("thunk_hive")
    .build_with_default::<ThunkWorker<i32>>()
    .unwrap();

// return results to your own channel...
let (tx, rx) = outcome_channel();
let _ = hive.swarm_send(
    (0..10).map(|i: i32| Thunk::from(move || i * i)),
    tx
);
assert_eq!(285, rx.into_outputs().take(10).sum());

// return results as an iterator...
let total = hive
    .swarm_unordered((0..10).map(|i: i32| Thunk::from(move || i * -i)))
    .into_outputs()
    .sum();
assert_eq!(-285, total);
```

#### 3. Parallelize a complex task using a stateful `Worker`

Suppose you'd like to parallelize executions of a line-delimited process, such as `cat`.
This requires defining a `struct` to hold the process `stdin` and `stdout`, and
implementing the `Worker` trait for this struct. We'll also use a custom `Queen` to keep track
of the [`Child`](https://doc.rust-lang.org/stable/std/process/struct.Child.html) processes and make sure they're terminated properly.

```rust
use beekeeper::bee::prelude::*;
use beekeeper::hive::prelude::*;
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};

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

    fn apply(
        &mut self,
        input: Self::Input,
        _: &Context<Self::Input>
    ) -> WorkerResult<Self> {
        self.write_char(input).map_err(|error| {
            ApplyError::Fatal { input: Some(input), error }
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
        self.wait_for_all().into_iter().for_each(|result| {
            match result {
                Ok(status) if status.success() => (),
                Ok(status) => {
                    eprintln!("Child process failed: {}", status);
                }
                Err(e) => {
                    eprintln!("Error waiting for child process: {}", e);
                }
            }
        })
    }
}

// build the Hive
let hive = Builder::new()
    .num_threads(4)
    .build_default_mut::<CatQueen>()
    .unwrap();

// prepare inputs
let inputs = (0..8).map(|i| 97 + i);

// execute tasks and collect outputs
let output = hive
    .swarm(inputs)
    .into_outputs()
    .fold(String::new(), |mut a, b| {
        a.push_str(&b);
        a
    })
    .into_bytes();

// verify the output - note that `swarm` ensures the outputs are in
// the same order as the inputs
assert_eq!(output, b"abcdefgh");

// shutdown the hive, use the Queen to wait on child processes, and
// report errors
let (mut queen, _outcomes) = hive.try_into_husk().unwrap().into_parts();
let (wait_ok, wait_err): (Vec<_>, Vec<_>) = queen
    .into_inner()
    .wait_for_all()
    .into_iter()
    .partition(Result::is_ok);
if !wait_err.is_empty() {
    panic!(
        "Error(s) occurred while waiting for child processes: {:?}",
        wait_err
    );
}
let exec_err_codes: Vec<_> = wait_ok
    .into_iter()
    .map(Result::unwrap)
    .filter_map(|status| (!status.success()).then(|| status.code()))
    .flatten()
    .collect();
if !exec_err_codes.is_empty() {
    panic!(
        "Child process(es) failed with exit codes: {:?}",
        exec_err_codes
    );
}
```

<!-- cargo-rdme end -->

## Status

Early versions of this crate (< 0.3) had some fatal design flaws that needed to be corrected with breaking changes (see the [changelog](CHANGELOG.md)).

As of version 0.3, the `beekeeper` API is generally considered to be stable, but additional real-world battle-testing is desired before promoting the version to `1.0.0`. If you identify bugs or have suggestions for improvement, please [open an issue](https://github.com/jdidion/beekeeper/issues).

## Similar libraries

* [workerpool](https://docs.rs/workerpool/latest/workerpool/)
* [threadpool](http://github.com/rust-threadpool/rust-threadpool)
* [rust-scoped-pool](http://github.com/reem/rust-scoped-pool)
* [scoped-threadpool-rs](https://github.com/Kimundi/scoped-threadpool-rs)
* [executors](https://github.com/Bathtor/rust-executors)
* [rayon](https://docs.rs/rayon/latest/rayon/struct.ThreadPool.html)

## License

You may choose either of the following licenses:

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Credits

Beekeeper began as a fork of [workerpool](https://docs.rs/workerpool/latest/workerpool/).

The [logo](assets/logo.png) was generated using [DeepAI](https://deepai.org/styles).
