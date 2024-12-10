# Drudge

[![CI](https://github.com/jdidion/drudge/actions/workflows/ci.yaml/badge.svg)](https://github.com/jdidion/drudge/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/drudge.svg)](https://crates.io/crates/drudge)
[![docs.rs](https://docs.rs/drudge/badge.svg)](https://docs.rs/drudge)

A worker threadpool used to execute a number of tasks atop stateful workers in parallel. It spawns a specified number of worker threads and replenishes the pool if a process thread panics.

This is a fork of the `workerpool` crate with the following differences:

* A `Worker` does not need to implement `Default`. Instead, a `Queen` is used to create `Worker`s, and there are implementations of `Queen` provided for `Worker` types that implement `Default` or `Clone`.
* A `Worker` may have mutable state (i.e., the `Worker::try_it` function takes `&mut self` rather than `&self`).
* A `Worker` may return an error rather than panic.
* Panics within a `Worker` execution are trapped and sent to the caller.
* Functions that do not provide a `Sender` for the `Worker` output instead store the output in the `Hive`'s shared memory for later recovery.

A single `Worker` runs in its own thread, to be implemented according to the trait:

```rust
pub trait Worker: Debug + Sized + 'static {
    type Input: Send;
    type Output: Send;
    type Error: Send + Debug;

    fn try_it(
        &mut self,
        input: Self::Input,
        _: &Context,
    ) -> Result<Self::Output, WorkerError<Self>>;
}
```

## Usage

```toml
[dependencies]
drudge = "0.1.0"
```

To enable thread pinning (via the [core_affinity](https://docs.rs/core_affinity/latest/core_affinity/) crate), enable the `affinity` feature:

```toml
[dependencies]
drudge = { version = "0.1.0", features = ["affinity"] }
```

By default, this crate uses `std::sync::mpsc` channels for communicating results from the `Hive` to a consumer. To use an alternative channel implementation, enable *exactly one* of the following features:

* `crossbeam`
* `flume`
* `kanal`
* `loole`

This crate provides `Hive<W: Worker, Q: Queen>`.

To create a `Hive`, use a `Builder` and set the necessary options. `Builder::default()` creates a `Hive` with all available threads and no thread pinning. There are multiple `build*` funcitons depending on the traits that `Worker` implements.

`Hive` has four groups of functions for executing tasks:
- `apply`: submits a single task to the hive.
- `map`: submits an arbitrary-sized batch (an `Iterator`) of tasks to the hive.
- `swarm`: submits a batch of tasks to the hive, where the size of the batch is known (i.e., it implements `IntoIterator<IntoIter = ExactSizeIterator>`).
- `scan`: like map/swarm, but also takes a state value and a function; the function takes the state value and an item from the batch and returns an input that is sent to the hive for processing.

Each group of functions has multiple variants.
* The functions that end with `_send` all take a channel sender as a second argument and will deliver results to that channel as they become available.
* The functions that end with `_store` are all non-blocking functions that return the indices associated with the submitted tasks and will store the task results in the hive. The results can be retrieved from the hive later by index, e.g. using `remove_success`. Note that, since these functions are non-blocking, it is necessary to call `hive.join` or otherwise prevent the `Hive` from being `drop`ped until the tasks are completed.
* For executing single tasks, there is `try_apply`, which submits the tasks and blocks waiting for the result.
* For executing batches of tasks, there are `map`/`swarm`/`scan`, which return an iterator yields results in the same order they were submitted. There are `_unordered` versions of the same functions that yield results as they become available.

Other functions of interest:
- `hive.join()` _blocking_ waits for all tasks to complete.

Several example workers are provided in `drudge::utils`:
- A stateless `ThunkWorker<O, E>`, which executes on inputs of `Thunk<T: Result<O, E>>` - effectively argumentless functions that are `Sized + Send`. These thunks are creates by wrapping functions (`FnOnce() -> Result<T, E>`) with `Thunk::of`.
  - There is also `ThunkWorker<T>` for `Thunk<T>`s
- A `Func<I, O, E>`, which wraps a function pointer `fn(I) -> Result<O, E>`.
  - There is also `InfallibleFunc<I, O>`, which wraps a function pointer `fn(I) -> O`.
- `Identity<T>`, which simply returns the input value.

```rust
use drudge::client_api::*;
use drudge::utils::{InfallibleThunk, ThunkWorker};
use std::sync::mpsc;

fn main() {
    let n_workers = 4;
    let n_tasks = 8;
    let hive = Builder::new().num_threads(n_workers).build_default::<ThunkWorker<i32>>();

    // use your own channel...
    let (tx, rx) = mpsc::channel();
    let _ = hive.swarm_send((0..n_tasks).map(|i| Thunk::of(move || i * i)), tx);
    assert_eq!(140, rx.into_results().into_outputs().take(n_tasks as usize).sum());

    // or use ours
    let total = hive
        .swarm((0..n_tasks)
        .map(|i| Thunk::of(move || i * i)))
        .into_outputs()
        .sum();
    assert_eq!(140, total);
}
```

For stateful workers, you have to implement `Worker` yourself.

Suppose there's a line-delimited process, such as `cat` or `tr`, which you'd like running on many threads for use in a pool-like manner. You may create and use a worker, with maintained state of the stdin/stdout for the process, as follows:

```rust
 use drudge::{impl_api::*, user_api::*};
 use std::io::prelude::*;
 use std::io::{self, BufReader};
 use std::process::{Command, ChildStdin, ChildStdout, Stdio};

 #[derive(Debug)]
 struct LineDelimitedWorker {
     stdin: ChildStdin,
     stdout: BufReader<ChildStdout>,
 }

 impl Default for LineDelimitedWorker {
     fn default() -> Self {
         let child = Command::new("cat")
             .stdin(Stdio::piped())
             .stdout(Stdio::piped())
             .stderr(Stdio::inherit())
             .spawn()
             .unwrap();
         Self {
             stdin: child.stdin.unwrap(),
             stdout: BufReader::new(child.stdout.unwrap()),
         }
     }
 }

impl LineDelimitedWorker {
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

 impl Worker for LineDelimitedWorker {
     type Input = u8;
     type Output = String;
     type Error = io::Error;

     fn appply(&mut self, input: Self::Input) -> WorkerResult<Self> {
         self.write_char(input).map_err(ApplyError::NotRetryable)
     }
 }

 fn main() {
    let n_workers = 4;
    let n_tasks = 8;
    let hive = Builder::new()
        .num_threads(n_workers)
        .build_default::<LineDelimitedWorker>();

    let inputs: Vec<u8> = (0..n_tasks).map(|i| 97 + i).collect();
    let output = hive
        .swarm(inputs)
        .fold(String::new(), |mut a, b| {
            a.push_str(&b.unwrap());
            a
        })
        .into_bytes();
    assert_eq!(output, b"abcdefgh");
 }
```

## Similar libraries

* [workerpool]()
* [threadpool](http://github.com/rust-threadpool/rust-threadpool)
* [rust-scoped-pool](http://github.com/reem/rust-scoped-pool)
* [scoped-threadpool-rs](https://github.com/Kimundi/scoped-threadpool-rs)
* [crossbeam](https://github.com/aturon/crossbeam)

## License

This work is derivative of
[workerpool](http://github.com/rust-threadpool/rust-threadpool).

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
