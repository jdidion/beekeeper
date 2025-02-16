# Beekeeper changelog

## 0.3.0

The general theme of this release is performance improvement by eliminating thread contention due to unnecessary locking of shared state. This required making some breaking changes to the API.

* **Breaking**
  * `beekeeper::hive::Hive` type signature has changed
    * Removed the `W: Worker` parameter as it is redundant (can be obtained from `Q::Kind`)
    * Added `T: TaskQueues`to specify the `TaskQueues` implementation
  * The `Builder` interface has been re-written to enable maximum flexibility.
    * `Builder` is now a trait that must be in scope.
    * `ChannelBuilder` implements the previous builder functionality.
    * `OpenBuilder` has no type parameters and can be specialized to create a `Hive` with any combination of `Queen` and `TaskQueues`.
    * `BeeBuilder` and `FullBuilder` are intermediate types that generally should not be instantiated directly.
  * `beekeeper::bee::Queen::create` now takes `&self` rather than `&mut self`. There is a new type, `beekeeper::bee::QueenMut`, with a `create(&mut self)` method, and needs to be wrapped in a `beekeeper::bee::QueenCell` to implement the `Queen` trait. This enables the `Hive` to create new workers without locking in the case of a `Queen` that does not need mutable state.
  * `beekeeper::bee::Context` now takes a generic parameter that must be input type of the `Worker`.
* Features
  * Added the `TaskQueues` trait, which enables `Hive` to be specialized for different implementations of global (i.e., sending tasks from the `Hive` to worker threads) and local (i.e., worker thread-specific) queues.
    * `ChannelTaskQueues` implements the existing behavior, using a channel for sending tasks.
    * `WorkstealingTaskQueues` has been added to implement the workstealing pattern, based on `crossbeam::dequeue`.
  * Added the `batching` feature, which enables worker threads to queue up batches of tasks locally, which can alleviate contention between threads in the pool, especially when there are many short-lived tasks. This feature is only used by `ChannelTaskQueues`.
  * Added the `Context::submit` method, which enables tasks to submit new tasks to the `Hive`.
* Other
  * Switched to using thread-local retry queues for the implementation of the `retry` feature, to reduce thread-contention.
  * Switched to storing `Outcome`s in the hive using a data structure that does not require locking when inserting, which should reduce thread contention when using `*_store` operations.
  * Switched to using `crossbeam_channel` for the task input channel in `ChannelTaskQueues`.

## 0.2.1

* Bugfixes
  * Reverted accidental change to default features in Cargo.toml
  * Panics during drop of worker threads
* Other
  * Added initial performance benchmarks

## 0.2.0

* **Breaking**
  * `Builder::build*`, `Husk::into_hive*` now return `Hive` rather than `Result<Hive, SpawnError>`
  * `beekeeper::hive::SpawnError` has been removed
  * `Hive::grow` and `Hive::use_all_cores` now return `Result<usize, Poisoned>` rather than `usize`
  * `Hive::num_threads` has been renamed `Hive::max_workers`
* Features
  * `Hive` now keeps track of spawn results
    * `Hive::alive_workers` reports the number of worker threads that are currently alive (<= `max_workers`)
    * `Hive::has_dead_workers` returns `true` if the `Hive` has encountered any errors spawing worker threads
    * `Hive::revive_workers` attempts to re-spawn any dead worker threads
* Bugfixes
  * Ordered iterators would enter an infinite loop if there were missing indicies

## 0.1.0

* Initial release