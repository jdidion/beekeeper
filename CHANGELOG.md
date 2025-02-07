# Beekeeper changelog

## 0.3.0

* **Breaking**
  * `beekeeper::bee::Context` now takes a generic parameter that must be input type of the `Worker`.
* Features
  * Added the `batching` feature, which enables worker threads to queue up batches of tasks locally, which can alleviate contention between threads in the pool, especially when there are many short-lived tasks.
  * Added the `Context::submit` method, which enables tasks to submit new tasks to the `Hive`.
* Other
  * Switched to using thread-local retry queues for the implementation of the `retry` feature, to reduce thread-contention.
  * Switched to storing `Outcome`s in the hive using a data structure that does not require locking when inserting, which should reduce thread contention when using `*_store` operations.

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