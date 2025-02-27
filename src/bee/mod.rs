#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
//! Traits for defining workers in the worker pool.
//!
//! A [`Hive`](crate::hive::Hive) is populated by bees:
//! * The [`Worker`]s process the tasks submitted to the `Hive`.
//! * The [`Queen`] creates a new `Worker` for each thread in the `Hive`.
//!
//! # Worker
//!
//! A worker is defined by implementing the [`Worker`] trait. A `Worker` implementation has three
//! associated types:
//! * `Input`: the type of the input to the worker.
//! * `Output`: the type of the output produced by the worker.
//! * `Error`: the type of error that can occur during the execution of the worker.
//!
//! Note that all of a `Worker`'s associated types must be [`Send`]; however, the `Worker` itself
//! will only ever exist within the context of a single worker thread, and thus does not itself
//! need to be `Send`.
//!
//! The `Worker` trait has a single method, [`apply`](crate::bee::Worker::apply), which
//! takes an input of type `Input` and a [`Context`] and returns a `Result` containing an either an
//! `Output` or an [`ApplyError`].
//!
//! The `Context` contains information about the task, including:
//! * The task ID. Each task submitted to a `Hive` is assigned an ID that is unique within
//!   that `Hive`.
//! * Whether the task has been cancelled: the user may request that all active tasks are cancelled,
//!   such as by calling [`Hive::suspend()`](crate::hive::Hive::suspend). A `Worker` is not
//!   required to handle cancellation, but for long-running tasks it is suggested that the worker
//!   periodically check the cancellation flag by calling
//!   [`Context::is_cancelled()`](crate::bee::context::Context::is_cancelled). If the cancellation
//!   flag is set, the worker may terminate early by returning [`ApplyError::Cancelled`].
//! * If the `retry` feature is enabled, the `Context` also contains the retry
//!   [`attempt`](crate::bee::context::Context::attempt), which starts at `0` the first time the task
//!   is attempted and increments by `1` for each subsequent retry attempt.
//!
//! If a fatal error occurs during processing of the task, the worker should return
//! [`ApplyError::Fatal`].
//!
//! If the task instead fails due to a transient error, the worker should return
//! [`ApplyError::Retryable`]. If the `retry` feature is enabled, then a task that fails with a
//! `ApplyError::Retryable` error will be retried, otherwise the error is converted to `Fatal`.
//!
//! A `Worker` should not panic. However, if it must execute code that may panic, it can do so
//! within a closure passed to [`Panic::try_call`](crate::panic::Panic::try_call) and convert an
//! `Err` result to an [`ApplyError::Panic`]. In the worst-case scenario, if a worker fails with an
//! uncaught panic, the worker thread will terminate and the `Hive` will spawn a new worker thread;
//! however, the input on which the worker failed will be irretrievably lost.
//!
//! As an alternative to implementing the `Worker` trait, you may instead implement
//! [`RefWorker`], which is similar to `Worker`, with the following differences:
//! * You implement [`apply_ref`](crate::bee::RefWorker::apply_ref) instead of `apply`.
//! * The `apply_ref` method takes a reference to the input rather than an owned value.
//! * The `apply_ref` method returns a `Result` containing an either an `Output` or a
//!   [`ApplyRefError`].
//! * You do not need to catch panics - the blanket implementation of `Worker::apply` for
//!   `RefWorker` calls `apply_ref` within a `Panic::try_call` closure and automatically handles the
//!   result.
//!
//! ## Stock Workers
//!
//! The [`stock`] submodule provides some commonly used worker implementations:
//! * [`Caller`](crate::bee::stock::Caller): a worker that wraps a callable (function or closure)
//!   with a single input parameter of type `Input` (i.e., the worker's associated `Input` type)
//!   and an output of type `Output`.
//!     * A [`OnceCaller`](crate::bee::stock::OnceCaller) is like `Caller`, but it may also return
//!       an error, which is always considered fatal.
//!     * A [`RefCaller`](crate::bee::stock::RefCaller) is like `OnceCaller`, except that it passes
//!       an `&Input` to its wrapped callable. The benefit of using `RefCaller` is that the input
//!       can be recovered if there is an error.
//!     * [`RetryCaller`](crate::bee::stock::RetryCaller) is like `OnceCaller`, but its error type
//!       is `ApplyError`, which enables transient errors to be retried (when the `retry` feature
//!       is enabled).
//! * [`ThunkWorker`](crate::bee::stock::ThunkWorker): a worker that processes
//!   [`Thunk`](crate::bee::stock::Thunk)s, which are no-argument callables (functions or closures)
//!   with a common return type.
//!     * [`FunkWorker`](crate::bee::stock::FunkWorker) is like `ThunkWorker` except that it
//!       processes fallible thunks (`Funk`s), which also have a common error type.
//!     * [`PunkWorker`](crate::bee::stock::PunkWorker) is like `ThunkWorker` except that it
//!       processes thunks that may panic (`Punk`s).
//! * [`EchoWorker`](crate::bee::stock::EchoWorker): simply returns its input. This is primarily
//!   useful for testing.
//!
//! # Queen
//!
//! A queen is defined by implementing the [`Queen`] trait. A single `Queen` instance is used to
//! create the `Worker` instances for each worker thread in a `Hive`.
//!
//! It is often not necessary to manually implement the `Queen` trait. For exmaple, if your `Worker`
//! implements `Default`, then you can use [`DefaultQueen`] implicitly by calling
//! [`Builder::build_with_default`](crate::hive::Builder::build_with_default). Similarly,
//! if your `Worker` implements `Clone`, then you can use [`CloneQueen`]
//! implicitly by calling [`Builder::build_with`](crate::hive::Builder::build_with).
//!
//! A `Queen` should never panic when creating `Worker`s.
//!
//! # Implementation Notes
//!
//! It is easiest to use the [`prelude`] when implementing your bees:
//!
//! ```
//! use beekeeper::bee::prelude::*;
//! ```
//!
//! Note that both `Queen::create()` and `Worker::apply()` receive `&mut self`, meaning that they
//! can modify their own state.
//!
//! The state of a `Hive`'s `Queen` may be interrogated either
//! [during](crate::hive::Hive::queen) or [after](crate::hive::Hive::try_into_husk) the
//! life of the `Hive`. However, `Worker`s may never be accessed directly. Thus, it is often
//! more appropriate to use synchronized types (`Arc`, `Mutex`, etc.) to share state between
//! workers, the queen, and/or the client thread(s).
mod context;
mod error;
mod queen;
pub mod stock;
mod worker;

pub use self::context::{Context, LocalContext, TaskId, TaskMeta};
pub use self::error::{ApplyError, ApplyRefError};
pub use self::queen::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut};
pub use self::worker::{RefWorker, RefWorkerResult, Worker, WorkerError, WorkerResult};

pub mod prelude {
    pub use super::{
        ApplyError, ApplyRefError, Context, Queen, QueenCell, QueenMut, RefWorker, RefWorkerResult,
        Worker, WorkerError, WorkerResult,
    };
}
