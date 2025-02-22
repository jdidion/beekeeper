//! There are a few different builder types.
//!
//! * Open: has no type parameters; can only set config parameters. Has methods to create
//!   typed builders.
//! * Bee-typed: has type parameters for the `Worker` and `Queen` types.
//! * Queue-typed: builder instances that are specific to the `TaskQueues` type.
//! * Fully-typed: builder that has type parameters for the `Worker`, `Queen`, and `TaskQueues`
//!   types. This is the only builder with a `build` method to create a `Hive`.
//!
//! Generic - Queue
//!    |    /
//!   Bee  /
//!    |  /
//!   Full
//!
//! All builders implement the `BuilderConfig` trait, which provides methods to set configuration
//! parameters. The configuration options available:
//! * [`Builder::num_threads`]: number of worker threads that will be spawned by the built `Hive`.
//!     * [`Builder::with_default_num_threads`] will set `num_threads` to the global default value.
//!     * [`Builder::with_thread_per_core`] will set `num_threads` to the number of available CPU
//!       cores.
//! * [`Builder::thread_name`]: thread name for each of the threads spawned by the built `Hive`. By
//!   default, threads are unnamed.
//! * [`Builder::thread_stack_size`]: stack size (in bytes) for each of the threads spawned by the
//!   built `Hive`. See the
//!   [`std::thread`](https://doc.rust-lang.org/stable/std/thread/index.html#stack-size)
//!   documentation for details on the default stack size.
//!
//! The following configuration options are available when the `retry` feature is enabled:
//! * [`Builder::max_retries`]: maximum number of times a `Worker` will retry an
//!   [`ApplyError::Retryable`](crate::bee::ApplyError#Retryable) before giving up.
//! * [`Builder::retry_factor`]: [`Duration`](std::time::Duration) factor for exponential backoff
//!   when retrying an `ApplyError::Retryable` error.
//! * [`Builder::with_default_retries`] sets the retry options to the global defaults, while
//!   [`Builder::with_no_retries`] disabled retrying.
//!
//! The following configuration options are available when the `affinity` feature is enabled:
//! * [`Builder::core_affinity`]: List of CPU core indices to which the threads should be pinned.
//!     * [`Builder::with_default_core_affinity`] will set the list to all CPU core indices, though
//!       only the first `num_threads` indices will be used.
//!
mod bee;
mod full;
mod open;
mod queue;

pub use bee::BeeBuilder;
pub use full::FullBuilder;
pub use open::OpenBuilder;
pub use queue::TaskQueuesBuilder;
pub use queue::channel::ChannelBuilder;
pub use queue::workstealing::WorkstealingBuilder;

pub fn open(with_defaults: bool) -> OpenBuilder {
    if with_defaults {
        OpenBuilder::default()
    } else {
        OpenBuilder::empty()
    }
}

pub fn channel(with_defaults: bool) -> ChannelBuilder {
    if with_defaults {
        ChannelBuilder::default()
    } else {
        ChannelBuilder::empty()
    }
}

pub fn workstealing(with_defaults: bool) -> WorkstealingBuilder {
    if with_defaults {
        WorkstealingBuilder::default()
    } else {
        WorkstealingBuilder::empty()
    }
}

use crate::hive::inner::{BuilderConfig, Token};

// #[cfg(all(test, feature = "affinity"))]
// mod affinity_tests {
//     use super::{OpenBuilder, Token};
//     use crate::hive::cores::Cores;

//     #[test]
//     fn test_with_affinity() {
//         let mut builder = OpenBuilder::empty();
//         builder = builder.with_default_core_affinity();
//         assert_eq!(builder.config(Token).affinity.get(), Some(Cores::all()));
//     }
// }

// #[cfg(all(test, feature = "batching"))]
// mod batching_tests {
//     use super::OpenBuilder;
// }

// #[cfg(all(test, feature = "retry"))]
// mod retry_tests {
//     use super::OpenBuilder;
//     use std::time::Duration;
// }
