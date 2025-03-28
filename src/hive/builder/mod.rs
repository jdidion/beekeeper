//! There are a few different builder types.
//!
//! * Open: has no type parameters; can only set config parameters. Has methods to create
//!   typed builders.
//! * Bee-typed: has type parameters for the `Worker` and `Queen` types.
//! * Queue-typed: builder instances that are specific to the `TaskQueues` type.
//! * Fully-typed: builder that has type parameters for the `Worker`, `Queen`, and `TaskQueues`
//!   types. This is the only builder with a `build` method to create a `Hive`.
//!
//! All builders implement the `Builder` trait, which provides methods to set configuration
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
//! The following configuration options are available when the `affinity` feature is enabled:
//! * [`Builder::core_affinity`]: List of CPU core indices to which the threads should be pinned.
//!     * [`Builder::with_default_core_affinity`] will set the list to all CPU core indices, though
//!       only the first `num_threads` indices will be used.
//!
//! The following configuration options are available when the `local-batch` feature is enabled:
//! * [`Builder::batch_limit`]: Maximum number of tasks that can queued by a worker.
//! * [`Builder::weight_limit`]: Maximum "weight" of tasks that can be queued by a worker.
//! * [`Builder::with_default_batch_limit`] and [`Builder::with_default_weight_limit`] set the
//!   local-batch options to the global defaults, while [`Builder::with_no_local_batching`]
//!   disables local-batching.
//!
//! The following configuration options are available when the `retry` feature is enabled:
//! * [`Builder::max_retries`]: maximum number of times a `Worker` will retry an
//!   [`ApplyError::Retryable`](crate::bee::ApplyError#Retryable) before giving up.
//! * [`Builder::retry_factor`]: [`Duration`](std::time::Duration) factor for exponential backoff
//!   when retrying an `ApplyError::Retryable` error.
//! * [`Builder::with_default_max_retries`] and [`Builder::with_default_retry_factor`] set the
//!   retry options to the global defaults, while [`Builder::with_no_retries`] disables retrying.
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

use crate::hive::inner::{Builder, BuilderConfig, Token};

/// Creates a new `OpenBuilder`. If `with_defaults` is `true`, the builder will be pre-configured
/// with the global defaults.
pub fn open(with_defaults: bool) -> OpenBuilder {
    if with_defaults {
        OpenBuilder::default()
    } else {
        OpenBuilder::empty()
    }
}

/// Creates a new `ChannelBuilder`. If `with_defaults` is `true`, the builder will be
/// pre-configured with the global defaults.
pub fn channel(with_defaults: bool) -> ChannelBuilder {
    if with_defaults {
        ChannelBuilder::default()
    } else {
        ChannelBuilder::empty()
    }
}
/// Creates a new `WorkstealingBuilder`. If `with_defaults` is `true`, the builder will be
/// pre-configured with the global defaults.
pub fn workstealing(with_defaults: bool) -> WorkstealingBuilder {
    if with_defaults {
        WorkstealingBuilder::default()
    } else {
        WorkstealingBuilder::empty()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::hive::Builder;
    use rstest::*;

    #[rstest]
    fn test_create<B: Builder, F: Fn(bool) -> B>(
        #[values(open, channel, workstealing)] builder_factory: F,
        #[values(true, false)] with_defaults: bool,
    ) {
        let mut builder = builder_factory(with_defaults)
            .num_threads(4)
            .thread_name("foo")
            .thread_stack_size(100);
        crate::hive::inner::builder_test_utils::check_builder(&mut builder);
    }
}
