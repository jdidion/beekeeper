//! There are a few different builder types. All builders implement the `BuilderConfig` trait,
//! which provides methods to set configuration parameters.
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
mod bee;
mod channel;
mod full;
mod open;
mod workstealing;

pub use bee::BeeBuilder;
pub use channel::ChannelBuilder;
pub use full::FullBuilder;
pub use open::OpenBuilder;
pub use workstealing::WorkstealingBuilder;

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
