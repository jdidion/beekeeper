//! Internal data structures needed to implement `Hive`.
mod builder;
mod config;
mod counter;
mod gate;
mod queue;
mod shared;
mod task;

/// Prelude-like module that collects all the functions for setting global configuration defaults.
pub mod set_config {
    pub use super::config::{reset_defaults, set_num_threads_default, set_num_threads_default_all};
    #[cfg(feature = "local-batch")]
    pub use super::config::{set_batch_limit_default, set_weight_limit_default};
    #[cfg(feature = "retry")]
    pub use super::config::{
        set_max_retries_default, set_retries_default_disabled, set_retry_factor_default,
    };
}

// Note: it would be more appropriate for the publicly exported traits (`Builder`, `TaskQueues`)
// to be in the `beekeeper::hive` module, but they need to be in `inner` for visiblity reasons.

pub use self::builder::{Builder, BuilderConfig};
pub use self::queue::{ChannelTaskQueues, TaskQueues, WorkerQueues, WorkstealingTaskQueues};
pub use self::task::TaskInput;

use self::counter::DualCounter;
use self::gate::{Gate, PhasedGate};
use self::queue::PopTaskError;
use crate::atomic::{AtomicAny, AtomicBool, AtomicOption, AtomicUsize};
use crate::bee::{Queen, TaskMeta, Worker};
use crate::hive::{OutcomeQueue, OutcomeSender, SpawnError};
use parking_lot::Mutex;
use std::thread::JoinHandle;

type Any<T> = AtomicOption<T, AtomicAny<T>>;
type Usize = AtomicOption<usize, AtomicUsize>;
#[cfg(feature = "retry")]
type U8 = AtomicOption<u8, crate::atomic::AtomicU8>;
#[cfg(any(feature = "local-batch", feature = "retry"))]
type U64 = AtomicOption<u64, crate::atomic::AtomicU64>;

/// Private, zero-size struct used to call private methods in public sealed traits.
pub struct Token;

/// Internal representation of a task to be processed by a `Hive`.
#[derive(Debug)]
pub struct Task<W: Worker> {
    input: W::Input,
    meta: TaskMeta,
    outcome_tx: Option<OutcomeSender<W>>,
}

/// Data shared by all worker threads in a `Hive`. This is the private API used by the `Hive` and
/// worker threads to enqueue, dequeue, and process tasks.
pub struct Shared<Q: Queen, T: TaskQueues<Q::Kind>> {
    /// core configuration parameters
    config: Config,
    /// the `Queen` used to create new workers
    queen: Q,
    /// global and local task queues used by the `Hive` to send tasks to the worker threads
    task_queues: T,
    /// The results of spawning each worker
    spawn_results: Mutex<Vec<Result<JoinHandle<()>, SpawnError>>>,
    /// allows for 2^48 queued tasks and 2^16 active tasks
    num_tasks: DualCounter<48>,
    /// ID that will be assigned to the next task submitted to the `Hive`
    next_task_id: AtomicUsize,
    /// number of times a worker has panicked
    num_panics: AtomicUsize,
    /// number of `Hive` clones with a reference to this shared data
    num_referrers: AtomicUsize,
    /// whether the internal state of the hive is corrupted - if true, this prevents new tasks from
    /// processed (new tasks may be queued but they will never be processed); currently, this can
    /// only happen if the task counter somehow get corrupted
    poisoned: AtomicBool,
    /// whether the hive is suspended - if true, active tasks may complete and new tasks may be
    /// queued, but new tasks will not be processed
    suspended: AtomicBool,
    /// gate used by worker threads to wait until the hive is resumed
    resume_gate: Gate,
    /// gate used by client threads to wait until all tasks have completed
    join_gate: PhasedGate,
    /// outcomes stored in the hive
    outcomes: OutcomeQueue<Q::Kind>,
}

/// Core configuration parameters that are set by a `Builder`, used in a `Hive`, and preserved in a
/// `Husk`. Fields are `AtomicOption`s, which enables them to be transitioned back and forth
/// between thread-safe and non-thread-safe contexts.
#[derive(Clone, Debug)]
pub struct Config {
    /// Number of worker threads to spawn
    num_threads: Usize,
    /// Name to give each worker thread
    thread_name: Any<String>,
    /// Stack size for each worker thread
    thread_stack_size: Usize,
    /// CPU cores to which worker threads can be pinned
    #[cfg(feature = "affinity")]
    affinity: Any<crate::hive::cores::Cores>,
    /// Maximum number of tasks for a worker thread to take when receiving from the input channel
    #[cfg(feature = "local-batch")]
    batch_limit: Usize,
    /// Maximum "weight" of tasks a worker thread may have active and pending
    #[cfg(feature = "local-batch")]
    weight_limit: U64,
    /// Maximum number of retries for a task
    #[cfg(feature = "retry")]
    max_retries: U8,
    /// Multiplier for the retry backoff strategy
    #[cfg(feature = "retry")]
    retry_factor: U64,
}

#[cfg(test)]
pub(super) mod builder_test_utils {
    use super::*;

    pub fn check_builder<B: Builder>(builder: &mut B) {
        let config = builder.config_ref(Token);
        assert_eq!(config.num_threads.get(), Some(4));
        assert_eq!(config.thread_name.get(), Some("foo".into()));
        assert_eq!(config.thread_stack_size.get(), Some(100));
    }
}
