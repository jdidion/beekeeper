mod builder;
mod config;
mod counter;
mod gate;
mod queue;
mod shared;
mod task;

pub mod set_config {
    #[cfg(feature = "batching")]
    pub use super::config::set_batch_size_default;
    pub use super::config::{reset_defaults, set_num_threads_default, set_num_threads_default_all};
    #[cfg(feature = "retry")]
    pub use super::config::{
        set_max_retries_default, set_retries_default_disabled, set_retry_factor_default,
    };
}

pub use self::builder::{Builder, BuilderConfig};
pub use self::queue::{ChannelTaskQueues, TaskQueues};

use self::counter::DualCounter;
use self::gate::{Gate, PhasedGate};
use self::queue::PopTaskError;
use crate::atomic::{AtomicAny, AtomicBool, AtomicOption, AtomicUsize};
use crate::bee::{Queen, TaskId, Worker};
use crate::hive::{OutcomeQueue, OutcomeSender, SpawnError};
use parking_lot::Mutex;
use std::thread::JoinHandle;

type Any<T> = AtomicOption<T, AtomicAny<T>>;
type Usize = AtomicOption<usize, AtomicUsize>;
#[cfg(feature = "retry")]
type U32 = AtomicOption<u32, crate::atomic::AtomicU32>;
#[cfg(feature = "retry")]
type U64 = AtomicOption<u64, crate::atomic::AtomicU64>;

/// Private, zero-size struct used to call private methods in public sealed traits.
pub struct Token;

/// Internal representation of a task to be processed by a `Hive`.
#[derive(Debug)]
pub struct Task<W: Worker> {
    id: TaskId,
    input: W::Input,
    outcome_tx: Option<OutcomeSender<W>>,
    #[cfg(feature = "retry")]
    attempt: u32,
}

/// Data shared by all worker threads in a `Hive`.
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
    #[cfg(feature = "batching")]
    batch_size: Usize,
    /// Maximum number of retries for a task
    #[cfg(feature = "retry")]
    max_retries: U32,
    /// Multiplier for the retry backoff strategy
    #[cfg(feature = "retry")]
    retry_factor: U64,
}
