mod builder;
mod condvar;
mod config;
#[allow(clippy::module_inception)]
mod hive;
mod husk;
mod outcome;
//mod scoped;
mod shared;
mod task;

#[cfg(feature = "affinity")]
pub mod cores;
#[cfg(feature = "retry")]
mod delay;

pub mod prelude {
    pub use super::{
        outcome_channel, Builder, Hive, Husk, Outcome, OutcomeBatch, OutcomeDerefStore,
        OutcomeIteratorExt, OutcomeStore,
    };
}

pub use crate::channel::channel as outcome_channel;

pub use builder::Builder;
pub use config::{reset_defaults, set_num_threads_default, set_num_threads_default_all};
pub use husk::Husk;
pub use outcome::{Outcome, OutcomeBatch, OutcomeDerefStore, OutcomeIteratorExt, OutcomeStore};

#[cfg(feature = "retry")]
pub use config::{set_max_retries_default, set_retries_default_disabled, set_retry_factor_default};

use self::outcome::{Outcomes, OutcomesDeref};
use crate::atomic::{AtomicAny, AtomicBool, AtomicOption, AtomicUsize};
use crate::bee::{Context, Queen, Worker};
use condvar::{MutexCondvar, PhasedCondvar};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

type TaskSender<W> = std::sync::mpsc::Sender<Task<W>>;
type TaskReceiver<W> = std::sync::mpsc::Receiver<Task<W>>;
type OutcomeSender<W> = crate::channel::Sender<Outcome<W>>;
//pub type OutcomeReceiver<W> = channel::Receiver<Outcome<W>>;

type Usize = AtomicOption<usize, AtomicUsize>;
type Any<T> = AtomicOption<T, AtomicAny<T>>;

#[cfg(feature = "retry")]
mod retry_prelude {
    pub use parking_lot::RwLock;
    pub use std::time::Instant;

    use crate::atomic::{AtomicOption, AtomicU32, AtomicU64};

    pub type U32 = AtomicOption<u32, AtomicU32>;
    pub type U64 = AtomicOption<u64, AtomicU64>;
}
#[cfg(feature = "retry")]
use retry_prelude::*;

/// A pool of worker threads that each execute the same function. A `Hive` is created by a
/// [`Builder`].
///
/// A `Hive` has a [`Queen`] that creates a [`Worker`] for each thread in the pool. The `Worker` has
/// a [`apply`] method that is called to execute a task, which consists of an input value, a
/// `Context`, and an optional output channel. If the `Worker` processes the task successfully, the
/// output is sent to the output channel if one is provided, otherwise it is retained in the `Hive`
/// for later retrieval. If the `Worker` encounters an error, then it will retry the task if the
/// error is retryable and the `Hive` has been configured to retry tasks. If a task cannot be
/// processed after the maximum number of retries, then an error is sent to the output channel or
/// retained in the `Hive` for later retrieval.
///
/// A `Worker` should never panic, but if it does, the worker thread will terminate and the `Hive`
/// will spawn a new worker thread with a new `Worker`.
///
/// When a `Hive` is dropped, all the worker threads are terminated automatically. Prior to
/// dropping the `Hive`, the `into_husk()` method can be called to retrieve all of the `Hive` data
/// necessary to build a new `Hive`, as well as any stored outcomes (those that were not sent to an
/// output channel).

#[derive(Debug)]
pub struct Hive<W: Worker, Q: Queen<Kind = W>> {
    task_tx: TaskSender<W>,
    shared: Arc<Shared<W, Q>>,
}

/// Internal representation of a task to be processed by a `Hive`.
struct Task<W: Worker> {
    input: W::Input,
    ctx: Context,
    outcome_tx: Option<OutcomeSender<W>>,
}

/// Core configuration parameters that are set by a `Builder`, used in a `Hive`, and preserved in a
/// `Husk`. Fields are `AtomicOption`s, which enables them to be transitioned back and forth
/// between thread-safe and non-thread-safe contexts.
#[derive(Clone, Debug, Default)]
struct Config {
    /// Number of worker threads to spawn
    num_threads: Usize,
    /// Name to give each worker thread
    thread_name: Any<String>,
    /// Stack size for each worker thread
    thread_stack_size: Usize,
    /// Maximum number of retries for a task
    #[cfg(feature = "retry")]
    max_retries: U32,
    /// Multiplier for the retry backoff strategy
    #[cfg(feature = "retry")]
    retry_factor: U64,
    /// CPU cores to which worker threads can be pinned
    #[cfg(feature = "affinity")]
    affinity: Any<cores::Cores>,
}

/// Data shared by all worker threads in a `Hive`.
struct Shared<W: Worker, Q: Queen<Kind = W>> {
    config: Config,
    queen: Mutex<Q>,
    task_rx: Mutex<TaskReceiver<W>>,
    num_tasks_queued: AtomicUsize,
    num_tasks_active: AtomicUsize,
    next_task_index: AtomicUsize,
    num_panics: AtomicUsize,
    suspended: Arc<AtomicBool>,
    suspended_condvar: MutexCondvar,
    empty_condvar: PhasedCondvar,
    outcomes: Mutex<HashMap<usize, Outcome<W>>>,
    #[cfg(feature = "retry")]
    retry_queue: Mutex<delay::DelayQueue<Task<W>>>,
    #[cfg(feature = "retry")]
    next_retry: RwLock<Option<Instant>>,
}

#[cfg(test)]
mod test {
    use super::{Builder, Hive, Outcome, OutcomeDerefStore, OutcomeIteratorExt, OutcomeStore};
    use crate::bee::stock::{Caller, OnceCaller, RefCaller, Thunk, ThunkWorker};
    use crate::bee::{
        ApplyError, ApplyRefError, Context, DefaultQueen, Queen, RefWorker, RefWorkerResult,
        Worker, WorkerResult,
    };
    use crate::channel::{Message, ReceiverExt};
    use crate::hive::outcome::OutcomesDeref;
    use std::{
        fmt::Debug,
        io::{self, BufRead, BufReader, Write},
        process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Barrier,
        },
        thread,
        time::Duration,
    };

    const TEST_TASKS: usize = 4;
    const ONE_SEC: Duration = Duration::from_secs(1);
    const SHORT_TASK: Duration = Duration::from_secs(2);
    const LONG_TASK: Duration = Duration::from_secs(5);

    /// Convenience function that returns a `Hive` configured with the global defaults, and the
    /// specified number of workers that execute `Thunk<T>`s, i.e. closures that return `T`.
    pub fn thunk_hive<T: Send + Sync + Debug + 'static>(
        num_threads: usize,
    ) -> Hive<ThunkWorker<T>, DefaultQueen<ThunkWorker<T>>> {
        Builder::default()
            .num_threads(num_threads)
            .build_with_default()
    }

    #[test]
    fn test_works() {
        let hive = thunk_hive(TEST_TASKS);
        let (tx, rx) = super::outcome_channel();
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            hive.apply_store(Thunk::of(move || {
                tx.send(1).unwrap();
            }));
        }
        assert_eq!(rx.iter().take(TEST_TASKS).sum::<usize>(), TEST_TASKS);
    }

    #[test]
    fn test_grow_from_zero() {
        let hive = thunk_hive::<u8>(0);
        // check that with 0 threads no tasks are scheduled
        let (tx, rx) = super::outcome_channel();
        let _ = hive.apply_send(Thunk::of(|| 0), tx);
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks_queued(), 1);
        assert!(matches!(rx.try_recv_msg(), Message::ChannelEmpty));
        hive.grow(1);
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks_queued(), 0);
        assert!(matches!(
            rx.try_recv_msg(),
            Message::Received(Outcome::Success { value: 0, .. })
        ));
    }

    #[test]
    fn test_grow() {
        let hive = thunk_hive(TEST_TASKS);
        // queue some long-running tasks
        for _ in 0..TEST_TASKS {
            hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks_active(), TEST_TASKS);
        // increase the number of threads
        let new_threads = 4;
        let total_threads = new_threads + TEST_TASKS;
        hive.grow(new_threads);
        // queue some more long-running tasks
        for _ in 0..new_threads {
            hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks_active(), total_threads);
        let husk = hive.into_husk();
        assert_eq!(husk.iter_successes().count(), total_threads);
    }

    #[test]
    fn test_suspend() {
        let hive = thunk_hive(TEST_TASKS);
        // queue some long-running tasks
        let total_tasks = 2 * TEST_TASKS;
        for _ in 0..total_tasks {
            hive.apply_store(Thunk::of(|| thread::sleep(SHORT_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks_active(), TEST_TASKS);
        assert_eq!(hive.num_tasks_queued(), TEST_TASKS);
        hive.suspend();
        // active tasks should finish but no more tasks should be started
        thread::sleep(SHORT_TASK);
        assert_eq!(hive.num_tasks_active(), 0);
        assert_eq!(hive.num_tasks_queued(), TEST_TASKS);
        assert_eq!(hive.num_successes(), TEST_TASKS);
        hive.resume();
        // new tasks should start
        thread::sleep(ONE_SEC);
        assert_eq!(hive.num_tasks_active(), TEST_TASKS);
        assert_eq!(hive.num_tasks_queued(), 0);
        thread::sleep(SHORT_TASK);
        // all tasks should be completed
        assert_eq!(hive.num_tasks_active(), 0);
        assert_eq!(hive.num_tasks_queued(), 0);
        assert_eq!(hive.num_successes(), total_tasks);
    }

    #[derive(Debug, Default)]
    struct MyRefWorker;

    impl RefWorker for MyRefWorker {
        type Input = u8;
        type Output = u8;
        type Error = ();

        fn apply_ref(&mut self, input: &Self::Input, ctx: &Context) -> RefWorkerResult<Self> {
            for _ in 0..3 {
                thread::sleep(Duration::from_secs(1));
                if ctx.is_cancelled() {
                    return Err(ApplyRefError::Cancelled);
                }
            }
            Ok(*input)
        }
    }

    #[test]
    fn test_suspend_with_cancelled_tasks() {
        let hive = Builder::new()
            .num_threads(TEST_TASKS)
            .build_with_default::<MyRefWorker>();
        hive.swarm_store(0..TEST_TASKS as u8);
        hive.suspend();
        // wait for tasks to be cancelled
        thread::sleep(Duration::from_secs(2));
        hive.resume_store();
        thread::sleep(Duration::from_secs(1));
        // unprocessed tasks should be requeued
        assert_eq!(hive.num_tasks_active(), TEST_TASKS);
        thread::sleep(Duration::from_secs(3));
        assert_eq!(hive.num_successes(), TEST_TASKS);
    }

    #[test]
    fn test_num_tasks_active() {
        let hive = thunk_hive(TEST_TASKS);
        for _ in 0..2 * TEST_TASKS {
            hive.apply_store(Thunk::of(|| loop {
                thread::sleep(LONG_TASK)
            }));
        }
        thread::sleep(ONE_SEC);
        let num_tasks_active = hive.num_tasks_active();
        assert_eq!(num_tasks_active, TEST_TASKS);
        let num_threads = hive.num_threads();
        assert_eq!(num_threads, TEST_TASKS);
    }

    #[test]
    fn test_all_threads() {
        let hive = Builder::new()
            .with_thread_per_core()
            .build_with_default::<ThunkWorker<()>>();
        let num_threads = num_cpus::get();
        for _ in 0..num_threads {
            hive.apply_store(Thunk::of(|| loop {
                thread::sleep(LONG_TASK)
            }));
        }
        thread::sleep(ONE_SEC);
        let num_tasks_active = hive.num_tasks_active();
        assert_eq!(num_tasks_active, num_threads);
        let num_threads = hive.num_threads();
        assert_eq!(num_threads, num_threads);
    }

    #[test]
    fn test_panic() {
        let hive = thunk_hive(TEST_TASKS);
        let (tx, _) = super::outcome_channel();
        // Panic all the existing threads.
        for _ in 0..TEST_TASKS {
            hive.apply_send(Thunk::of(|| panic!("intentional panic")), tx.clone());
        }
        hive.join();
        // Ensure that none of the threads have panicked
        assert_eq!(hive.num_panics(), TEST_TASKS);
        let husk = hive.into_husk();
        assert_eq!(husk.num_panics(), TEST_TASKS);
    }

    #[test]
    fn test_catch_panic() {
        let hive = Builder::new()
            .num_threads(TEST_TASKS)
            .build_with(RefCaller::of(|_: &u8| -> Result<u8, String> {
                panic!("intentional panic")
            }));
        let (tx, rx) = super::outcome_channel();
        // Panic all the existing threads.
        for i in 0..TEST_TASKS {
            hive.apply_send(i as u8, tx.clone());
        }
        hive.join();
        // Ensure that none of the threads have panicked
        assert_eq!(hive.num_panics(), 0);
        // Check that all the results are Outcome::Panic
        for outcome in rx.into_iter().take(TEST_TASKS) {
            assert!(matches!(outcome, Outcome::Panic { .. }));
        }
    }

    #[test]
    fn test_should_not_panic_on_drop_if_subtasks_panic_after_drop() {
        let hive = thunk_hive(TEST_TASKS);
        let waiter = Arc::new(Barrier::new(TEST_TASKS + 1));

        // Panic all the existing threads in a bit.
        for _ in 0..TEST_TASKS {
            let waiter = waiter.clone();
            hive.apply_store(Thunk::of(move || {
                waiter.wait();
                panic!("intentional panic");
            }));
        }

        drop(hive);

        // Kick off the failure.
        waiter.wait();
    }

    #[test]
    fn test_massive_task_creation() {
        let test_tasks = 4_200_000;

        let hive = thunk_hive(TEST_TASKS);
        let b0 = Arc::new(Barrier::new(TEST_TASKS + 1));
        let b1 = Arc::new(Barrier::new(TEST_TASKS + 1));

        let (tx, rx) = super::outcome_channel();

        for i in 0..test_tasks {
            let tx = tx.clone();
            let (b0, b1) = (b0.clone(), b1.clone());

            hive.apply_store(Thunk::of(move || {
                // Wait until the pool has been filled once.
                if i < TEST_TASKS {
                    b0.wait();
                    // wait so the pool can be measured
                    b1.wait();
                }
                assert!(tx.send(1).is_ok());
            }));
        }

        b0.wait();
        assert_eq!(hive.num_tasks_active(), TEST_TASKS);
        b1.wait();

        assert_eq!(rx.iter().take(test_tasks).sum::<usize>(), test_tasks);
        hive.join();

        let atomic_num_tasks_active = hive.num_tasks_active();
        assert!(
            atomic_num_tasks_active == 0,
            "atomic_num_tasks_active: {}",
            atomic_num_tasks_active
        );
    }

    #[test]
    fn test_name() {
        let name = "test";
        let hive = Builder::new()
            .thread_name(name.to_owned())
            .num_threads(2)
            .build_with_default::<ThunkWorker<()>>();
        let (tx, rx) = super::outcome_channel();

        // initial thread should share the name "test"
        for _ in 0..2 {
            let tx = tx.clone();
            hive.apply_store(Thunk::of(move || {
                let name = thread::current().name().unwrap().to_owned();
                tx.send(name).unwrap();
            }));
        }

        // new spawn thread should share the name "test" too.
        hive.grow(3);
        let tx_clone = tx.clone();
        hive.apply_store(Thunk::of(move || {
            let name = thread::current().name().unwrap().to_owned();
            tx_clone.send(name).unwrap();
        }));

        for thread_name in rx.iter().take(3) {
            assert_eq!(name, thread_name);
        }
    }

    #[test]
    fn test_stack_size() {
        let stack_size = 4_000_000;

        let hive = Builder::new()
            .num_threads(1)
            .thread_stack_size(stack_size)
            .build_with_default::<ThunkWorker<usize>>();

        let actual_stack_size = hive
            .apply(Thunk::of(|| {
                //println!("This thread has a 4 MB stack size!");
                stacker::remaining_stack().unwrap()
            }))
            .unwrap() as f64;

        // measured value should be within 1% of actual
        assert!(actual_stack_size > (stack_size as f64 * 0.99));
        assert!(actual_stack_size < (stack_size as f64 * 1.01));
    }

    #[test]
    fn test_debug() {
        let hive = thunk_hive::<()>(4);
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { task_tx: Sender { .. }, shared: Shared { name: None, num_threads: 4, num_tasks_queued: 0, num_tasks_active: 0 } }"
        );

        let hive = Builder::new()
            .thread_name("hello")
            .num_threads(4)
            .build_with_default::<ThunkWorker<()>>();
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { task_tx: Sender { .. }, shared: Shared { name: \"hello\", num_threads: 4, num_tasks_queued: 0, num_tasks_active: 0 } }"
        );

        let hive = thunk_hive(4);
        hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        thread::sleep(ONE_SEC);
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { task_tx: Sender { .. }, shared: Shared { name: None, num_threads: 4, num_tasks_queued: 0, num_tasks_active: 1 } }"
        );
    }

    #[test]
    fn test_repeated_join() {
        let hive = Builder::new()
            .thread_name("repeated join test")
            .num_threads(8)
            .build_with_default::<ThunkWorker<()>>();
        let test_count = Arc::new(AtomicUsize::new(0));

        for _ in 0..42 {
            let test_count = test_count.clone();
            hive.apply_store(Thunk::of(move || {
                thread::sleep(SHORT_TASK);
                test_count.fetch_add(1, Ordering::Release);
            }));
        }

        println!("{:?}", hive);
        hive.join();
        assert_eq!(42, test_count.load(Ordering::Acquire));

        for _ in 0..42 {
            let test_count = test_count.clone();
            hive.apply_store(Thunk::of(move || {
                thread::sleep(SHORT_TASK);
                test_count.fetch_add(1, Ordering::Relaxed);
            }));
        }
        hive.join();
        assert_eq!(84, test_count.load(Ordering::Relaxed));
    }

    #[test]
    fn test_multi_join() {
        // Toggle the following lines to debug the deadlock
        fn error(_s: String) {
            //use ::std::io::Write;
            //let stderr = ::std::io::stderr();
            //let mut stderr = stderr.lock();
            //stderr.write(&_s.as_bytes()).is_ok();
        }

        let hive0 = Builder::new()
            .thread_name("multi join pool0")
            .num_threads(4)
            .build_with_default::<ThunkWorker<()>>();
        let hive1 = Builder::new()
            .thread_name("multi join pool1")
            .num_threads(4)
            .build_with_default::<ThunkWorker<u32>>();
        let (tx, rx) = super::outcome_channel();

        for i in 0..8 {
            let pool1 = hive1.clone();
            let pool0_ = hive0.clone();
            let tx = tx.clone();
            hive0.apply_store(Thunk::of(move || {
                pool1.apply_send(
                    Thunk::of(move || {
                        error(format!("p1: {} -=- {:?}\n", i, pool0_));
                        pool0_.join();
                        error(format!("p1: send({})\n", i));
                        i
                    }),
                    tx,
                );
                error(format!("p0: {}\n", i));
            }));
        }
        drop(tx);

        let msg = rx.try_recv_msg();
        match msg {
            Message::Received(_) => dbg!("received"),
            Message::ChannelEmpty => dbg!("channel empty"),
            Message::ChannelDisconnected => dbg!("channel disconnected"),
        };
        assert!(matches!(msg, Message::ChannelEmpty));
        error(format!("{:?}\n{:?}\n", hive0, hive1));
        hive0.join();
        error(format!("pool0.join() complete =-= {:?}", hive1));
        hive1.join();
        error("pool1.join() complete\n".into());
        assert_eq!(
            rx.into_iter().map(Outcome::unwrap).sum::<u32>(),
            1 + 2 + 3 + 4 + 5 + 6 + 7
        );
    }

    #[test]
    fn test_empty_hive() {
        // Joining an empty hive must return imminently
        let hive = thunk_hive::<()>(4);
        hive.join();
    }

    #[test]
    fn test_no_fun_or_joy() {
        // What happens when you keep adding tasks after a join

        fn sleepy_function() {
            thread::sleep(LONG_TASK);
        }

        let hive = Builder::new()
            .thread_name("no fun or joy")
            .num_threads(8)
            .build_with_default::<ThunkWorker<()>>();

        hive.apply_store(Thunk::of(sleepy_function));

        let p_t = hive.clone();
        thread::spawn(move || {
            (0..23)
                .inspect(|_| {
                    p_t.apply_store(Thunk::of(sleepy_function));
                })
                .count();
        });

        hive.join();
    }

    #[test]
    fn test_map() {
        let hive = Builder::new()
            .num_threads(2)
            .build_with_default::<ThunkWorker<u8>>();
        let outputs: Vec<_> = hive
            .map((0..10u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((10 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>())
    }

    #[test]
    fn test_map_unordered() {
        let hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let outputs: Vec<_> = hive
            .map_unordered((0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        assert_eq!(outputs, (0..8).rev().collect::<Vec<_>>())
    }

    #[test]
    fn test_map_send() {
        let hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let (tx, rx) = super::outcome_channel();
        let mut indices = hive.map_send(
            (0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }),
            tx,
        );
        let (mut outcome_indices, values): (Vec<usize>, Vec<u8>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, index } => (index, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        assert_eq!(values, (0..8).rev().collect::<Vec<_>>());
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    fn test_map_store() {
        let mut hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let mut indices = hive.map_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive.join();
        for i in indices.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_indices, values): (Vec<usize>, Vec<u8>) = indices
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .collect();
        assert_eq!(values, (0..8).collect::<Vec<_>>());
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    fn test_swarm() {
        let hive = Builder::new()
            .num_threads(2)
            .build_with_default::<ThunkWorker<u8>>();
        let outputs: Vec<_> = hive
            .swarm((0..10u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((10 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>())
    }

    #[test]
    fn test_swarm_unordered() {
        let hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let outputs: Vec<_> = hive
            .swarm_unordered((0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }))
            .map(Outcome::unwrap)
            .collect();
        assert_eq!(outputs, (0..8).rev().collect::<Vec<_>>())
    }

    #[test]
    fn test_swarm_send() {
        let hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let (tx, rx) = super::outcome_channel();
        let mut indices = hive.swarm_send(
            (0..8u8).map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }),
            tx,
        );
        let (mut outcome_indices, values): (Vec<usize>, Vec<u8>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, index } => (index, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        assert_eq!(values, (0..8).rev().collect::<Vec<_>>());
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    fn test_swarm_store() {
        let mut hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let mut indices = hive.swarm_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive.join();
        for i in indices.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_indices, values): (Vec<usize>, Vec<u8>) = indices
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .collect();
        assert_eq!(values, (0..8).collect::<Vec<_>>());
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    fn test_scan() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with(Caller::of(|i| i * i));
        let (outputs, state) = hive.scan(0..10, 0, |acc, i| {
            *acc += i;
            *acc
        });
        let mut outputs = outputs.unwrap();
        outputs.sort();
        assert_eq!(outputs.len(), 10);
        assert_eq!(state, 45);
        assert_eq!(
            outputs,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_scan_send() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with(Caller::of(|i| i * i));
        let (tx, rx) = super::outcome_channel();
        let (mut indices, state) = hive.scan_send(0..10, tx, 0, |acc, i| {
            *acc += i;
            *acc
        });
        assert_eq!(indices.len(), 10);
        assert_eq!(state, 45);
        let (mut outcome_indices, mut values): (Vec<usize>, Vec<i32>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, index } => (index, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        values.sort();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    fn test_try_scan_send() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with(Caller::of(|i| i * i));
        let (tx, rx) = super::outcome_channel();
        let (mut indices, state) = hive
            .try_scan_send(0..10, tx, 0, |acc, i| {
                *acc += i;
                Ok::<_, String>(*acc)
            })
            .unwrap();
        assert_eq!(indices.len(), 10);
        assert_eq!(state, 45);
        let (mut outcome_indices, mut values): (Vec<usize>, Vec<i32>) = rx
            .iter()
            .map(|outcome| match outcome {
                Outcome::Success { value, index } => (index, value),
                _ => panic!("unexpected error"),
            })
            .unzip();
        values.sort();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    #[should_panic]
    fn test_try_scan_send_fail() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with(OnceCaller::of(|i: i32| Ok::<_, String>(i * i)));
        let (tx, _) = super::outcome_channel();
        hive.try_scan_send(0..10, tx, 0, |_, _| Err("fail"))
            .unwrap();
    }

    #[test]
    fn test_scan_store() {
        let mut hive = Builder::new()
            .num_threads(4)
            .build_with(Caller::of(|i| i * i));
        let (mut indices, state) = hive.scan_store(0..10, 0, |acc, i| {
            *acc += i;
            *acc
        });
        assert_eq!(indices.len(), 10);
        assert_eq!(state, 45);
        hive.join();
        for i in indices.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_indices, values): (Vec<usize>, Vec<i32>) = indices
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .unzip();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    fn test_try_scan_store() {
        let mut hive = Builder::new()
            .num_threads(4)
            .build_with(Caller::of(|i| i * i));
        let (mut indices, state) = hive
            .try_scan_store(0..10, 0, |acc, i| {
                *acc += i;
                Ok::<i32, String>(*acc)
            })
            .unwrap();
        assert_eq!(indices.len(), 10);
        assert_eq!(state, 45);
        hive.join();
        for i in indices.iter() {
            assert!(hive.outcomes_deref().get(i).unwrap().is_success());
        }
        let (mut outcome_indices, values): (Vec<usize>, Vec<i32>) = indices
            .clone()
            .into_iter()
            .map(|i| (i, hive.remove_success(i).unwrap()))
            .unzip();
        assert_eq!(
            values,
            (0..10)
                .scan(0, |acc, i| {
                    *acc += i;
                    Some(*acc)
                })
                .map(|i| i * i)
                .collect::<Vec<_>>()
        );
        indices.sort();
        outcome_indices.sort();
        assert_eq!(indices, outcome_indices);
    }

    #[test]
    #[should_panic]
    fn test_try_scan_store_fail() {
        let hive = Builder::new()
            .num_threads(4)
            .build_with(OnceCaller::of(|i: i32| Ok::<i32, String>(i * i)));
        hive.try_scan_store(0..10, 0, |_, _| Err("fail")).unwrap();
    }

    #[test]
    fn test_husk() {
        let hive1 = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let indices = hive1.map_store((0..8u8).map(|i| Thunk::of(move || i)));
        hive1.join();
        let mut husk1 = hive1.into_husk();
        for i in indices.iter() {
            assert!(husk1.outcomes_deref().get(i).unwrap().is_success());
            assert!(matches!(husk1.get(*i), Some(Outcome::Success { .. })));
        }

        let builder = husk1.as_builder();
        let hive2 = builder
            .num_threads(4)
            .build_with_default::<ThunkWorker<u8>>();
        hive2.map_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive2.join();
        let mut husk2 = hive2.into_husk();

        let mut outputs1 = husk1
            .remove_all()
            .into_iter()
            .map(Outcome::unwrap)
            .collect::<Vec<_>>();
        outputs1.sort();
        let mut outputs2 = husk2
            .remove_all()
            .into_iter()
            .map(Outcome::unwrap)
            .collect::<Vec<_>>();
        outputs2.sort();
        assert_eq!(outputs1, outputs2);

        let hive3 = husk1.into_hive();
        hive3.map_store((0..8u8).map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive3.join();
        let husk3 = hive3.into_husk();
        let (_, outcomes3) = husk3.into_parts();
        let mut outputs3 = outcomes3
            .into_iter()
            .map(Outcome::unwrap)
            .collect::<Vec<_>>();
        outputs3.sort();
        assert_eq!(outputs1, outputs3);
    }

    #[test]
    fn test_clone() {
        let hive = Builder::new()
            .thread_name("clone example")
            .num_threads(2)
            .build_with_default::<ThunkWorker<()>>();

        // This batch of tasks will occupy the pool for some time
        for _ in 0..6 {
            hive.apply_store(Thunk::of(|| {
                thread::sleep(SHORT_TASK);
            }));
        }

        // The following tasks will be inserted into the pool in a random fashion
        let t0 = {
            let pool = hive.clone();
            thread::spawn(move || {
                // wait for the first batch of tasks to finish
                pool.join();

                let (tx, rx) = super::outcome_channel();
                for i in 0..42 {
                    let tx = tx.clone();
                    pool.apply_store(Thunk::of(move || {
                        tx.send(i).expect("channel will be waiting");
                    }));
                }
                drop(tx);
                rx.iter().sum::<i32>()
            })
        };
        let t1 = {
            let pool = hive.clone();
            thread::spawn(move || {
                // wait for the first batch of tasks to finish
                pool.join();

                let (tx, rx) = super::outcome_channel();
                for i in 1..12 {
                    let tx = tx.clone();
                    pool.apply_store(Thunk::of(move || {
                        tx.send(i).expect("channel will be waiting");
                    }));
                }
                drop(tx);
                rx.iter().product::<i32>()
            })
        };

        assert_eq!(
            861,
            t0.join()
                .expect("thread 0 will return after calculating additions",)
        );
        assert_eq!(
            39916800,
            t1.join()
                .expect("thread 1 will return after calculating multiplications",)
        );
    }

    type VoidThunkWorker = ThunkWorker<()>;
    type VoidThunkWorkerHive = Hive<VoidThunkWorker, crate::bee::DefaultQueen<VoidThunkWorker>>;

    #[test]
    fn test_send() {
        fn assert_send<T: Send>() {}
        assert_send::<VoidThunkWorkerHive>();
    }

    #[test]
    fn test_cloned_eq() {
        let a = thunk_hive::<()>(2);
        assert_eq!(a, a.clone());
    }

    #[test]
    /// The scenario is joining threads should not be stuck once their wave of joins has completed.
    /// So once one thread joining on a pool has succeded other threads joining on the same pool
    /// must get out even if the thread is used for other tasks while the first group is finishing
    /// their join.
    ///
    /// In this example, this means the waiting threads will exit the join in groups of four
    /// because the waiter pool has four processes.
    fn test_join_wavesurfer() {
        let n_cycles = 4;
        let n_workers = 4;
        let (tx, rx) = super::outcome_channel();
        let builder = Builder::new()
            .num_threads(n_workers)
            .thread_name("join wavesurfer");
        let waiter_hive = builder.clone().build_with_default::<ThunkWorker<()>>();
        let clock_hive = builder.build_with_default::<ThunkWorker<()>>();

        let barrier = Arc::new(Barrier::new(3));
        let wave_clock = Arc::new(AtomicUsize::new(0));
        let clock_thread = {
            let barrier = barrier.clone();
            let wave_clock = wave_clock.clone();
            thread::spawn(move || {
                barrier.wait();
                println!("clock thread past barrier");
                for wave_num in 0..n_cycles {
                    let prev_wave = wave_clock.swap(wave_num, Ordering::SeqCst);
                    println!(
                        "Prev wave: {}, new wave: {}; sleeping for 1sec",
                        prev_wave, wave_num
                    );
                    thread::sleep(ONE_SEC);
                    println!("clock thread wake");
                }
            })
        };

        {
            let barrier = barrier.clone();
            clock_hive.apply_store(Thunk::of(move || {
                barrier.wait();
                println!("clock hive past barrier; sleeping for 100ms");
                // this sleep is for stabilisation on weaker platforms
                thread::sleep(Duration::from_millis(100));
                println!("clock hive wake");
            }));
        }

        // prepare three waves of tasks
        for worker in 0..3 * n_workers {
            let tx = tx.clone();
            let clock_hive = clock_hive.clone();
            let wave_clock = wave_clock.clone();
            println!("calling waiter_hive.apply_store for worker {}", worker);
            waiter_hive.apply_store(Thunk::of(move || {
                let wave_before = wave_clock.load(Ordering::SeqCst);
                println!("Worker: {}, wave before: {}; joining", worker, wave_before);
                clock_hive.join();
                println!(
                    "Worker: {} past join, submitting task to clock_hive",
                    worker
                );
                // submit tasks for the next wave
                clock_hive.apply_store(Thunk::of(|| thread::sleep(ONE_SEC)));
                let wave_after = wave_clock.load(Ordering::SeqCst);
                println!(
                    "Worker: {} past task submission, wave after: {}",
                    worker, wave_after
                );
                tx.send((wave_before, wave_after, worker)).unwrap();
            }));
        }
        println!("all scheduled at {}", wave_clock.load(Ordering::SeqCst));
        barrier.wait();

        println!("Main thread past barrier; joining clock_hive");
        clock_hive.join();

        drop(tx);
        let mut hist = vec![0; n_cycles];
        let mut data = vec![];
        for (before, after, worker) in rx.iter() {
            let mut dur = after - before;
            if dur >= n_cycles - 1 {
                dur = n_cycles - 1;
            }
            hist[dur] += 1;
            data.push((before, after, worker));
        }

        println!("Histogram of wave duration:");
        for (i, n) in hist.iter().enumerate() {
            println!(
                "\t{}: {} {}",
                i,
                n,
                &*(0..*n).fold("".to_owned(), |s, _| s + "*")
            );
        }

        for (wave_before, wave_after, worker) in data.iter() {
            println!(
                "Before: {}, After: {}, Worker: {}",
                wave_before, wave_after, worker
            );
            if *worker < n_workers {
                assert_eq!(wave_before, wave_after);
            } else {
                assert!(wave_before < wave_after);
            }
        }
        clock_thread.join().unwrap();
    }

    // cargo-llvm-cov doesn't yet support doctests in stable, so we need to duplicate them in
    // unit tests to get coverage

    #[test]
    fn doctest_lib_2() {
        // create a hive to process `Thunk`s - no-argument closures with the same return type (`i32`)
        let hive = Builder::new()
            .num_threads(4)
            .thread_name("thunk_hive")
            .build_with_default::<ThunkWorker<i32>>();

        // return results to your own channel...
        let (tx, rx) = crate::hive::outcome_channel();
        let indices = hive.swarm_send((0..10).map(|i: i32| Thunk::of(move || i * i)), tx);
        let outputs: Vec<_> = rx.take_outputs(indices).collect();
        assert_eq!(285, outputs.into_iter().sum());

        // return results as an iterator...
        let outputs2: Vec<_> = hive
            .swarm((0..10).map(|i: i32| Thunk::of(move || i * -i)))
            .into_outputs()
            .collect();
        assert_eq!(-285, outputs2.into_iter().sum());
    }

    #[test]
    fn doctest_lib_3() {
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

            fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
                self.write_char(input).map_err(|error| ApplyError::Fatal {
                    input: Some(input),
                    error,
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

        impl Queen for CatQueen {
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
                self.wait_for_all()
                    .into_iter()
                    .for_each(|result| match result {
                        Ok(status) if status.success() => (),
                        Ok(status) => eprintln!("Child process failed: {}", status),
                        Err(e) => eprintln!("Error waiting for child process: {}", e),
                    })
            }
        }

        // build the Hive
        let hive = Builder::new().num_threads(4).build_default::<CatQueen>();

        // prepare inputs
        let inputs: Vec<u8> = (0..8).map(|i| 97 + i).collect();

        // execute tasks and collect outputs
        let output = hive
            .swarm(inputs)
            .into_outputs()
            .fold(String::new(), |mut a, b| {
                a.push_str(&b);
                a
            })
            .into_bytes();

        // verify the output - note that `swarm` ensures the outputs are in the same order
        // as the inputs
        assert_eq!(output, b"abcdefgh");

        // shutdown the hive, use the Queen to wait on child processes, and report errors
        let (mut queen, _) = hive.into_husk().into_parts();
        let (wait_ok, wait_err): (Vec<_>, Vec<_>) =
            queen.wait_for_all().into_iter().partition(Result::is_ok);
        if !wait_err.is_empty() {
            panic!(
                "Error(s) occurred while waiting for child processes: {:?}",
                wait_err
            );
        }
        let exec_err_codes: Vec<_> = wait_ok
            .into_iter()
            .map(Result::unwrap)
            .filter(|status| !status.success())
            .filter_map(|status| status.code())
            .collect();
        if !exec_err_codes.is_empty() {
            panic!(
                "Child process(es) failed with exit codes: {:?}",
                exec_err_codes
            );
        }
    }
}

#[cfg(all(test, feature = "affinity"))]
mod affinity_tests {
    use crate::bee::stock::{Thunk, ThunkWorker};
    use crate::hive::Builder;

    #[test]
    fn test_affinity() {
        let hive = Builder::new()
            .thread_name("affinity example")
            .num_threads(2)
            .thread_affinity(0..2)
            .build_with_default::<ThunkWorker<()>>();

        hive.map_store((0..10).map(move |i| {
            Thunk::of(move || {
                if let Some(affininty) = core_affinity::get_core_ids() {
                    println!("task {} on thread with affinity {:?}", i, affininty);
                }
            })
        }));
    }

    #[test]
    fn test_use_all_cores() {
        let hive = Builder::new()
            .thread_name("affinity example")
            .with_thread_per_core()
            .with_default_thread_affinity()
            .build_with_default::<ThunkWorker<()>>();

        hive.map_store((0..num_cpus::get()).map(move |i| {
            Thunk::of(move || {
                if let Some(affininty) = core_affinity::get_core_ids() {
                    println!("task {} on thread with affinity {:?}", i, affininty);
                }
            })
        }));
    }
}

#[cfg(all(test, feature = "retry"))]
mod retry_tests {
    use crate::bee::stock::RetryCaller;
    use crate::bee::{ApplyError, Context};
    use crate::hive::{Builder, Outcome, OutcomeIteratorExt};
    use std::time::{Duration, SystemTime};

    fn echo_time(i: usize, ctx: &Context) -> Result<String, ApplyError<usize, String>> {
        let attempt = ctx.attempt();
        if attempt == 3 {
            Ok("Success".into())
        } else {
            // the delay between each message should be exponential
            println!("Task {} attempt {}: {:?}", i, attempt, SystemTime::now());
            Err(ApplyError::Retryable {
                input: i,
                error: "Retryable".into(),
            })
        }
    }

    #[test]
    fn test_retries() {
        let hive = Builder::new()
            .with_thread_per_core()
            .max_retries(3)
            .retry_factor(Duration::from_secs(1))
            .build_with(RetryCaller::of(echo_time));

        let v: Result<Vec<_>, _> = hive.swarm(0..10).into_results().collect();
        assert_eq!(v.unwrap().len(), 10);
    }

    #[test]
    fn test_retries_fail() {
        fn sometimes_fail(i: usize, _: &Context) -> Result<String, ApplyError<usize, String>> {
            match i % 3 {
                0 => Ok("Success".into()),
                1 => Err(ApplyError::Retryable {
                    input: i,
                    error: "Retryable".into(),
                }),
                2 => Err(ApplyError::Fatal {
                    input: Some(i),
                    error: "NotRetryable".into(),
                }),
                _ => unreachable!(),
            }
        }

        let hive = Builder::new()
            .with_thread_per_core()
            .max_retries(3)
            .build_with(RetryCaller::of(sometimes_fail));

        let (success, retry_failed, not_retried) = hive.swarm(0..10).fold(
            (0, 0, 0),
            |(success, retry_failed, not_retried), outcome| match outcome {
                Outcome::Success { .. } => (success + 1, retry_failed, not_retried),
                Outcome::MaxRetriesAttempted { .. } => (success, retry_failed + 1, not_retried),
                Outcome::Failure { .. } => (success, retry_failed, not_retried + 1),
                _ => unreachable!(),
            },
        );

        assert_eq!(success, 4);
        assert_eq!(retry_failed, 3);
        assert_eq!(not_retried, 3);
    }

    #[test]
    fn test_disable_retries() {
        let hive = Builder::new()
            .with_thread_per_core()
            .with_no_retries()
            .build_with(RetryCaller::of(echo_time));
        let v: Result<Vec<_>, _> = hive.swarm(0..10).into_results().collect();
        assert!(matches!(v, Err(_)));
    }
}
