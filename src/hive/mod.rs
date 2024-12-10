#[cfg(feature = "affinity")]
mod affinity;
mod builder;
mod error;
mod husk;
mod outcome;
//mod scoped;
mod shared;
mod spawn;
mod stored;
mod thread;

pub use crate::channel::channel as outcome_channel;
#[cfg(feature = "affinity")]
pub use affinity::{CoreId, Cores};
pub use builder::{
    reset_defaults, set_max_retries_default, set_num_threads_default, set_num_threads_default_all,
    set_retries_default_disabled, set_retry_factor_default, Builder,
};
pub use error::{HiveError, HiveResult, HiveResultExt, TaskResult, TaskResultIteratorExt};
pub use husk::Husk;
pub use outcome::{Outcome, OutcomeIteratorExt};
pub use stored::Stored;
pub use thread::Hive;

pub(self) use shared::{OutcomeSender, Shared, Task, TaskSender};

pub mod prelude {
    pub use super::{
        Builder, Hive, HiveError, HiveResultExt, Husk, Outcome, OutcomeIteratorExt, Stored,
        TaskResultIteratorExt,
    };
}

#[cfg(test)]
mod test {
    use super::{
        Builder, Hive, HiveError, Outcome, OutcomeIteratorExt, Stored, TaskResultIteratorExt,
    };
    use crate::channel::{Message, ReceiverExt};
    use crate::task::{ApplyError, Context, Worker, WorkerResult};
    use crate::util::{self, Caller, RefCaller, RetryCaller, Thunk, ThunkWorker};
    use std::{
        io::{self, BufRead, BufReader, Write},
        process::{ChildStdin, ChildStdout, Command, Stdio},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Barrier,
        },
        thread,
        time::{self, Duration},
    };

    const TEST_TASKS: usize = 4;
    const ONE_SEC: Duration = Duration::from_secs(1);
    const SHORT_TASK: Duration = Duration::from_secs(2);
    const LONG_TASK: Duration = Duration::from_secs(5);

    #[test]
    fn test_works() {
        let hive = util::thunk_hive(TEST_TASKS);
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
        let hive = util::thunk_hive::<u8>(0);
        // check that with 0 threads no tasks are scheduled
        let (tx, rx) = super::outcome_channel();
        let _ = hive.apply_send(Thunk::of(|| 0), tx);
        thread::sleep(ONE_SEC);
        assert_eq!(hive.queued_count(), 1);
        matches!(rx.try_recv_msg(), Message::ChannelEmpty);
        hive.set_num_threads(1);
        thread::sleep(ONE_SEC);
        assert_eq!(hive.queued_count(), 0);
        matches!(
            rx.try_recv_msg(),
            Message::Received(Outcome::Success { value: 0, .. })
        );
    }

    #[test]
    fn test_set_num_threads_increasing() {
        let hive = util::thunk_hive(TEST_TASKS);
        // queue some long-running tasks
        for _ in 0..TEST_TASKS {
            hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.active_count(), TEST_TASKS);
        // increase the number of threads
        let new_thread_amount = TEST_TASKS + 8;
        hive.set_num_threads(new_thread_amount);
        // queue some more long-running tasks
        for _ in 0..(new_thread_amount - TEST_TASKS) {
            hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.active_count(), new_thread_amount);
        let husk = hive.into_husk();
        assert_eq!(husk.iter_successes().count(), new_thread_amount);
    }

    #[test]
    fn test_set_num_threads_decreasing() {
        let new_thread_amount = 2;
        let hive = util::thunk_hive(TEST_TASKS);
        for _ in 0..TEST_TASKS {
            hive.apply_store(Thunk::of(|| ()));
        }
        hive.set_num_threads(new_thread_amount);
        for _ in 0..new_thread_amount {
            hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        }
        thread::sleep(ONE_SEC);
        assert_eq!(hive.active_count(), new_thread_amount);
        hive.join();
        let husk = hive.into_husk();
        assert_eq!(
            husk.iter_successes().count(),
            TEST_TASKS + new_thread_amount
        );
    }

    #[test]
    fn test_shrink() {
        let test_tasks_begin = TEST_TASKS + 2;

        let hive = util::thunk_hive(test_tasks_begin);
        let b0 = Arc::new(Barrier::new(test_tasks_begin + 1));
        let b1 = Arc::new(Barrier::new(test_tasks_begin + 1));

        for _ in 0..test_tasks_begin {
            let (b0, b1) = (b0.clone(), b1.clone());
            hive.apply_store(Thunk::of(move || {
                b0.wait();
                b1.wait();
            }));
        }

        let b2 = Arc::new(Barrier::new(TEST_TASKS + 1));
        let b3 = Arc::new(Barrier::new(TEST_TASKS + 1));

        for _ in 0..TEST_TASKS {
            let (b2, b3) = (b2.clone(), b3.clone());
            hive.apply_store(Thunk::of(move || {
                b2.wait();
                b3.wait();
            }));
        }

        b0.wait();
        hive.set_num_threads(TEST_TASKS);

        assert_eq!(hive.active_count(), test_tasks_begin);
        b1.wait();

        b2.wait();
        assert_eq!(hive.active_count(), TEST_TASKS);
        b3.wait();
    }

    #[test]
    fn test_active_count() {
        let hive = util::thunk_hive(TEST_TASKS);
        for _ in 0..2 * TEST_TASKS {
            hive.apply_store(Thunk::of(|| loop {
                thread::sleep(LONG_TASK)
            }));
        }
        thread::sleep(ONE_SEC);
        let active_count = hive.active_count();
        assert_eq!(active_count, TEST_TASKS);
        let initialized_count = hive.max_count();
        assert_eq!(initialized_count, TEST_TASKS);
    }

    #[test]
    fn test_all_threads() {
        let hive = Builder::new()
            .all_threads()
            .build_with_default::<ThunkWorker<()>>();
        let num_threads = num_cpus::get();
        for _ in 0..num_threads {
            hive.apply_store(Thunk::of(|| loop {
                thread::sleep(LONG_TASK)
            }));
        }
        thread::sleep(ONE_SEC);
        let active_count = hive.active_count();
        assert_eq!(active_count, num_threads);
        let initialized_count = hive.max_count();
        assert_eq!(initialized_count, num_threads);
    }

    #[test]
    fn test_panic() {
        let hive = util::thunk_hive(TEST_TASKS);
        let (tx, _) = super::outcome_channel();
        // Panic all the existing threads.
        for _ in 0..TEST_TASKS {
            hive.apply_send(Thunk::of(|| panic!("intentional panic")), tx.clone());
        }
        hive.join();
        // Ensure that none of the threads have panicked
        assert_eq!(hive.panic_count(), TEST_TASKS);
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
        assert_eq!(hive.panic_count(), 0);
        // Check that all the results are Outcome::Panic
        for outcome in rx.into_iter().take(TEST_TASKS) {
            assert!(matches!(outcome, Outcome::Panic { .. }));
        }
    }

    #[test]
    fn test_should_not_panic_on_drop_if_subtasks_panic_after_drop() {
        let hive = util::thunk_hive(TEST_TASKS);
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

        let hive = util::thunk_hive(TEST_TASKS);
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
        assert_eq!(hive.active_count(), TEST_TASKS);
        b1.wait();

        assert_eq!(rx.iter().take(test_tasks).sum::<usize>(), test_tasks);
        hive.join();

        let atomic_active_count = hive.active_count();
        assert!(
            atomic_active_count == 0,
            "atomic_active_count: {}",
            atomic_active_count
        );
    }

    fn echo_time(i: usize, ctx: &Context) -> Result<String, ApplyError<usize, String>> {
        let attempt = ctx.attempt();
        if attempt == 3 {
            Ok("Success".into())
        } else {
            // the delay between each message should be exponential
            println!(
                "Task {} attempt {}: {:?}",
                i,
                attempt,
                time::SystemTime::now()
            );
            Err(ApplyError::Retryable {
                input: i,
                error: "Retryable".into(),
            })
        }
    }

    #[test]
    fn test_retries() {
        let hive = Builder::new()
            .all_threads()
            .max_retries(3)
            .retry_factor(time::Duration::from_secs(1))
            .build_with(RetryCaller::of(echo_time));

        let v: Result<Vec<_>, _> = hive.swarm(0..10).collect();
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
                2 => Err(ApplyError::NotRetryable {
                    input: Some(i),
                    error: "NotRetryable".into(),
                }),
                _ => unreachable!(),
            }
        }

        let hive = Builder::new()
            .all_threads()
            .max_retries(3)
            .build_with(RetryCaller::of(sometimes_fail));

        let (success, retry_failed, not_retried) =
            hive.swarm(0..10)
                .fold(
                    (0, 0, 0),
                    |(success, retry_failed, not_retried), result| match result {
                        Ok(_) => (success + 1, retry_failed, not_retried),
                        Err(HiveError::MaxRetriesAttempted(_)) => {
                            (success, retry_failed + 1, not_retried)
                        }
                        Err(HiveError::Failed(_)) => (success, retry_failed, not_retried + 1),
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
            .all_threads()
            .no_retries()
            .build_with(RetryCaller::of(echo_time));

        let v: Result<Vec<_>, _> = hive.swarm(0..10).collect();
        assert!(matches!(v, Err(_)));
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
        hive.set_num_threads(3);
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
            .try_apply(Thunk::of(|| {
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
        let hive = util::thunk_hive::<()>(4);
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { task_tx: Sender { .. }, shared: Shared { name: None, queued_count: 0, active_count: 0, max_count: 4 } }"
        );

        let hive = Builder::new()
            .thread_name("hello")
            .num_threads(4)
            .build_with_default::<ThunkWorker<()>>();
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { task_tx: Sender { .. }, shared: Shared { name: Some(\"hello\"), queued_count: 0, active_count: 0, max_count: 4 } }"
        );

        let hive = util::thunk_hive(4);
        hive.apply_store(Thunk::of(|| thread::sleep(LONG_TASK)));
        thread::sleep(ONE_SEC);
        let debug = format!("{:?}", hive);
        assert_eq!(
            debug,
            "Hive { task_tx: Sender { .. }, shared: Shared { name: None, queued_count: 0, active_count: 1, max_count: 4 } }"
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

        assert!(matches!(rx.try_recv_msg(), Message::ChannelEmpty));
        error(format!("{:?}\n{:?}\n", hive0, hive1));
        hive0.join();
        error(format!("pool0.join() complete =-= {:?}", hive1));
        hive1.join();
        error("pool1.join() complete\n".into());
        assert_eq!(
            rx.into_unordered_results().into_outputs().sum::<u32>(),
            1 + 2 + 3 + 4 + 5 + 6 + 7
        );
    }

    #[test]
    fn test_empty_hive() {
        // Joining an empty hive must return imminently
        let hive = util::thunk_hive::<()>(4);
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
                .map(|_| p_t.apply_store(Thunk::of(sleepy_function)))
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
            .map((0..10u8).into_iter().map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((10 - i as u64) * 100));
                    i
                })
            }))
            .into_outputs()
            .collect();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>())
    }

    #[test]
    fn test_map_unordered() {
        let hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let outputs: Vec<_> = hive
            .map_unordered((0..8u8).into_iter().map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }))
            .into_outputs()
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
            (0..8u8).into_iter().map(|i| {
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
        let mut indices = hive.map_store((0..8u8).into_iter().map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive.join();
        let (mut outcome_indices, values): (Vec<usize>, Vec<u8>) = indices
            .clone()
            .into_iter()
            .map(|i| hive.remove_success(i).unwrap())
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
            .swarm((0..10u8).into_iter().map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((10 - i as u64) * 100));
                    i
                })
            }))
            .into_outputs()
            .collect();
        assert_eq!(outputs, (0..10).collect::<Vec<_>>())
    }

    #[test]
    fn test_swarm_unordered() {
        let hive = Builder::new()
            .num_threads(8)
            .build_with_default::<ThunkWorker<u8>>();
        let outputs: Vec<_> = hive
            .swarm_unordered((0..8u8).into_iter().map(|i| {
                Thunk::of(move || {
                    thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                    i
                })
            }))
            .into_outputs()
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
            (0..8u8).into_iter().map(|i| {
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
        let mut indices = hive.swarm_store((0..8u8).into_iter().map(|i| {
            Thunk::of(move || {
                thread::sleep(Duration::from_millis((8 - i as u64) * 100));
                i
            })
        }));
        hive.join();
        let (mut outcome_indices, values): (Vec<usize>, Vec<u8>) = indices
            .clone()
            .into_iter()
            .map(|i| hive.remove_success(i).unwrap())
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
        let (outputs, state) = hive
            .scan(0..10, 0, |acc, i| {
                *acc += i;
                *acc
            })
            .unwrap();
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
    type VoidThunkWorkerHive = Hive<VoidThunkWorker, crate::task::DefaultQueen<VoidThunkWorker>>;

    #[test]
    fn test_send() {
        fn assert_send<T: Send>() {}
        assert_send::<VoidThunkWorkerHive>();
    }

    #[test]
    fn test_cloned_eq() {
        let a = util::thunk_hive::<()>(2);
        assert_eq!(a, a.clone());
    }

    #[test]
    /// The scenario is joining threads should not be stuck once their wave of joins has completed.
    /// So once one thread joining on a pool has succeded other threads joining on the same pool
    /// must get out even if the thread is used for other tasks while the first group is finishing
    /// their join.
    ///
    /// In this example this means the waiting threads will exit the join in groups of four because
    /// the waiter pool has four processes.
    fn test_join_wavesurfer() {
        let n_cycles = 4;
        let n_processes = 4;
        let (tx, rx) = super::outcome_channel();
        let builder = Builder::new()
            .num_threads(n_processes)
            .thread_name("join wavesurfer");
        let p_waiter = builder.clone().build_with_default::<ThunkWorker<()>>();
        let p_clock = builder.build_with_default::<ThunkWorker<()>>();

        let barrier = Arc::new(Barrier::new(3));
        let wave_clock = Arc::new(AtomicUsize::new(0));
        let clock_thread = {
            let barrier = barrier.clone();
            let wave_clock = wave_clock.clone();
            thread::spawn(move || {
                barrier.wait();
                for wave_num in 0..n_cycles {
                    wave_clock.store(wave_num, Ordering::SeqCst);
                    thread::sleep(ONE_SEC);
                }
            })
        };

        {
            let barrier = barrier.clone();
            p_clock.apply_store(Thunk::of(move || {
                barrier.wait();
                // this sleep is for stabilisation on weaker platforms
                thread::sleep(Duration::from_millis(100));
            }));
        }

        // prepare three waves of tasks
        for i in 0..3 * n_processes {
            let p_clock = p_clock.clone();
            let tx = tx.clone();
            let wave_clock = wave_clock.clone();
            p_waiter.apply_store(Thunk::of(move || {
                let now = wave_clock.load(Ordering::SeqCst);
                p_clock.join();
                // submit tasks for the second wave
                p_clock.apply_store(Thunk::of(|| thread::sleep(ONE_SEC)));
                let clock = wave_clock.load(Ordering::SeqCst);
                tx.send((now, clock, i)).unwrap();
            }));
        }
        println!("all scheduled at {}", wave_clock.load(Ordering::SeqCst));
        barrier.wait();

        p_clock.join();
        //p_waiter.join();

        drop(tx);
        let mut hist = vec![0; n_cycles];
        let mut data = vec![];
        for (now, after, i) in rx.iter() {
            let mut dur = after - now;
            if dur >= n_cycles - 1 {
                dur = n_cycles - 1;
            }
            hist[dur] += 1;

            data.push((now, after, i));
        }
        for (i, n) in hist.iter().enumerate() {
            println!(
                "\t{}: {} {}",
                i,
                n,
                &*(0..*n).fold("".to_owned(), |s, _| s + "*")
            );
        }
        assert!(data.iter().all(|&(cycle, stop, i)| if i < n_processes {
            cycle == stop
        } else {
            cycle < stop
        }));

        clock_thread.join().unwrap();
    }

    // cargo-llvm-cov doesn't yet support doctests in stable, so we need to duplicate them in
    // unit tests to get coverage

    #[test]
    fn doctest_lib_2() {
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

            fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
                self.write_char(input)
                    .map_err(|error| ApplyError::NotRetryable {
                        input: Some(input),
                        error,
                    })
            }
        }

        let n_workers = 4;
        let n_tasks = 8;
        let hive = Builder::new()
            .num_threads(n_workers)
            .build_with_default::<LineDelimitedWorker>();

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
}

#[cfg(all(test, feature = "affinity"))]
mod affinity_tests {
    use crate::hive::Builder;
    use crate::util::{Thunk, ThunkWorker};

    #[test]
    fn test_affinity() {
        let hive = Builder::new()
            .thread_name("clone example")
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
}
