use beekeeper::bee::prelude::*;
use beekeeper::bee::stock::EchoWorker;
use beekeeper::hive::{outcome_channel, Builder};
use divan::{bench, black_box_drop, AllocProfiler, Bencher};
use itertools::iproduct;
use std::time::Duration;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

const THREADS: &[usize] = &[1, 4, 8, 16];
const NUM_SHORT_TASKS: &[usize] = &[1, 100, 10_000, 1_000_000];
const NUM_LONG_TASKS: &[usize] = &[1, 10, 100, 1_000];

#[bench(args = iproduct!(THREADS, NUM_SHORT_TASKS))]
fn bench_apply_short_task(bencher: Bencher, (num_threads, num_tasks): (&usize, &usize)) {
    let hive = Builder::new()
        .num_threads(*num_threads)
        .build_with_default::<EchoWorker<usize>>();
    bencher.bench_local(|| {
        let (tx, rx) = outcome_channel();
        for i in 0..*num_tasks {
            hive.apply_send(i, tx.clone());
        }
        hive.join();
        rx.into_iter().take(*num_tasks).for_each(black_box_drop);
    })
}

#[derive(Debug, Clone)]
struct DelayWorker(Duration);

impl Worker for DelayWorker {
    type Input = usize;
    type Output = usize;
    type Error = ();

    fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
        std::thread::sleep(self.0);
        Ok(input)
    }
}

#[bench(args = iproduct!(THREADS, NUM_LONG_TASKS))]
fn bench_apply_long_task(bencher: Bencher, (num_threads, num_tasks): (&usize, &usize)) {
    let hive = Builder::new()
        .num_threads(*num_threads)
        .build_with(DelayWorker(Duration::from_millis(1)));
    bencher.bench_local(|| {
        let (tx, rx) = outcome_channel();
        for i in 0..*num_tasks {
            hive.apply_send(i, tx.clone());
        }
        hive.join();
        rx.into_iter().take(*num_tasks).for_each(black_box_drop);
    })
}

fn main() {
    divan::main();
}