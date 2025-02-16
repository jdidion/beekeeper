use beekeeper::bee::stock::EchoWorker;
use beekeeper::hive::{outcome_channel, Builder, ChannelBuilder};
use divan::{bench, black_box_drop, AllocProfiler, Bencher};
use itertools::iproduct;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

const THREADS: &[usize] = &[1, 4, 8, 16];
const TASKS: &[usize] = &[1, 100, 10_000, 1_000_000];

#[bench(args = iproduct!(THREADS, TASKS))]
fn bench_apply_short_task(bencher: Bencher, (num_threads, num_tasks): (&usize, &usize)) {
    let hive = ChannelBuilder::empty()
        .num_threads(*num_threads)
        .with_worker_default::<EchoWorker<usize>>()
        .build();
    bencher.bench_local(|| {
        let (tx, rx) = outcome_channel();
        for i in 0..*num_tasks {
            hive.apply_send(i, &tx);
        }
        hive.join();
        rx.into_iter().take(*num_tasks).for_each(black_box_drop);
    })
}

fn main() {
    divan::main();
}
