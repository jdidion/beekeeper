use crate::hive::{Cores, Husk, Outcome};
use crate::task::{Context, Queen, Worker};
use parking_lot::{Condvar, Mutex};
use std::{
    collections::HashMap,
    fmt, iter, mem,
    ops::DerefMut,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
    thread::Builder,
    time::Duration,
};

pub type TaskSender<W> = std::sync::mpsc::Sender<Task<W>>;
pub type TaskReceiver<W> = std::sync::mpsc::Receiver<Task<W>>;
pub type OutcomeSender<W> = crate::channel::Sender<Outcome<W>>;
//pub type OutcomeReceiver<W> = channel::Receiver<Outcome<W>>;

/// Internal representation of a task to be processed by a `Hive`.
// TODO: find a way to re-queue tasks with a delay so as not to block worker threads when
// retrying
pub struct Task<W: Worker> {
    input: W::Input,
    ctx: Context,
    outcome_tx: Option<OutcomeSender<W>>,
}

impl<W: Worker> Task<W> {
    /// Returns the index of this task.
    pub fn index(&self) -> usize {
        self.ctx.index()
    }

    /// Consumes this `Task` and returns a tuple `(input, context, outcome_tx)`.
    pub fn into_parts(self) -> (W::Input, Context, Option<OutcomeSender<W>>) {
        (self.input, self.ctx, self.outcome_tx)
    }
}

/// Data shared by all worker threads in a `Hive`.
pub struct Shared<W: Worker, Q: Queen<Kind = W>> {
    // fields used for thread spawning
    name: Option<String>,
    num_threads: AtomicUsize,
    stack_size: Option<usize>,
    #[cfg(feature = "affinity")]
    affinity: Mutex<Cores>,
    queen: Mutex<Q>,
    panic_count: AtomicUsize,
    // fields used for task processing
    queued_count: AtomicUsize,
    active_count: AtomicUsize,
    task_rx: Mutex<TaskReceiver<W>>,
    next_index: AtomicUsize,
    max_retries: Option<u32>,
    retry_factor: Option<u64>,
    cancelled: Arc<AtomicBool>,
    outcomes: Mutex<HashMap<usize, Outcome<W>>>,
    // fields used for join handling
    empty_trigger: Mutex<()>,
    empty_condvar: Condvar,
    join_generation: AtomicUsize,
}

impl<W: Worker, Q: Queen<Kind = W>> Shared<W, Q> {
    pub fn new(
        queen: Q,
        task_rx: TaskReceiver<W>,
        thread_name: Option<String>,
        stack_size: Option<usize>,
        max_retries: Option<u32>,
        retry_factor: Option<Duration>,
    ) -> Self {
        Shared {
            name: thread_name,
            num_threads: AtomicUsize::new(0),
            stack_size,
            #[cfg(feature = "affinity")]
            affinity: Mutex::new(Cores::empty()),
            queen: Mutex::new(queen),
            panic_count: AtomicUsize::new(0),
            queued_count: AtomicUsize::new(0),
            active_count: AtomicUsize::new(0),
            task_rx: Mutex::new(task_rx),
            next_index: AtomicUsize::new(0),
            max_retries,
            retry_factor: retry_factor.map(|d| d.as_nanos() as u64),
            cancelled: Arc::new(AtomicBool::new(false)),
            outcomes: Mutex::new(HashMap::new()),
            empty_condvar: Condvar::new(),
            empty_trigger: Mutex::new(()),
            join_generation: AtomicUsize::new(0),
        }
    }

    /// Increments the number of queued tasks. Returns a new `Task` with the provided input and
    /// `outcome_tx` and the next index.
    pub fn prepare_task(&self, input: W::Input, outcome_tx: Option<OutcomeSender<W>>) -> Task<W> {
        self.queued_count.fetch_add(1, Ordering::SeqCst);
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        Task {
            input,
            ctx: Context::new(index, self.cancelled.clone()),
            outcome_tx,
        }
    }

    /// Increments the number of queued tasks by the number of provided inputs. Returns an iterator
    /// over `Task`s created from the provided inputs, `outcome_tx`s, and sequential indices.
    pub fn prepare_batch<'a, T: Iterator<Item = W::Input> + 'a>(
        &'a self,
        min_size: usize,
        inputs: T,
        outcome_tx: Option<OutcomeSender<W>>,
    ) -> impl Iterator<Item = Task<W>> + 'a {
        self.queued_count.fetch_add(min_size, Ordering::SeqCst);
        let index_start = self.next_index.fetch_add(min_size, Ordering::SeqCst);
        let index_end = index_start + min_size;
        inputs
            .map(Some)
            .chain(iter::repeat_with(|| None))
            .zip(
                (index_start..index_end)
                    .map(Some)
                    .chain(iter::repeat_with(|| None)),
            )
            .map_while(move |pair| match pair {
                (Some(input), Some(index)) => Some(Task {
                    input,
                    ctx: Context::new(index, self.cancelled.clone()),
                    //attempt: 0,
                    outcome_tx: outcome_tx.clone(),
                }),
                (Some(input), None) => Some(self.prepare_task(input, outcome_tx.clone())),
                (None, Some(_)) => panic!("batch contained fewer than {min_size} items"),
                (None, None) => None,
            })
    }

    /// Returns a new [`Builder`] for creating a new thread in the `Hive`.
    pub fn as_thread_builder(&self) -> Builder {
        let mut builder = Builder::new();
        if let Some(ref name) = self.name {
            builder = builder.name(name.clone());
        }
        if let Some(ref stack_size) = self.stack_size {
            builder = builder.stack_size(stack_size.to_owned());
        }
        builder
    }

    /// Adds a core index to which a thread in the `Hive` is pinned.
    #[cfg(feature = "affinity")]
    pub fn add_core_affinity(&self, core_index: usize) -> bool {
        self.affinity.lock().insert(core_index)
    }

    /// Removes a core index after dropping the thread in the `Hive` that was pinned to it.
    #[cfg(feature = "affinity")]
    pub fn remove_core_affinity(&self, core_index: usize) -> bool {
        self.affinity.lock().remove(&core_index)
    }

    /// Removes from `other` the core indices to which the threads in the hive are currently
    /// pinned.
    #[cfg(feature = "affinity")]
    pub fn diff_core_affinity(&self, other: &mut Cores) {
        let affinity = self.affinity.lock();
        other.remove_all(&affinity);
    }

    /// Sets the `cancelled` flag. Worker threads may terminate early. No new worker threads will
    /// be spawned.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns `true` if the `cancelled` flag has been set.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Returns a new `Worker` from the queen, or an error if a `Worker` could not be created.
    pub fn create_worker(&self) -> Q::Kind {
        self.queen.lock().create()
    }

    /// Returns `true` if the hive is configured to retry tasks.
    pub fn has_retries(&self) -> bool {
        self.max_retries.is_some()
    }

    /// Returns the maximum number of retries allowed for a task.
    pub fn max_retries(&self) -> u32 {
        self.max_retries.unwrap_or(0)
    }

    /// Returns the `Duration` to wait before retrying a task on the given `attempt`.
    pub fn get_delay(&self, attempt: u32) -> Option<Duration> {
        self.retry_factor.map(|retry_factor| {
            2u64.checked_pow(attempt - 1)
                .and_then(|multiplier| {
                    retry_factor
                        .checked_mul(multiplier)
                        .or_else(|| Some(u64::MAX))
                        .map(Duration::from_nanos)
                })
                .unwrap()
        })
    }

    /// Returns `true` if the number of active threads is greater than the maximum number of
    /// threads allowed in the hive.
    pub fn too_many_threads(&self) -> bool {
        let active_threads = self.active_count.load(Ordering::Acquire);
        let num_threads = self.num_threads.load(Ordering::Relaxed);
        active_threads >= num_threads
    }

    /// Returns the next queued `Task`. The thread blocks until a new task becomes available, and
    /// since this requires holding a lock on the task `Reciever`, this also blocks any other
    /// threads that call this method. Returns `None` if the task `Sender` has hung up and there
    /// are no tasks queued. Also returns `None` if the cancelled flag has been set.
    pub fn next_task(&self) -> Option<Task<W>> {
        if self.is_cancelled() {
            None
        } else {
            self.task_rx
                .lock()
                .recv()
                .inspect(|_| {
                    self.active_count.fetch_add(1, Ordering::SeqCst);
                    self.queued_count.fetch_sub(1, Ordering::SeqCst);
                })
                .ok()
        }
    }

    /// Called by a worker thread after completing a task. Notifies any thread that has `join`ed
    /// the `Hive` if there is no more work to be done.
    pub fn finish_task(&self, panicking: bool) {
        self.active_count.fetch_sub(1, Ordering::SeqCst);
        if panicking {
            self.panic_count.fetch_add(1, Ordering::SeqCst);
        }
        self.no_work_notify_all();
    }

    /// Returns a mutable reference to the retained task outcomes.
    pub fn outcomes(&self) -> impl DerefMut<Target = HashMap<usize, Outcome<W>>> + '_ {
        self.outcomes.lock()
    }

    /// Adds a new outcome to the retained task outcomes.
    pub fn add_outcome(&self, outcome: Outcome<W>) {
        let mut lock = self.outcomes.lock();
        lock.insert(outcome.index(), outcome);
    }

    /// Removes and returns all retained task outcomes.
    pub fn take_outcomes(&self) -> HashMap<usize, Outcome<W>> {
        let mut lock = self.outcomes.lock();
        mem::take(&mut *lock)
    }

    /// Returns the number of tasks currently queued for processing.
    pub fn queued_count(&self) -> usize {
        self.queued_count.load(Ordering::Relaxed)
    }

    /// Returns the number of tasks currently being processed.
    pub fn active_count(&self) -> usize {
        self.active_count.load(Ordering::SeqCst)
    }

    /// Returns the maximum number of tasks that can be processed concurrently.
    pub fn max_count(&self) -> usize {
        self.num_threads.load(Ordering::Relaxed)
    }

    /// Returns the number of times one of this `Hive`'s worker threads has paniced.
    pub fn panic_count(&self) -> usize {
        self.panic_count.load(Ordering::Relaxed)
    }

    /// Returns `true` if there are either active tasks or if there are queued tasks and the
    /// cancelled flag hasn't been set.
    pub fn has_work(&self) -> bool {
        self.active_count.load(Ordering::SeqCst) > 0
            || (!self.is_cancelled() && self.queued_count.load(Ordering::SeqCst) > 0)
    }

    /// Notify all observers joining this hive if there is no more work to do.
    pub fn no_work_notify_all(&self) {
        if !self.has_work() {
            let _lock = self.empty_trigger.lock();
            self.empty_condvar.notify_all();
        }
    }

    /// Sets the maximum number of threads allowed in the `Hive` and returns the previous maximum.
    pub fn replace_num_threads(&self, new_count: usize) -> usize {
        self.num_threads.swap(new_count, Ordering::Release)
    }

    /// Blocks the current thread until all active tasks have been processed. Also waits until all
    /// queued tasks have been processed unless the cancelled flag has been set.
    pub fn wait_on_done(&self) {
        if self.has_work() {
            let generation = self.join_generation.load(Ordering::SeqCst);
            let mut lock = self.empty_trigger.lock();
            while generation == self.join_generation.load(Ordering::Relaxed) && self.has_work() {
                self.empty_condvar.wait(&mut lock);
            }
            // increase generation for the first thread to come out of the loop
            let _ = self.join_generation.compare_exchange(
                generation,
                generation.wrapping_add(1),
                Ordering::SeqCst,
                Ordering::Relaxed,
            );
        }
    }

    /// Consumes this `Shared` and returns a `Husk` containing the `Queen`, panic count, stored
    /// outcomes, and all configuraiton information necessary to create a new `Hive`. Any queued
    /// tasks are converted into `Outcome::Unprocessed` outcomes.
    pub fn into_husk(self) -> Husk<W, Q> {
        let rx = self.task_rx.into_inner();
        let mut outcomes = self.outcomes.into_inner();
        iter::from_fn(|| rx.try_recv().ok()).for_each(|unprocessed| {
            let (value, ctx, _) = unprocessed.into_parts();
            outcomes.insert(
                ctx.index(),
                Outcome::Unprocessed {
                    input: value,
                    index: ctx.index(),
                },
            );
        });
        Husk::new(
            self.queen.into_inner(),
            self.panic_count.into_inner(),
            outcomes,
            self.num_threads.into_inner(),
            self.name.clone(),
            self.stack_size,
            #[cfg(feature = "affinity")]
            self.affinity.into_inner(),
        )
    }
}

impl<W: Worker, Q: Queen<Kind = W>> fmt::Debug for Shared<W, Q> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Shared")
            .field("name", &self.name)
            .field("queued_count", &self.queued_count)
            .field("active_count", &self.active_count)
            .field("max_count", &self.num_threads)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::task::DefaultQueen;
    use crate::util::ThunkWorker;

    type VoidThunkWorker = ThunkWorker<()>;
    type VoidThunkWorkerShared = super::Shared<VoidThunkWorker, DefaultQueen<VoidThunkWorker>>;

    #[test]
    fn test_sync_shared() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<VoidThunkWorkerShared>();
    }

    #[test]
    fn test_send_shared() {
        fn assert_send<T: Send>() {}
        assert_send::<VoidThunkWorkerShared>();
    }
}
