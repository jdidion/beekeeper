use super::{Config, Hive};
use crate::bee::{CloneQueen, DefaultQueen, Queen, Worker};

/// A `Builder` for a `Hive`.
///
/// The configuration options available:
/// * `num_threads`: maximum number of threads that will be alive at any given moment by the built
///   [`Hive`].
/// * `thread_name`: thread name for each of the threads spawned by the built [`Hive`].
/// * `thread_stack_size`: stack size (in bytes) for each of the threads spawned by the built
///   [`Hive`].
/// * `max_retries`: maximum number of times a `Worker` will retry an [`ApplyError::Retryable`]
///   before giving up. Only available with feature `retry`.
/// * `retry_factor`: `Duration` factor for exponential backoff when retrying an
///   `ApplyError::Retryable` error. Only available with feature `retry`.
/// * `affinity`: List of CPU core indicies to which the threads should be pinned. Only available
///   with feature `affinity`.
///
/// Calling `Builder::new()` creates an unconfigured `Builder`, while calling `Builder::default()`
/// creates a `Builder` with `num_threads`, `max_retries`, and `retry_factor` set to the global
/// default values, which can be changed using the `drudge::hive::set_*_default` functions.
///
/// [`Hive`]: hive/struct.Hive.html
/// [`ApplyError::Retryable`]: task/enum.ApplyError.html#variant.Retryable
///
/// # Examples
///
/// Build a [`Hive`] that uses a maximum of eight threads simultaneously and each thread has
/// a 8 MB stack size:
///
/// ```
/// type MyWorker = drudge::bee::stock::ThunkWorker<()>;
///
/// let hive = drudge::hive::Builder::new()
///     .num_threads(8)
///     .thread_stack_size(8_000_000)
///     .build_with_default::<MyWorker>();
/// ```
#[derive(Clone)]
pub struct Builder(Config);

impl Builder {
    /// Returns a new `Builder` with no options configured.
    pub fn new() -> Self {
        Self(Config::empty())
    }

    /// Sets the maximum number of worker threads that will be alive at any given moment in the
    /// built [`Hive`]. If not specified, the built `Hive` will not be initialized with worker
    /// threads until [`Hive::set_num_threads`] is called.
    ///
    /// [`Hive`]: hive/struct.Hive.html
    ///
    /// # Examples
    ///
    /// No more than eight threads will be alive simultaneously for this hive:
    ///
    /// ```
    /// use drudge::bee::stock::{Thunk, ThunkWorker};
    /// use drudge::hive::{Builder, Hive};
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .num_threads(8)
    ///     .build_with_default::<ThunkWorker<()>>();
    ///
    /// for _ in 0..100 {
    ///     hive.apply_store(Thunk::of(|| {
    ///         println!("Hello from a worker thread!")
    ///     }));
    /// }
    /// # }
    /// ```
    pub fn num_threads(mut self, num: usize) -> Self {
        let _ = self.0.num_threads.set(Some(num));
        self
    }

    /// Sets the number of worker threads to the global default value.
    pub fn with_default_num_threads(mut self) -> Self {
        let _ = self
            .0
            .num_threads
            .set(super::config::DEFAULTS.lock().num_threads.get());
        self
    }

    /// Specifies that the built [`Hive`] will use all available CPU cores for worker threads.
    ///
    /// [`Hive`]: hive/struct.Hive.html
    ///
    /// # Examples
    ///
    /// All available threads will be alive simultaneously for this pool:
    ///
    /// ```
    /// use drudge::bee::stock::{Thunk, ThunkWorker};
    /// use drudge::hive::{Builder, Hive};
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .thread_per_core()
    ///     .build_with_default::<ThunkWorker<()>>();
    ///
    /// for _ in 0..100 {
    ///     hive.apply_store(Thunk::of(|| {
    ///         println!("Hello from a worker thread!")
    ///     }));
    /// }
    /// # }
    /// ```
    pub fn with_thread_per_core(mut self) -> Self {
        let _ = self.0.num_threads.set(Some(num_cpus::get()));
        self
    }

    /// Sets the thread name for each of the threads spawned by the built [`Hive`]. If not
    /// specified, threads spawned by the thread pool will be unnamed.
    ///
    /// [`Hive`]: hive/struct.Hive.html
    ///
    /// # Examples
    ///
    /// Each thread spawned by this hive will have the name "foo":
    ///
    /// ```
    /// use drudge::bee::stock::{Thunk, ThunkWorker};
    /// use drudge::hive::{Builder, Hive};
    /// use std::thread;
    ///
    /// # fn main() {
    /// let hive = Builder::default()
    ///     .thread_name("foo")
    ///     .build_with_default::<ThunkWorker<()>>();
    ///
    /// for _ in 0..100 {
    ///     hive.apply_store(Thunk::of(|| {
    ///         assert_eq!(thread::current().name(), Some("foo"));
    ///     }));
    /// }
    /// # hive.join();
    /// # }
    /// ```
    pub fn thread_name<T: Into<String>>(mut self, name: T) -> Self {
        let _ = self.0.thread_name.set(Some(name.into()));
        self
    }

    /// Sets the stack size (in bytes) for each of the threads spawned by the built [`Hive`].
    /// If not specified, threads spawned by the hive will have a stack size [as specified in
    /// the `std::thread` documentation][thread].
    ///
    /// [thread]: https://doc.rust-lang.org/nightly/std/thread/index.html#stack-size
    /// [`Hive`]: hive/struct.Hive.html
    ///
    /// # Examples
    ///
    /// Each thread spawned by this hive will have a 4 MB stack:
    ///
    /// ```
    /// use drudge::bee::stock::{Thunk, ThunkWorker};
    /// use drudge::hive::{Builder, Hive};
    ///
    /// # fn main() {
    /// let hive = Builder::default()
    ///     .thread_stack_size(4_000_000)
    ///     .build_with_default::<ThunkWorker<()>>();
    ///
    /// for _ in 0..100 {
    ///     hive.apply_store(Thunk::of(|| {
    ///         println!("This thread has a 4 MB stack size!");
    ///     }));
    /// }
    /// # hive.join();
    /// # }
    /// ```
    pub fn thread_stack_size(mut self, size: usize) -> Self {
        let _ = self.0.thread_stack_size.set(Some(size));
        self
    }

    /// Consumes this `Builder` and returns a new `Hive` using the given `Queen` to create
    /// `Worker`s.
    ///
    /// # Examples
    ///
    /// ```
    /// # use drudge::hive::{Builder, Hive};
    /// # use drudge::bee::{Context, Queen, Worker, WorkerResult};
    ///
    /// #[derive(Debug)]
    /// struct CounterWorker {
    ///     index: usize,
    ///     input_count: usize,
    ///     input_sum: usize,
    /// }
    ///
    /// impl CounterWorker {
    ///     fn new(index: usize) -> Self {
    ///         Self {
    ///             index,
    ///             input_count: 0,
    ///             input_sum: 0,
    ///         }
    ///     }
    /// }
    ///
    /// impl Worker for CounterWorker {
    ///     type Input = usize;
    ///     type Output = String;
    ///     type Error = ();
    ///
    ///     fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
    ///         self.input_count += 1;
    ///         self.input_sum += input;
    ///         let s = format!(
    ///             "CounterWorker {}: Input {}, Count {}, Sum {}",
    ///             self.index, input, self.input_count, self.input_sum
    ///         );
    ///         Ok(s)
    ///     }
    /// }
    ///
    /// #[derive(Debug, Default)]
    /// struct CounterQueen {
    ///     num_workers: usize
    /// }
    ///
    /// impl Queen for CounterQueen {
    ///     type Kind = CounterWorker;
    ///
    ///     fn create(&mut self) -> Self::Kind {
    ///         self.num_workers += 1;
    ///         CounterWorker::new(self.num_workers)
    ///     }
    /// }
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .num_threads(8)
    ///     .thread_stack_size(4_000_000)
    ///     .build(CounterQueen::default());
    ///
    /// for i in 0..100 {
    ///     hive.apply_store(i);
    /// }
    /// let husk = hive.into_husk();
    /// assert_eq!(husk.queen().num_workers, 8);
    /// # }
    /// ```
    pub fn build<W: Worker, Q: Queen<Kind = W>>(self, queen: Q) -> Hive<W, Q> {
        Hive::new(self.0, queen)
    }

    /// Consumes this `Builder` and returns a new `Hive` using a `Queen` created with
    /// `Q::default()` to create `Worker`s.
    pub fn build_default<W: Worker, Q: Queen<Kind = W> + Default>(self) -> Hive<W, Q> {
        Hive::new(self.0, Q::default())
    }

    /// Consumes this `Builder` and returns a new `Hive` with `Worker`s created by cloning
    /// `worker`.
    ///
    /// # Examples
    /// ```
    /// # use drudge::hive::{Builder, OutcomeIteratorExt};
    /// # use drudge::bee::{Context, Worker, WorkerResult};
    ///
    /// #[derive(Debug, Clone)]
    /// struct MathWorker(isize);
    ///
    /// impl MathWorker {
    ///     fn new(left_operand: isize) -> Self {
    ///         assert!(left_operand != 0);
    ///         Self(left_operand)
    ///     }
    /// }
    ///
    /// impl Worker for MathWorker {
    ///     type Input = (isize, u8);
    ///     type Output = isize;
    ///     type Error = ();
    ///
    ///     fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
    ///         let (operand, operator) = input;
    ///         let value = match operator % 4 {
    ///             0 => operand + self.0,
    ///             1 => operand - self.0,
    ///             2 => operand * self.0,
    ///             3 => operand / self.0,
    ///             _ => unreachable!(),
    ///         };
    ///         Ok(value)
    ///     }
    /// }
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .num_threads(8)
    ///     .thread_stack_size(4_000_000)
    ///     .build_with(MathWorker(5isize));
    ///
    /// let sum: isize = hive.map((0..100).zip((0..4).cycle())).into_outputs().sum();
    /// assert_eq!(sum, 8920);
    /// # }
    /// ```
    pub fn build_with<W>(self, worker: W) -> Hive<W, CloneQueen<W>>
    where
        W: Worker + Send + Sync + Clone,
    {
        Hive::new(self.0, CloneQueen::new(worker))
    }

    /// Consumes this `Builder` and returns a new `Hive` with `Worker`s created using
    /// `W::default()`.
    ///
    /// # Examples
    /// ```
    /// # use drudge::hive::{Builder, OutcomeIteratorExt};
    /// # use drudge::bee::{Context, Worker,  WorkerResult};
    /// # use std::num::NonZeroIsize;
    ///
    /// #[derive(Debug, Default)]
    /// struct MathWorker(isize); // value is always `0`
    ///
    /// impl Worker for MathWorker {
    ///     type Input = (NonZeroIsize, u8);
    ///     type Output = isize;
    ///     type Error = ();
    ///
    ///     fn apply(&mut self, input: Self::Input, _: &Context) -> WorkerResult<Self> {
    ///         let (operand, operator) = input;
    ///         let result = match operator % 4 {
    ///             0 => self.0 + operand.get(),
    ///             1 => self.0 - operand.get(),
    ///             2 => self.0 * operand.get(),
    ///             3 => self.0 / operand.get(),
    ///             _ => unreachable!(),
    ///         };
    ///         Ok(result)
    ///     }
    /// }
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .num_threads(8)
    ///     .thread_stack_size(4_000_000)
    ///     .build_with_default::<MathWorker>();
    ///
    /// let sum: isize = hive.map(
    ///     (1..=100).map(|i| NonZeroIsize::new(i).unwrap()).zip((0..4).cycle())
    /// ).into_outputs().sum();
    /// assert_eq!(sum, -25);
    /// # }
    /// ```
    pub fn build_with_default<W: Worker + Send + Sync + Default>(self) -> Hive<W, DefaultQueen<W>> {
        Hive::new(self.0, DefaultQueen::default())
    }
}

impl Default for Builder {
    /// Creates a new `Builder` with default configuration options:
    /// * `num_threads = DEFAULT_THREADS`
    ///
    /// The following default configuration options are used when the `retry` feature is enabled:
    /// * `max_retries = DEFAULT_RETRIES`
    /// * `retry_factor = DEFAULT_RETRY_FACTOR`
    fn default() -> Self {
        Builder(Config::with_defaults())
    }
}

impl From<Config> for Builder {
    fn from(value: Config) -> Self {
        Self(value)
    }
}

#[cfg(feature = "affinity")]
mod affinity {
    use super::Builder;
    use crate::hive::cores::Cores;

    impl Builder {
        /// Sets set list of CPU core indicies to which threads in the `Hive` should be pinned.
        ///
        /// Core indices are integers in the range `0..N`, where `N` is the number of available CPU
        /// cores as reported by `num_cpus::get()`. The mapping between core indicies and core IDs is
        /// platform-specific. All CPU cores on a given system should be equivalent, and thus it does
        /// not matter which cores are pinned so long as a core is not pinned to multiple threads.
        ///
        /// Excess core indicies (i.e. if `affinity.len() > num_threads`) are ignored. If
        /// `affinity.len() < num_threads` then the excess threads will not be pinned.
        ///
        /// # Examples
        ///
        /// Each thread spawned by this hive will be pinned to a core:
        ///
        /// ```
        /// use drudge::bee::stock::{Thunk, ThunkWorker};
        /// use drudge::hive::{Builder, Hive};
        ///
        /// # fn main() {
        /// let hive = Builder::new()
        ///     .num_threads(4)
        ///     .thread_affinity(0..4)
        ///     .build_with_default::<ThunkWorker<()>>();
        ///
        /// for _ in 0..100 {
        ///     hive.apply_store(Thunk::of(|| {
        ///         println!("This thread is pinned!");
        ///     }));
        /// }
        /// # hive.join();
        /// # }
        /// ```
        pub fn thread_affinity<C: Into<Cores>>(mut self, affinity: C) -> Self {
            let _ = self.0.affinity.set(Some(affinity.into()));
            self
        }

        /// Specifies that worker threads should be pinned to all available CPU cores. If `num_threads`
        /// is greater than the available number of CPU cores, then some threads might not be pinned.
        pub fn with_default_thread_affinity(mut self) -> Self {
            let _ = self.0.affinity.set(Some(Cores::all()));
            self
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::hive::cores::Cores;
        use crate::hive::Builder;

        #[test]
        fn test_with_affinity() {
            let mut builder = Builder::new();
            builder = builder.with_default_thread_affinity();
            assert_eq!(builder.0.affinity.get(), Some(Cores::all()));
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use super::Builder;
    use std::time::Duration;

    impl Builder {
        /// Sets the maximum number of times to retry an [`ApplyError::Retryable`] error. A worker
        /// thread will retry a task until it either returns `Err(ApplyError::NotRetryable)` or the
        /// maximum number of retries is reached. Each time a task is retried, the worker thread will
        /// first sleep for `retry_factor * (2 ** (attempt - 1))` before attempting the task again. If
        /// not specified, tasks are retried a default number of times. Set to `0` to disable retrying.
        ///
        /// [`ApplyError::Retryable`]: enum.ApplyError.html#variant.Retryable
        /// [`Hive`]: hive/struct.Hive.html
        ///
        /// # Examples
        ///
        /// ```
        /// use drudge::bee::{ApplyError, Context};
        /// use drudge::bee::stock::RetryCaller;
        /// use drudge::hive::{Builder, Hive};
        /// use std::time;
        ///
        /// fn sometimes_fail(
        ///     i: usize,
        ///     _: &Context
        /// ) -> Result<String, ApplyError<usize, String>> {
        ///     match i % 3 {
        ///         0 => Ok("Success".into()),
        ///         1 => Err(ApplyError::Retryable { input: i, error: "Retryable".into() }),
        ///         2 => Err(ApplyError::Fatal { input: Some(i), error: "NotRetryable".into() }),
        ///         _ => unreachable!(),
        ///     }
        /// }
        ///
        /// # fn main() {
        /// let hive = Builder::default()
        ///     .max_retries(3)
        ///     .build_with(RetryCaller::of(sometimes_fail));
        ///
        /// for i in 0..10 {
        ///     hive.apply_store(i);
        /// }
        /// # hive.join();
        /// # }
        /// ```
        pub fn max_retries(mut self, limit: u32) -> Self {
            let _ = if limit == 0 {
                self.0.max_retries.set(None)
            } else {
                self.0.max_retries.set(Some(limit))
            };
            self
        }

        /// Sets the exponential back-off factor for retrying tasks. Each time a task is retried, the
        /// thread will first sleep for `retry_factor * (2 ** (attempt - 1))`. If not specififed, a
        /// default retry factor is used. Set to `Duration::ZERO` to disable exponential backoff.
        ///
        /// # Examples
        ///
        /// ```
        /// use drudge::bee::{ApplyError, Context};
        /// use drudge::bee::stock::RetryCaller;
        /// use drudge::hive::{Builder, Hive};
        /// use std::time;
        ///
        /// fn echo_time(i: usize, ctx: &Context) -> Result<String, ApplyError<usize, String>> {
        ///     let attempt = ctx.attempt();
        ///     if attempt == 3 {
        ///         Ok("Success".into())
        ///     } else {
        ///         // the delay between each message should be exponential
        ///         println!("Task {} attempt {}: {:?}", i, attempt, time::SystemTime::now());
        ///         Err(ApplyError::Retryable { input: i, error: "Retryable".into() })
        ///     }
        /// }
        ///
        /// # fn main() {
        /// let hive = Builder::default()
        ///     .max_retries(3)
        ///     .retry_factor(time::Duration::from_secs(1))
        ///     .build_with(RetryCaller::of(echo_time));
        ///
        /// for i in 0..10 {
        ///     hive.apply_store(i);
        /// }
        /// # hive.join();
        /// # }
        /// ```
        pub fn retry_factor(mut self, duration: Duration) -> Self {
            let _ = if duration == Duration::ZERO {
                self.0.retry_factor.set(None)
            } else {
                self.0.set_retry_factor_from(duration)
            };
            self
        }

        /// Sets retry parameters to their default values.
        pub fn with_default_retries(mut self) -> Self {
            let defaults = crate::hive::config::DEFAULTS.lock();
            let _ = self.0.max_retries.set(defaults.max_retries.get());
            let _ = self.0.retry_factor.set(defaults.retry_factor.get());
            self
        }

        /// Disables retrying tasks.
        pub fn with_no_retries(self) -> Self {
            self.max_retries(0).retry_factor(Duration::ZERO)
        }
    }
}
