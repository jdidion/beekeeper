use super::{Config, Token};

/// Private (sealed) trait depended on by `Builder` that must be implemented by builder types.
pub trait BuilderConfig {
    /// Returns a reference to the underlying `Config`.
    fn config(&mut self, token: Token) -> &mut Config;
}

/// Trait that provides `Builder` types with methods for setting configuration parameters.
///
/// This is a sealed trait, meaning it cannot be implemented outside of this crate.
pub trait Builder: BuilderConfig + Sized {
    /// Sets the maximum number of worker threads that will be alive at any given moment in the
    /// built [`Hive`]. If not specified, the built `Hive` will not be initialized with worker
    /// threads until [`Hive::grow`] is called.
    ///
    /// # Examples
    ///
    /// No more than eight threads will be alive simultaneously for this hive:
    ///
    /// ```
    /// use beekeeper::bee::stock::{Thunk, ThunkWorker};
    /// use beekeeper::hive::{Builder, Hive};
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
    fn num_threads(mut self, num: usize) -> Self {
        let _ = self.config(Token).num_threads.set(Some(num));
        self
    }

    /// Sets the number of worker threads to the global default value.
    fn with_default_num_threads(mut self) -> Self {
        let _ = self
            .config(Token)
            .num_threads
            .set(super::config::DEFAULTS.lock().num_threads.get());
        self
    }

    /// Specifies that the built [`Hive`] will use all available CPU cores for worker threads.
    ///
    /// # Examples
    ///
    /// All available threads will be alive simultaneously for this hive:
    ///
    /// ```
    /// use beekeeper::bee::stock::{Thunk, ThunkWorker};
    /// use beekeeper::hive::{Builder, Hive};
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .with_thread_per_core()
    ///     .build_with_default::<ThunkWorker<()>>();
    ///
    /// for _ in 0..100 {
    ///     hive.apply_store(Thunk::of(|| {
    ///         println!("Hello from a worker thread!")
    ///     }));
    /// }
    /// # }
    /// ```
    fn with_thread_per_core(mut self) -> Self {
        let _ = self.config(Token).num_threads.set(Some(num_cpus::get()));
        self
    }

    /// Sets the thread name for each of the threads spawned by the built [`Hive`]. If not
    /// specified, threads spawned by the thread pool will be unnamed.
    ///
    /// # Examples
    ///
    /// Each thread spawned by this hive will have the name `"foo"`:
    ///
    /// ```
    /// use beekeeper::bee::stock::{Thunk, ThunkWorker};
    /// use beekeeper::hive::{Builder, Hive};
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
    fn thread_name<T: Into<String>>(mut self, name: T) -> Self {
        let _ = self.config(Token).thread_name.set(Some(name.into()));
        self
    }

    /// Sets the stack size (in bytes) for each of the threads spawned by the built [`Hive`].
    /// If not specified, threads spawned by the hive will have a stack size [as specified in
    /// the `std::thread` documentation][thread].
    ///
    /// [thread]: https://doc.rust-lang.org/nightly/std/thread/index.html#stack-size
    ///
    /// # Examples
    ///
    /// Each thread spawned by this hive will have a 4 MB stack:
    ///
    /// ```
    /// use beekeeper::bee::stock::{Thunk, ThunkWorker};
    /// use beekeeper::hive::{Builder, Hive};
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
    fn thread_stack_size(mut self, size: usize) -> Self {
        let _ = self.config(Token).thread_stack_size.set(Some(size));
        self
    }

    /// Sets set list of CPU core indices to which threads in the `Hive` should be pinned.
    ///
    /// Core indices are integers in the range `0..N`, where `N` is the number of available CPU
    /// cores as reported by [`num_cpus::get()`]. The mapping between core indices and core IDs
    /// is platform-specific. All CPU cores on a given system should be equivalent, and thus it
    /// does not matter which cores are pinned so long as a core is not pinned to multiple
    /// threads.
    ///
    /// Excess core indices (i.e., if `affinity.len() > num_threads`) are ignored. If
    /// `affinity.len() < num_threads` then the excess threads will not be pinned.
    ///
    /// # Examples
    ///
    /// Each thread spawned by this hive will be pinned to a core:
    ///
    /// ```
    /// use beekeeper::bee::stock::{Thunk, ThunkWorker};
    /// use beekeeper::hive::{Builder, Hive};
    ///
    /// # fn main() {
    /// let hive = Builder::new()
    ///     .num_threads(4)
    ///     .core_affinity(0..4)
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
    #[cfg(feature = "affinity")]
    fn core_affinity<C: Into<crate::hive::cores::Cores>>(mut self, affinity: C) -> Self {
        let _ = self.config(Token).affinity.set(Some(affinity.into()));
        self
    }

    /// Specifies that worker threads should be pinned to all available CPU cores. If
    /// `num_threads` is greater than the available number of CPU cores, then some threads
    /// might not be pinned.
    #[cfg(feature = "affinity")]
    fn with_default_core_affinity(mut self) -> Self {
        let _ = self
            .config(Token)
            .affinity
            .set(Some(crate::hive::cores::Cores::all()));
        self
    }

    /// Sets the worker thread batch size. If `batch_size` is `0`, batching is disabled, but
    /// note that the performance may be worse than with the `batching` feature disabled.
    #[cfg(feature = "batching")]
    fn batch_size(mut self, batch_size: usize) -> Self {
        if batch_size == 0 {
            self.config(Token).batch_size.set(None);
        } else {
            self.config(Token).batch_size.set(Some(batch_size));
        }
        self
    }

    /// Sets the worker thread batch size to the global default value.
    #[cfg(feature = "batching")]
    fn with_default_batch_size(mut self) -> Self {
        let _ = self
            .config(Token)
            .batch_size
            .set(super::config::DEFAULTS.lock().batch_size.get());
        self
    }

    /// Sets the maximum number of times to retry a
    /// [`ApplyError::Retryable`](crate::bee::ApplyError::Retryable) error. A worker
    /// thread will retry a task until it either returns
    /// [`ApplyError::Fatal`](crate::bee::ApplyError::Fatal) or the maximum number of retries is
    /// reached. Each time a task is retried, the worker thread will first sleep for
    /// `retry_factor * (2 ** (attempt - 1))` before attempting the task again. If not
    /// specified, tasks are retried a default number of times. If set to `0`, tasks will be
    /// retried immediately without delay.
    ///
    /// # Examples
    ///
    /// ```
    /// use beekeeper::bee::{ApplyError, Context};
    /// use beekeeper::bee::stock::RetryCaller;
    /// use beekeeper::hive::{Builder, Hive};
    /// use std::time;
    ///
    /// fn sometimes_fail(
    ///     i: usize,
    ///     _: &Context
    /// ) -> Result<String, ApplyError<usize, String>> {
    ///     match i % 3 {
    ///         0 => Ok("Success".into()),
    ///         1 => Err(ApplyError::Retryable { input: i, error: "Retryable".into() }),
    ///         2 => Err(ApplyError::Fatal { input: Some(i), error: "Fatal".into() }),
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
    #[cfg(feature = "retry")]
    fn max_retries(mut self, limit: u32) -> Self {
        let _ = if limit == 0 {
            self.config(Token).max_retries.set(None)
        } else {
            self.config(Token).max_retries.set(Some(limit))
        };
        self
    }

    /// Sets the exponential back-off factor for retrying tasks. Each time a task is retried,
    /// the thread will first sleep for `retry_factor * (2 ** (attempt - 1))`. If not
    /// specififed, a default retry factor is used. Set to
    /// [`Duration::ZERO`](std::time::Duration::ZERO) to disableexponential backoff.
    ///
    /// # Examples
    ///
    /// ```
    /// use beekeeper::bee::{ApplyError, Context};
    /// use beekeeper::bee::stock::RetryCaller;
    /// use beekeeper::hive::{Builder, Hive};
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
    #[cfg(feature = "retry")]
    fn retry_factor(mut self, duration: std::time::Duration) -> Self {
        let _ = if duration == std::time::Duration::ZERO {
            self.config(Token).retry_factor.set(None)
        } else {
            self.config(Token).set_retry_factor_from(duration)
        };
        self
    }

    /// Sets retry parameters to their default values.
    #[cfg(feature = "retry")]
    fn with_default_retries(mut self) -> Self {
        let defaults = super::config::DEFAULTS.lock();
        let _ = self
            .config(Token)
            .max_retries
            .set(defaults.max_retries.get());
        let _ = self
            .config(Token)
            .retry_factor
            .set(defaults.retry_factor.get());
        self
    }

    /// Disables retrying tasks.
    #[cfg(feature = "retry")]
    fn with_no_retries(self) -> Self {
        self.max_retries(0).retry_factor(std::time::Duration::ZERO)
    }
}

impl<B: BuilderConfig> Builder for B {}
