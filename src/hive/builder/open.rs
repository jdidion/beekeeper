use super::{BeeBuilder, BuilderConfig, ChannelBuilder, Token, WorkstealingBuilder};
use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut, Worker};
use crate::hive::Config;

/// A builder for a [`Hive`](crate::hive::Hive).
///
/// Calling [`Builder::new()`] creates an unconfigured `Builder`, while calling
/// [`Builder::default()`] creates a `Builder` with fields preset to the global default values.
/// Global defaults can be changed using the
/// [`beekeeper::hive::set_*_default`](crate::hive#functions) functions.
///
/// The configuration options available:
/// * [`Builder::num_threads`]: number of worker threads that will be spawned by the built `Hive`.
///     * [`Builder::with_default_num_threads`] will set `num_threads` to the global default value.
///     * [`Builder::with_thread_per_core`] will set `num_threads` to the number of available CPU
///       cores.
/// * [`Builder::thread_name`]: thread name for each of the threads spawned by the built `Hive`. By
///   default, threads are unnamed.
/// * [`Builder::thread_stack_size`]: stack size (in bytes) for each of the threads spawned by the
///   built `Hive`. See the
///   [`std::thread`](https://doc.rust-lang.org/stable/std/thread/index.html#stack-size)
///   documentation for details on the default stack size.
///
/// The following configuration options are available when the `retry` feature is enabled:
/// * [`Builder::max_retries`]: maximum number of times a `Worker` will retry an
///   [`ApplyError::Retryable`](crate::bee::ApplyError#Retryable) before giving up.
/// * [`Builder::retry_factor`]: [`Duration`](std::time::Duration) factor for exponential backoff
///   when retrying an `ApplyError::Retryable` error.
/// * [`Builder::with_default_retries`] sets the retry options to the global defaults, while
///   [`Builder::with_no_retries`] disabled retrying.
///
/// The following configuration options are available when the `affinity` feature is enabled:
/// * [`Builder::core_affinity`]: List of CPU core indices to which the threads should be pinned.
///     * [`Builder::with_default_core_affinity`] will set the list to all CPU core indices, though
///       only the first `num_threads` indices will be used.
///
/// To create the [`Hive`], call one of the `build*` methods:
/// * [`Builder::build`] requires a [`Queen`] instance.
/// * [`Builder::build_default`] requires a [`Queen`] type that implements [`Default`].
/// * [`Builder::build_with`] requires a [`Worker`] instance that implements [`Clone`].
/// * [`Builder::build_with_default`] requires a [`Worker`] type that implements [`Default`].
///
/// # Examples
///
/// Build a [`Hive`] that uses a maximum of eight threads simultaneously and each thread has
/// a 8 MB stack size:
///
/// ```
/// type MyWorker = beekeeper::bee::stock::ThunkWorker<()>;
///
/// let hive = beekeeper::hive::Builder::empty()
///     .num_threads(8)
///     .thread_stack_size(8_000_000)
///     .with_worker_default::<MyWorker>()
///     .with_channel_queues()
///     .build();
/// ```
#[derive(Clone, Default)]
pub struct OpenBuilder(Config);

impl OpenBuilder {
    /// Returns a new `Builder` with no options configured.
    pub fn empty() -> Self {
        Self(Config::empty())
    }

    /// Consumes this `Builder` and returns a new [`BeeBuilder`] using the given [`Queen`] to
    /// create [`Worker`]s.
    ///
    /// # Examples
    ///
    /// ```
    /// # use beekeeper::hive::{Builder, Hive};
    /// # use beekeeper::bee::{Context, Queen, Worker, WorkerResult};
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
    /// let husk = hive.try_into_husk().unwrap();
    /// assert_eq!(husk.queen().num_workers, 8);
    /// # }
    /// ```
    pub fn with_queen<Q: Queen, I: Into<Q>>(self, queen: I) -> BeeBuilder<Q> {
        BeeBuilder::from(self.0, queen.into())
    }

    /// Consumes this `Builder` and returns a new [`BeeBuilder`] using a [`Queen`] created with
    /// [`Q::default()`](std::default::Default) to create [`Worker`]s.
    pub fn with_queen_default<Q: Queen + Default>(self) -> BeeBuilder<Q> {
        BeeBuilder::from(self.0, Q::default())
    }

    /// Consumes this `Builder` and returns a new [`BeeBuilder`] using a [`QueenMut`] created with
    /// [`Q::default()`](std::default::Default) to create [`Worker`]s.
    pub fn with_queen_mut_default<Q: QueenMut + Default>(self) -> BeeBuilder<QueenCell<Q>> {
        BeeBuilder::from(self.0, QueenCell::new(Q::default()))
    }

    /// Consumes this `Builder` and returns a new [`BeeBuilder`] with [`Worker`]s created by
    /// cloning `worker`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use beekeeper::hive::{Builder, OutcomeIteratorExt};
    /// # use beekeeper::bee::{Context, Worker, WorkerResult};
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
    ///             0 => operand + self.config(Token),
    ///             1 => operand - self.config(Token),
    ///             2 => operand * self.config(Token),
    ///             3 => operand / self.config(Token),
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
    /// let sum: isize = hive
    ///     .map((0..100).zip((0..4).cycle()))
    ///     .into_outputs()
    ///     .sum();
    /// assert_eq!(sum, 8920);
    /// # }
    /// ```
    pub fn with_worker<W>(self, worker: W) -> BeeBuilder<CloneQueen<W>>
    where
        W: Worker + Send + Sync + Clone,
    {
        BeeBuilder::from(self.0, CloneQueen::new(worker))
    }

    /// Consumes this `Builder` and returns a new [`BeeBuilder`] with [`Worker`]s created using
    /// [`W::default()`](std::default::Default).
    ///
    /// # Examples
    ///
    /// ```
    /// # use beekeeper::hive::{Builder, OutcomeIteratorExt};
    /// # use beekeeper::bee::{Context, Worker,  WorkerResult};
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
    ///             0 => self.config(Token) + operand.get(),
    ///             1 => self.config(Token) - operand.get(),
    ///             2 => self.config(Token) * operand.get(),
    ///             3 => self.config(Token) / operand.get(),
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
    /// let sum: isize = hive
    ///     .map((1..=100).map(|i| NonZeroIsize::new(i).unwrap()).zip((0..4).cycle()))
    ///     .into_outputs()
    ///     .sum();
    /// assert_eq!(sum, -25);
    /// # }
    /// ```
    pub fn with_worker_default<W>(self) -> BeeBuilder<DefaultQueen<W>>
    where
        W: Worker + Send + Sync + Default,
    {
        BeeBuilder::from(self.0, DefaultQueen::default())
    }

    /// Consumes this `Builder` and returns a new [`ChannelBuilder`] using the current
    /// configuration.
    pub fn with_channel_queues(self) -> ChannelBuilder {
        ChannelBuilder::from(self.0)
    }

    pub fn with_workstealing_queues(self) -> WorkstealingBuilder {
        WorkstealingBuilder::from(self.0)
    }
}

impl BuilderConfig for OpenBuilder {
    fn config(&mut self, _: Token) -> &mut Config {
        &mut self.0
    }
}

impl From<Config> for OpenBuilder {
    fn from(value: Config) -> Self {
        Self(value)
    }
}
