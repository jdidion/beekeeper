use super::{BeeBuilder, BuilderConfig, ChannelBuilder, Token, WorkstealingBuilder};
use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut, Worker};
use crate::hive::Config;

/// A builder for a [`Hive`](crate::hive::Hive).
///
/// Calling [`OpenBuilder::empty()`] creates an unconfigured `Builder`, while calling
/// [`OpenBuilder::default()`] creates a `Builder` with fields preset to the global default values.
/// Global defaults can be changed using the
/// [`beekeeper::hive::set_*_default`](crate::hive#functions) functions.
///
/// See the [module documentation](crate::hive::builder) for details on the available configuration
/// options.
///
/// This builder needs to be specialized to both the `Queen` and `TaskQueues` types. You can do
/// this in either order.
///
/// * Calling one of the `with_queen*` methods returns a `BeeBuilder` specialized to a `Queen`.
/// * Calling `with_worker` or `with_worker_default` returns a `BeeBuilder` specialized to a
///   `CloneQueen` or `DefaultQueen` (respectively) for a specific `Worker` type.
/// * Calling `with_channel_queues` or `with_workstealing_queues` returns a `ChannelBuilder` or
///   `WorkstealingBuilder` specialized to a `TaskQueues` type.
///
/// # Examples
///
/// Build a [`Hive`] that uses a maximum of eight threads simultaneously and each thread has
/// a 8 MB stack size:
///
/// ```
/// # use beekeeper::hive::{Builder, OpenBuilder};
/// type MyWorker = beekeeper::bee::stock::ThunkWorker<()>;
///
/// let hive = OpenBuilder::empty()
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
    /// # use beekeeper::hive::prelude::*;
    /// # use beekeeper::bee::{Context, QueenMut, Worker, WorkerResult};
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
    ///     fn apply(&mut self, input: Self::Input, _: &Context<usize>) -> WorkerResult<Self> {
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
    /// impl QueenMut for CounterQueen {
    ///     type Kind = CounterWorker;
    ///
    ///     fn create(&mut self) -> Self::Kind {
    ///         self.num_workers += 1;
    ///         CounterWorker::new(self.num_workers)
    ///     }
    /// }
    ///
    /// # fn main() {
    /// let hive = channel_builder(false)
    ///     .num_threads(8)
    ///     .thread_stack_size(4_000_000)
    ///     .with_queen_mut_default::<CounterQueen>()
    ///     .build();
    ///
    /// for i in 0..100 {
    ///     hive.apply_store(i);
    /// }
    /// let husk = hive.try_into_husk(false).unwrap();
    /// assert_eq!(husk.queen().get().num_workers, 8);
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
    /// # use beekeeper::hive::prelude::*;
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
    ///     fn apply(&mut self, input: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
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
    /// let hive = channel_builder(false)
    ///     .num_threads(8)
    ///     .thread_stack_size(4_000_000)
    ///     .with_worker(MathWorker(5isize))
    ///     .build();
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
    /// # use beekeeper::hive::prelude::*;
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
    ///     fn apply(&mut self, input: Self::Input, _: &Context<Self::Input>) -> WorkerResult<Self> {
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
    /// let hive = channel_builder(false)
    ///     .num_threads(8)
    ///     .thread_stack_size(4_000_000)
    ///     .with_worker_default::<MathWorker>()
    ///     .build();
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

    /// Consumes this `Builder` and returns a new [`WorkstealingBuilder`] using the current
    /// configuration.
    pub fn with_workstealing_queues(self) -> WorkstealingBuilder {
        WorkstealingBuilder::from(self.0)
    }
}

impl BuilderConfig for OpenBuilder {
    fn config_ref(&mut self, _: Token) -> &mut Config {
        &mut self.0
    }
}

impl From<Config> for OpenBuilder {
    fn from(value: Config) -> Self {
        Self(value)
    }
}
