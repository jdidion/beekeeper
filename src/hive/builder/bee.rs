use super::{BuilderConfig, FullBuilder, Token};
use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut, Worker};
use crate::hive::{ChannelTaskQueues, Config, TaskQueues, WorkstealingTaskQueues};
use derive_more::Debug;
use std::any;

/// A Builder for creating `Hive` instances for specific [`Worker`] and [`TaskQueues`] types.
#[derive(Clone, Default, Debug)]
pub struct BeeBuilder<Q: Queen> {
    config: Config,
    #[debug("{}",any::type_name::<Q>())]
    queen: Q,
}

impl<Q: Queen> BeeBuilder<Q> {
    /// Creates a new `BeeBuilder` with the given queen and no options configured.
    pub fn empty<I: Into<Q>>(queen: Q) -> Self {
        Self {
            config: Config::empty(),
            queen,
        }
    }

    /// Creates a new `BeeBuilder` with the given `queen` and options configured with global
    /// preset values.
    pub fn preset<I: Into<Q>>(queen: Q) -> Self {
        Self {
            config: Config::default(),
            queen,
        }
    }

    /// Creates a new `BeeBuilder` from an existing `config` and a `queen`.
    pub(super) fn from_config_and_queen(config: Config, queen: Q) -> Self {
        Self { config, queen }
    }

    /// Creates a new `FullBuilder` with the current configuration and queen and specified
    /// `TaskQueues` type.
    pub fn with_queues<T: TaskQueues<Q::Kind>>(self) -> FullBuilder<Q, T> {
        FullBuilder::from_config_and_queen(self.config, self.queen)
    }

    /// Creates a new `FullBuilder` with the current configuration and queen and channel-based
    /// task queues.
    pub fn with_channel_queues(self) -> FullBuilder<Q, ChannelTaskQueues<Q::Kind>> {
        FullBuilder::from_config_and_queen(self.config, self.queen)
    }

    /// Creates a new `FullBuilder` with the current configuration and queen and workstealing
    /// task queues.
    pub fn with_workstealing_queues(self) -> FullBuilder<Q, WorkstealingTaskQueues<Q::Kind>> {
        FullBuilder::from_config_and_queen(self.config, self.queen)
    }
}

impl<Q: Queen + Default> BeeBuilder<Q> {
    /// Creates a new `BeeBuilder` with a queen created with
    /// [`Q::default()`](std::default::Default) and no options configured.
    pub fn empty_with_queen_default() -> Self {
        Self {
            config: Config::empty(),
            queen: Q::default(),
        }
    }

    /// Creates a new `BeeBuilder` with a queen created with
    /// [`Q::default()`](std::default::Default) and options configured with global defaults.
    pub fn preset_with_queen_default() -> Self {
        Self {
            config: Config::default(),
            queen: Q::default(),
        }
    }
}

impl<Q: QueenMut + Default> BeeBuilder<QueenCell<Q>> {
    /// Creates a new `BeeBuilder` with a queen created with
    /// [`Q::default()`](std::default::Default) and no options configured.
    pub fn empty_with_queen_mut_default() -> Self {
        Self {
            config: Config::empty(),
            queen: QueenCell::new(Q::default()),
        }
    }

    /// Creates a new `BeeBuilder` with a queen created with
    /// [`Q::default()`](std::default::Default) and options configured with global defaults.
    pub fn preset_with_queen_mut_default() -> Self {
        Self {
            config: Config::default(),
            queen: QueenCell::new(Q::default()),
        }
    }
}

impl<W: Worker + Send + Sync + Clone> BeeBuilder<CloneQueen<W>> {
    /// Creates a new `BeeBuilder` with a `CloneQueen` created with the given `worker` and no
    /// options configured.
    pub fn empty_with_worker(worker: W) -> Self {
        Self {
            config: Config::empty(),
            queen: CloneQueen::new(worker),
        }
    }

    /// Creates a new `BeeBuilder` with a `CloneQueen` created with the given `worker` and
    /// and options configured with global defaults.
    pub fn preset_with_worker(worker: W) -> Self {
        Self {
            config: Config::default(),
            queen: CloneQueen::new(worker),
        }
    }
}

impl<W: Worker + Send + Sync + Default> BeeBuilder<DefaultQueen<W>> {
    /// Creates a new `BeeBuilder` with a `DefaultQueen` created with the given `Worker` type and
    /// no options configured.
    pub fn empty_with_worker_default() -> Self {
        Self {
            config: Config::empty(),
            queen: DefaultQueen::default(),
        }
    }

    /// Creates a new `BeeBuilder` with a `DefaultQueen` created with the given `Worker` type and
    /// and options configured with global defaults.
    pub fn preset_with_worker_default() -> Self {
        Self {
            config: Config::default(),
            queen: DefaultQueen::default(),
        }
    }
}

impl<Q: Queen> BuilderConfig for BeeBuilder<Q> {
    fn config_ref(&mut self, _: Token) -> &mut Config {
        &mut self.config
    }
}

impl<Q: Queen + Default> From<Config> for BeeBuilder<Q> {
    fn from(value: Config) -> Self {
        Self::from_config_and_queen(value, Q::default())
    }
}

impl<Q: Queen> From<Q> for BeeBuilder<Q> {
    fn from(value: Q) -> Self {
        Self::from_config_and_queen(Config::default(), value)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::bee::stock::EchoWorker;
    use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut};
    use rstest::rstest;

    #[derive(Clone, Default)]
    struct TestQueen;

    impl Queen for TestQueen {
        type Kind = EchoWorker<usize>;

        fn create(&self) -> Self::Kind {
            EchoWorker::default()
        }
    }

    impl QueenMut for TestQueen {
        type Kind = EchoWorker<usize>;

        fn create(&mut self) -> Self::Kind {
            EchoWorker::default()
        }
    }

    #[rstest]
    fn test_queen<F, T, W>(
        #[values(
            BeeBuilder::<TestQueen>::empty::<TestQueen>,
            BeeBuilder::<TestQueen>::preset::<TestQueen>
        )]
        factory: F,
        #[values(
            BeeBuilder::<TestQueen>::with_channel_queues,
            BeeBuilder::<TestQueen>::with_workstealing_queues,
        )]
        with_fn: W,
    ) where
        F: Fn(TestQueen) -> BeeBuilder<TestQueen>,
        T: TaskQueues<EchoWorker<usize>>,
        W: Fn(BeeBuilder<TestQueen>) -> FullBuilder<TestQueen, T>,
    {
        let bee_builder = factory(TestQueen);
        let full_builder = with_fn(bee_builder);
        let _hive = full_builder.build();
    }

    #[rstest]
    fn test_queen_default<F, T, W>(
        #[values(
            BeeBuilder::<TestQueen>::empty_with_queen_default,
            BeeBuilder::<TestQueen>::preset_with_queen_default
        )]
        factory: F,
        #[values(
            BeeBuilder::<TestQueen>::with_channel_queues,
            BeeBuilder::<TestQueen>::with_workstealing_queues,
        )]
        with_fn: W,
    ) where
        F: Fn() -> BeeBuilder<TestQueen>,
        T: TaskQueues<EchoWorker<usize>>,
        W: Fn(BeeBuilder<TestQueen>) -> FullBuilder<TestQueen, T>,
    {
        let bee_builder = factory();
        let full_builder = with_fn(bee_builder);
        let _hive = full_builder.build();
    }

    #[rstest]
    fn test_queen_mut_default<F, T, W>(
        #[values(
            BeeBuilder::<QueenCell<TestQueen>>::empty_with_queen_mut_default,
            BeeBuilder::<QueenCell<TestQueen>>::preset_with_queen_mut_default
        )]
        factory: F,
        #[values(
            BeeBuilder::<QueenCell<TestQueen>>::with_channel_queues,
            BeeBuilder::<QueenCell<TestQueen>>::with_workstealing_queues,
        )]
        with_fn: W,
    ) where
        F: Fn() -> BeeBuilder<QueenCell<TestQueen>>,
        T: TaskQueues<EchoWorker<usize>>,
        W: Fn(BeeBuilder<QueenCell<TestQueen>>) -> FullBuilder<QueenCell<TestQueen>, T>,
    {
        let bee_builder = factory();
        let full_builder = with_fn(bee_builder);
        let _hive = full_builder.build();
    }

    #[rstest]
    fn test_worker<F, T, W>(
        #[values(
            BeeBuilder::<CloneQueen<EchoWorker<usize>>>::empty_with_worker,
            BeeBuilder::<CloneQueen<EchoWorker<usize>>>::preset_with_worker
        )]
        factory: F,
        #[values(
            BeeBuilder::<CloneQueen<EchoWorker<usize>>>::with_channel_queues,
            BeeBuilder::<CloneQueen<EchoWorker<usize>>>::with_workstealing_queues,
        )]
        with_fn: W,
    ) where
        F: Fn(EchoWorker<usize>) -> BeeBuilder<CloneQueen<EchoWorker<usize>>>,
        T: TaskQueues<EchoWorker<usize>>,
        W: Fn(
            BeeBuilder<CloneQueen<EchoWorker<usize>>>,
        ) -> FullBuilder<CloneQueen<EchoWorker<usize>>, T>,
    {
        let bee_builder = factory(EchoWorker::default());
        let full_builder = with_fn(bee_builder);
        let _hive = full_builder.build();
    }

    #[rstest]
    fn test_worker_default<F, T, W>(
        #[values(
            BeeBuilder::<DefaultQueen<EchoWorker<usize>>>::empty_with_worker_default,
            BeeBuilder::<DefaultQueen<EchoWorker<usize>>>::preset_with_worker_default
        )]
        factory: F,
        #[values(
            BeeBuilder::<DefaultQueen<EchoWorker<usize>>>::with_channel_queues,
            BeeBuilder::<DefaultQueen<EchoWorker<usize>>>::with_workstealing_queues,
        )]
        with_fn: W,
    ) where
        F: Fn() -> BeeBuilder<DefaultQueen<EchoWorker<usize>>>,
        T: TaskQueues<EchoWorker<usize>>,
        W: Fn(
            BeeBuilder<DefaultQueen<EchoWorker<usize>>>,
        ) -> FullBuilder<DefaultQueen<EchoWorker<usize>>, T>,
    {
        let bee_builder = factory();
        let full_builder = with_fn(bee_builder);
        let _hive = full_builder.build();
    }
}
