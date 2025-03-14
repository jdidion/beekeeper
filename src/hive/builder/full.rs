use super::{BuilderConfig, Token};
use crate::bee::Queen;
use crate::hive::{Config, Hive, TaskQueues};
use derive_more::Debug;
use std::any;
use std::marker::PhantomData;

/// A Builder for creating `Hive` instances for specific [`Queen`] and [`TaskQueues`] types.
#[derive(Clone, Default, Debug)]
pub struct FullBuilder<Q: Queen, T: TaskQueues<Q::Kind>> {
    config: Config,
    #[debug("{}", any::type_name::<Q>())]
    queen: Q,
    #[debug("{}", any::type_name::<T>())]
    _queues: PhantomData<T>,
}

impl<Q: Queen, T: TaskQueues<Q::Kind>> FullBuilder<Q, T> {
    /// Creates a new `FullBuilder` with the given queen and no options configured.
    pub fn empty<I: Into<Q>>(queen: Q) -> Self {
        Self {
            config: Config::empty(),
            queen,
            _queues: PhantomData,
        }
    }

    /// Creates a new `FullBuilder` with the given `queen` and options configured with global
    /// defaults.
    pub fn preset<I: Into<Q>>(queen: I) -> Self {
        Self {
            config: Config::default(),
            queen: queen.into(),
            _queues: PhantomData,
        }
    }

    /// Creates a new `FullBuilder` from an existing `config` and a `queen`.
    pub(super) fn from_config_and_queen(config: Config, queen: Q) -> Self {
        Self {
            config,
            queen,
            _queues: PhantomData,
        }
    }

    /// Consumes this `Builder` and returns a new [`Hive`].
    pub fn build(self) -> Hive<Q, T> {
        Hive::new(self.config, self.queen)
    }
}

impl<Q: Queen + Default, T: TaskQueues<Q::Kind>> From<Config> for FullBuilder<Q, T> {
    fn from(value: Config) -> Self {
        Self::from_config_and_queen(value, Q::default())
    }
}

impl<Q: Queen, T: TaskQueues<Q::Kind>> From<Q> for FullBuilder<Q, T> {
    fn from(value: Q) -> Self {
        Self::from_config_and_queen(Config::default(), value)
    }
}

impl<Q: Queen, T: TaskQueues<Q::Kind>> BuilderConfig for FullBuilder<Q, T> {
    fn config_ref(&mut self, _: Token) -> &mut Config {
        &mut self.config
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::bee::Queen;
    use crate::bee::stock::EchoWorker;
    use crate::hive::{ChannelTaskQueues, WorkstealingTaskQueues};
    use rstest::rstest;

    #[derive(Clone, Default)]
    struct TestQueen;

    impl Queen for TestQueen {
        type Kind = EchoWorker<usize>;

        fn create(&self) -> Self::Kind {
            EchoWorker::default()
        }
    }

    #[rstest]
    fn test_channel<F>(
        #[values(
            FullBuilder::<TestQueen, ChannelTaskQueues<EchoWorker<usize>>>::empty::<TestQueen>,
            FullBuilder::<TestQueen, ChannelTaskQueues<EchoWorker<usize>>>::preset::<TestQueen>
        )]
        factory: F,
    ) where
        F: Fn(TestQueen) -> FullBuilder<TestQueen, ChannelTaskQueues<EchoWorker<usize>>>,
    {
        let builder = factory(TestQueen);
        let _hive = builder.build();
    }

    #[rstest]
    fn test_workstealing<F>(
        #[values(
            FullBuilder::<TestQueen, WorkstealingTaskQueues<EchoWorker<usize>>>::empty::<TestQueen>,
            FullBuilder::<TestQueen, WorkstealingTaskQueues<EchoWorker<usize>>>::preset::<TestQueen>
        )]
        factory: F,
    ) where
        F: Fn(TestQueen) -> FullBuilder<TestQueen, WorkstealingTaskQueues<EchoWorker<usize>>>,
    {
        let builder = factory(TestQueen);
        let _hive = builder.build();
    }
}
