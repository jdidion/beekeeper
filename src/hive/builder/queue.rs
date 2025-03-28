use super::{Builder, FullBuilder};
use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut, Worker};
use crate::hive::TaskQueues;

/// Trait implemented by builders specialized to a `TaskQueues` type.
pub trait TaskQueuesBuilder: Builder + Clone + Default + Sized {
    /// The type of the `TaskQueues` to use when building the `Hive`.
    type TaskQueues<W: Worker>: TaskQueues<W>;

    /// Creates a new empty `Builder`.
    fn empty() -> Self;

    /// Consumes this `Builder` and returns a new [`FullBuilder`] using the given [`Queen`] to
    /// create [`Worker`]s.
    fn with_queen<Q: Queen>(self, queen: Q) -> FullBuilder<Q, Self::TaskQueues<Q::Kind>>;

    /// Consumes this `Builder` and returns a new [`FullBuilder`] using a [`Queen`] created with
    /// [`Q::default()`](std::default::Default) to create [`Worker`]s.
    fn with_queen_default<Q>(self) -> FullBuilder<Q, Self::TaskQueues<Q::Kind>>
    where
        Q: Queen + Default,
    {
        self.with_queen(Q::default())
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] using a [`QueenMut`] created with
    /// [`Q::default()`](std::default::Default) to create [`Worker`]s.
    fn with_queen_mut_default<Q>(self) -> FullBuilder<QueenCell<Q>, Self::TaskQueues<Q::Kind>>
    where
        Q: QueenMut + Default,
    {
        self.with_queen(QueenCell::new(Q::default()))
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] with [`Worker`]s created by
    /// cloning `worker`.
    fn with_worker<W>(self, worker: W) -> FullBuilder<CloneQueen<W>, Self::TaskQueues<W>>
    where
        W: Worker + Send + Sync + Clone,
    {
        self.with_queen(CloneQueen::new(worker))
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] with [`Worker`]s created using
    /// [`W::default()`](std::default::Default).
    fn with_worker_default<W>(self) -> FullBuilder<DefaultQueen<W>, Self::TaskQueues<W>>
    where
        W: Worker + Send + Sync + Default,
    {
        self.with_queen(DefaultQueen::default())
    }
}

pub mod channel {
    use super::*;
    use crate::hive::builder::{BuilderConfig, Token};
    use crate::hive::{ChannelTaskQueues, Config};

    /// `TaskQueuesBuilder` implementation for channel-based task queues.
    #[derive(Clone, Default, Debug)]
    pub struct ChannelBuilder(Config);

    impl BuilderConfig for ChannelBuilder {
        fn config_ref(&mut self, _: Token) -> &mut Config {
            &mut self.0
        }
    }

    impl TaskQueuesBuilder for ChannelBuilder {
        type TaskQueues<W: Worker> = ChannelTaskQueues<W>;

        fn empty() -> Self {
            Self(Config::empty())
        }

        /// Consumes this `Builder` and returns a new [`FullBuilder`] using the given [`Queen`] to
        /// create [`Worker`]s.
        fn with_queen<Q: Queen>(self, queen: Q) -> FullBuilder<Q, Self::TaskQueues<Q::Kind>> {
            FullBuilder::from_config_and_queen(self.0, queen)
        }
    }

    impl From<Config> for ChannelBuilder {
        fn from(value: Config) -> Self {
            Self(value)
        }
    }
}

pub mod workstealing {
    use super::*;
    use crate::hive::builder::{BuilderConfig, Token};
    use crate::hive::{Config, WorkstealingTaskQueues};

    /// `TaskQueuesBuilder` implementation for workstealing-based task queues.
    #[derive(Clone, Default, Debug)]
    pub struct WorkstealingBuilder(Config);

    impl BuilderConfig for WorkstealingBuilder {
        fn config_ref(&mut self, _: Token) -> &mut Config {
            &mut self.0
        }
    }

    impl TaskQueuesBuilder for WorkstealingBuilder {
        type TaskQueues<W: Worker> = WorkstealingTaskQueues<W>;

        fn empty() -> Self {
            Self(Config::empty())
        }

        /// Consumes this `Builder` and returns a new [`FullBuilder`] using the given [`Queen`] to
        /// create [`Worker`]s.
        fn with_queen<Q: Queen>(self, queen: Q) -> FullBuilder<Q, Self::TaskQueues<Q::Kind>> {
            FullBuilder::from_config_and_queen(self.0, queen)
        }
    }

    impl From<Config> for WorkstealingBuilder {
        fn from(value: Config) -> Self {
            Self(value)
        }
    }
}
