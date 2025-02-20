use super::FullBuilder;
use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut, Worker};
use crate::hive::{Builder, TaskQueues};

pub trait TaskQueuesBuilder: Builder + Default + Sized {
    type TaskQueues<W: Worker>: TaskQueues<W>;

    fn empty() -> Self;

    /// Consumes this `Builder` and returns a new [`FullBuilder`] using the given [`Queen`] to
    /// create [`Worker`]s.
    fn with_queen<Q: Queen, I: Into<Q>>(
        self,
        queen: I,
    ) -> FullBuilder<Q, Self::TaskQueues<Q::Kind>>;

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

    #[derive(Clone, Default)]
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
        fn with_queen<Q, I>(self, queen: I) -> FullBuilder<Q, Self::TaskQueues<Q::Kind>>
        where
            Q: Queen,
            I: Into<Q>,
        {
            FullBuilder::from(self.0, queen.into())
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

    #[derive(Clone, Default)]
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
        fn with_queen<Q, I>(self, queen: I) -> FullBuilder<Q, Self::TaskQueues<Q::Kind>>
        where
            Q: Queen,
            I: Into<Q>,
        {
            FullBuilder::from(self.0, queen.into())
        }
    }

    impl From<Config> for WorkstealingBuilder {
        fn from(value: Config) -> Self {
            Self(value)
        }
    }
}
