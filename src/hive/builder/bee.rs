use super::{BuilderConfig, FullBuilder, Token};
use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut, Worker};
use crate::hive::{ChannelTaskQueues, Config, TaskQueues, WorkstealingTaskQueues};

/// A Builder for creating `Hive` instances for specific [`Worker`] and [`TaskQueues`] types.
#[derive(Clone, Default)]
pub struct BeeBuilder<Q: Queen> {
    config: Config,
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
    pub(super) fn from(config: Config, queen: Q) -> Self {
        Self { config, queen }
    }

    /// Creates a new `FullBuilder` with the current configuration and queen and specified
    /// `TaskQueues` type.
    pub fn with_queues<T: TaskQueues<Q::Kind>>(self) -> FullBuilder<Q, T> {
        FullBuilder::from(self.config, self.queen)
    }

    /// Creates a new `FullBuilder` with the current configuration and queen and channel-based
    /// task queues.
    pub fn with_channel_queues(self) -> FullBuilder<Q, ChannelTaskQueues<Q::Kind>> {
        FullBuilder::from(self.config, self.queen)
    }

    pub fn with_workstealing_queues(self) -> FullBuilder<Q, WorkstealingTaskQueues<Q::Kind>> {
        FullBuilder::from(self.config, self.queen)
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
    pub fn empty_with_worker(worker: W) -> Self {
        Self {
            config: Config::empty(),
            queen: CloneQueen::new(worker),
        }
    }

    pub fn default_with_worker(worker: W) -> Self {
        Self {
            config: Config::default(),
            queen: CloneQueen::new(worker),
        }
    }
}

impl<W: Worker + Send + Sync + Default> BeeBuilder<DefaultQueen<W>> {
    pub fn empty_with_worker_default() -> Self {
        Self {
            config: Config::empty(),
            queen: DefaultQueen::default(),
        }
    }

    pub fn preset_with_worker_default() -> Self {
        Self {
            config: Config::default(),
            queen: DefaultQueen::default(),
        }
    }
}

impl<Q: Queen> BuilderConfig for BeeBuilder<Q> {
    fn config(&mut self, _: Token) -> &mut Config {
        &mut self.config
    }
}
