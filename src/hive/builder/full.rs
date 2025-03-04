use super::{BuilderConfig, Token};
use crate::bee::Queen;
use crate::hive::{Config, Hive, TaskQueues};
use std::marker::PhantomData;

/// A Builder for creating `Hive` instances for specific [`Queen`] and [`TaskQueues`] types.
#[derive(Clone, Default)]
pub struct FullBuilder<Q: Queen, T: TaskQueues<Q::Kind>> {
    config: Config,
    queen: Q,
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
    pub(super) fn from(config: Config, queen: Q) -> Self {
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

impl<Q: Queen, T: TaskQueues<Q::Kind>> BuilderConfig for FullBuilder<Q, T> {
    fn config_ref(&mut self, _: Token) -> &mut Config {
        &mut self.config
    }
}
