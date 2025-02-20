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
    pub fn empty<I: Into<Q>>(queen: Q) -> Self {
        Self {
            config: Config::empty(),
            queen,
            _queues: PhantomData,
        }
    }

    pub fn preset<I: Into<Q>>(queen: I) -> Self {
        Self {
            config: Config::default(),
            queen: queen.into(),
            _queues: PhantomData,
        }
    }

    pub(super) fn from(config: Config, queen: Q) -> Self {
        Self {
            config,
            queen,
            _queues: PhantomData,
        }
    }

    pub fn build(self) -> Hive<Q, T> {
        Hive::new(self.config, self.queen)
    }
}

impl<Q: Queen, T: TaskQueues<Q::Kind>> BuilderConfig for FullBuilder<Q, T> {
    fn config(&mut self, _: Token) -> &mut Config {
        &mut self.config
    }
}
