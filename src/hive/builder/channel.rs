use super::{BuilderConfig, FullBuilder, Token};
use crate::bee::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut, Worker};
use crate::hive::{ChannelTaskQueues, Config};

#[derive(Clone, Default)]
pub struct ChannelBuilder(Config);

impl ChannelBuilder {
    /// Creates a new `ChannelBuilder` with the given queen and no options configured.
    pub fn empty() -> Self {
        Self(Config::empty())
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] using the given [`Queen`] to
    /// create [`Worker`]s.
    pub fn with_queen<Q, I>(self, queen: I) -> FullBuilder<Q, ChannelTaskQueues<Q::Kind>>
    where
        Q: Queen,
        I: Into<Q>,
    {
        FullBuilder::from(self.0, queen.into())
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] using a [`Queen`] created with
    /// [`Q::default()`](std::default::Default) to create [`Worker`]s.
    pub fn with_queen_default<Q>(self) -> FullBuilder<Q, ChannelTaskQueues<Q::Kind>>
    where
        Q: Queen + Default,
    {
        FullBuilder::from(self.0, Q::default())
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] using a [`QueenMut`] created with
    /// [`Q::default()`](std::default::Default) to create [`Worker`]s.
    pub fn with_queen_mut_default<Q>(self) -> FullBuilder<QueenCell<Q>, ChannelTaskQueues<Q::Kind>>
    where
        Q: QueenMut + Default,
    {
        FullBuilder::from(self.0, QueenCell::new(Q::default()))
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] with [`Worker`]s created by
    /// cloning `worker`.
    pub fn with_worker<W>(self, worker: W) -> FullBuilder<CloneQueen<W>, ChannelTaskQueues<W>>
    where
        W: Worker + Send + Sync + Clone,
    {
        FullBuilder::from(self.0, CloneQueen::new(worker))
    }

    /// Consumes this `Builder` and returns a new [`FullBuilder`] with [`Worker`]s created using
    /// [`W::default()`](std::default::Default).
    pub fn with_worker_default<W>(self) -> FullBuilder<DefaultQueen<W>, ChannelTaskQueues<W>>
    where
        W: Worker + Send + Sync + Default,
    {
        FullBuilder::from(self.0, DefaultQueen::default())
    }
}

impl BuilderConfig for ChannelBuilder {
    fn config(&mut self, _: Token) -> &mut Config {
        &mut self.0
    }
}

impl From<Config> for ChannelBuilder {
    fn from(value: Config) -> Self {
        Self(value)
    }
}
