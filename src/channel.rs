//! Support for various channel implementations. A maximum one of the channel feature may be
//! enabled. If no channel feature is enabled, then `std::sync::mpsc` will be used.

pub use prelude::channel;
pub(crate) use prelude::*;

/// Possible results of calling `ReceiverExt::try_recv_msg()` on a `Receiver`.
pub enum Message<T> {
    /// A message was successfully received from the channel.
    Received(T),
    /// The channel was disconnected.
    ChannelDisconnected,
    /// The channel had no messages to receive.
    ChannelEmpty,
}

/// Trait implemented for all channel `Receiver` types that standardizes non-blocking `recv()`.
pub trait ReceiverExt<T> {
    /// Attempts to receive a message from the channel. Returns `Message::Received` if a message
    /// was successfully received, otherwise one of `Message`'s error variants.
    fn try_recv_msg(&self) -> Message<T>;
}

/// Trait implemented for channel `Receiver` types that do not already provide an `iter()` method.
pub trait ReceiverIter<T> {
    /// Returns an iterator that yields items from the channel. The iterator will block waiting for
    /// messages. The iterator wilL terminate when the channel is disconnected.
    fn iter(self) -> impl Iterator<Item = T>;
}

#[cfg(not(any(
    feature = "crossbeam",
    feature = "flume",
    feature = "kanal",
    feature = "loole"
)))]
pub mod prelude {
    pub use std::sync::mpsc::{channel, Receiver, Sender};

    use super::{Message, ReceiverExt};
    use std::sync::mpsc::TryRecvError;

    impl<T> ReceiverExt<T> for Receiver<T> {
        fn try_recv_msg(&self) -> super::Message<T> {
            match self.try_recv() {
                Ok(t) => Message::Received(t),
                Err(TryRecvError::Empty) => Message::ChannelEmpty,
                Err(TryRecvError::Disconnected) => Message::ChannelDisconnected,
            }
        }
    }
}

#[cfg(all(
    feature = "crossbeam",
    not(any(feature = "flume", feature = "kanal", feature = "loole"))
))]
pub mod prelude {
    pub use crossbeam_channel::{unbounded as channel, Receiver, Sender};

    use super::{Message, ReceiverExt};
    use crossbeam_channel::TryRecvError;

    impl<T> ReceiverExt<T> for Receiver<T> {
        fn try_recv_msg(&self) -> super::Message<T> {
            match self.try_recv() {
                Ok(t) => Message::Received(t),
                Err(TryRecvError::Empty) => Message::ChannelEmpty,
                Err(TryRecvError::Disconnected) => Message::ChannelDisconnected,
            }
        }
    }
}

#[cfg(all(
    feature = "flume",
    not(any(feature = "crossbeam", feature = "kanal", feature = "loole"))
))]
pub mod prelude {
    pub use flume::{unbounded as channel, Receiver, Sender};

    use super::{Message, ReceiverExt};
    use flume::TryRecvError;

    impl<T> ReceiverExt<T> for Receiver<T> {
        fn try_recv_msg(&self) -> super::Message<T> {
            match self.try_recv() {
                Ok(t) => Message::Received(t),
                Err(TryRecvError::Empty) => Message::ChannelEmpty,
                Err(TryRecvError::Disconnected) => Message::ChannelDisconnected,
            }
        }
    }
}

#[cfg(all(
    feature = "kanal",
    not(any(feature = "crossbeam", feature = "flume", feature = "loole"))
))]
pub mod prelude {
    pub use kanal::{unbounded as channel, Receiver, Sender};

    use super::{Message, ReceiverExt, ReceiverIter};

    impl<T> ReceiverExt<T> for Receiver<T> {
        fn try_recv_msg(&self) -> Message<T> {
            match self.try_recv() {
                Ok(Some(t)) => Message::Received(t),
                Ok(None) => Message::ChannelEmpty,
                Err(_) => Message::ChannelDisconnected,
            }
        }
    }

    impl<T> ReceiverIter<T> for Receiver<T> {
        fn iter(self) -> impl Iterator<Item = T> {
            self
        }
    }
}

#[cfg(all(
    feature = "loole",
    not(any(feature = "crossbeam", feature = "flume", feature = "kanal"))
))]
pub mod prelude {
    pub use loole::{unbounded as channel, Receiver, Sender};

    use super::{Message, ReceiverExt};
    use loole::TryRecvError;

    impl<T> ReceiverExt<T> for Receiver<T> {
        fn try_recv_msg(&self) -> super::Message<T> {
            match self.try_recv() {
                Ok(t) => Message::Received(t),
                Err(TryRecvError::Empty) => Message::ChannelEmpty,
                Err(TryRecvError::Disconnected) => Message::ChannelDisconnected,
            }
        }
    }
}
