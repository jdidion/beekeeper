//! Support for various channel implementations.
//!
//! A maximum one of the channel feature may be enabled. If no channel feature is enabled, then
//! `std::sync::mpsc` will be used.
use derive_more::Debug;
pub use prelude::channel;
pub(crate) use prelude::*;
use std::any;

/// Possible results of calling `ReceiverExt::try_recv_msg()` on a `Receiver`.
#[derive(Debug)]
pub enum Message<T> {
    /// A message was successfully received from the channel.
    #[debug("Received: {}", any::type_name::<T>())]
    Received(T),
    /// The channel was disconnected.
    ChannelDisconnected,
    /// The channel had no messages to receive.
    ChannelEmpty,
}

pub trait SenderExt<T> {
    /// Attempts to send a message to the channel. Returns `None` if the send was successful, or
    /// `Some(t)` if the send was not successful due to the channel being disconnected.
    fn try_send_msg(&self, msg: T) -> Option<T>;
}

/// Trait implemented for all channel `Receiver` types that standardizes non-blocking `recv()`.
pub trait ReceiverExt<T> {
    /// Attempts to receive a message from the channel. Returns `Message::Received` if a message
    /// was successfully received, otherwise one of `Message`'s error variants.
    fn try_recv_msg(&self) -> Message<T>;
}

#[cfg(not(any(feature = "crossbeam", feature = "flume", feature = "loole")))]
pub mod prelude {
    pub use std::sync::mpsc::{Receiver, SendError, Sender, channel};

    use super::{Message, ReceiverExt, SenderExt};
    use std::sync::mpsc::TryRecvError;

    impl<T> SenderExt<T> for Sender<T> {
        fn try_send_msg(&self, t: T) -> Option<T> {
            match self.send(t) {
                Ok(_) => None,
                Err(SendError(t)) => Some(t),
            }
        }
    }

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

#[cfg(all(feature = "crossbeam", not(any(feature = "flume", feature = "loole"))))]
pub mod prelude {
    pub use crossbeam_channel::{Receiver, SendError, Sender, unbounded as channel};

    use super::{Message, ReceiverExt, SenderExt};
    use crossbeam_channel::TryRecvError;

    impl<T> SenderExt<T> for Sender<T> {
        fn try_send_msg(&self, t: T) -> Option<T> {
            match self.send(t) {
                Ok(_) => None,
                Err(SendError(t)) => Some(t),
            }
        }
    }

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

#[cfg(all(feature = "flume", not(any(feature = "crossbeam", feature = "loole"))))]
pub mod prelude {
    pub use flume::{Receiver, SendError, Sender, unbounded as channel};

    use super::{Message, ReceiverExt, SenderExt};
    use flume::TryRecvError;

    impl<T> SenderExt<T> for Sender<T> {
        fn try_send_msg(&self, t: T) -> Option<T> {
            match self.send(t) {
                Ok(_) => None,
                Err(SendError(t)) => Some(t),
            }
        }
    }

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

#[cfg(all(feature = "loole", not(any(feature = "crossbeam", feature = "flume"))))]
pub mod prelude {
    pub use loole::{Receiver, SendError, Sender, unbounded as channel};

    use super::{Message, ReceiverExt, SenderExt};
    use loole::TryRecvError;

    impl<T> SenderExt<T> for Sender<T> {
        fn try_send_msg(&self, t: T) -> Option<T> {
            match self.send(t) {
                Ok(_) => None,
                Err(SendError(t)) => Some(t),
            }
        }
    }

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
