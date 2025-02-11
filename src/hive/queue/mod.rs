#[cfg(feature = "retry")]
mod delay;
mod global;
#[cfg(any(feature = "batching", feature = "retry"))]
mod local;
#[cfg(not(any(feature = "batching", feature = "retry")))]
mod null;

pub use global::ChannelGlobalQueue;
#[cfg(any(feature = "batching", feature = "retry"))]
pub use local::LocalQueuesImpl as DefaultLocalQueues;
#[cfg(not(any(feature = "batching", feature = "retry")))]
pub use null::LocalQueuesImpl as DefaultLocalQueues;

use super::{GlobalQueue, LocalQueues, QueuePair};
use crate::bee::Worker;
use std::marker::PhantomData;

pub(crate) type ChannelQueues<W> =
    DefaultQueuePair<W, ChannelGlobalQueue<W>, DefaultLocalQueues<W, ChannelGlobalQueue<W>>>;

pub(crate) struct DefaultQueuePair<
    W: Worker,
    G: GlobalQueue<W> + Default,
    L: LocalQueues<W, G> + Default,
> {
    _worker: PhantomData<W>,
    _global: PhantomData<G>,
    _local: PhantomData<L>,
}

impl<W, G, L> QueuePair<W> for DefaultQueuePair<W, G, L>
where
    W: Worker,
    G: GlobalQueue<W> + Default,
    L: LocalQueues<W, G> + Default,
{
    type Global = G;
    type Local = L;

    fn new() -> (Self::Global, Self::Local) {
        (Self::Global::default(), Self::Local::default())
    }
}
