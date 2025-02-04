#[cfg(any(feature = "batching", feature = "retry"))]
pub use channel::ChannelLocalQueues as LocalQueuesImpl;
#[cfg(not(any(feature = "batching", feature = "retry")))]
pub use null::NullLocalQueues as LocalQueuesImpl;

#[cfg(not(any(feature = "batching", feature = "retry")))]
mod null {
    use crate::bee::Worker;
    use crate::hive::LocalQueues;
    use std::marker::PhantomData;

    pub struct NullLocalQueues<W: Worker>(PhantomData<W>);

    impl<W: Worker> LocalQueues<W> for NullLocalQueues<W> {}
}

#[cfg(any(feature = "batching", feature = "retry"))]
mod channel {
    use crate::bee::Worker;
    use crate::hive::{LocalQueues, Task};
    use parking_lot::RwLock;

    pub struct ChannelLocalQueues<W: Worker> {
        /// thread-local queues of tasks used when the `batching` feature is enabled
        #[cfg(feature = "batching")]
        batch_queues: RwLock<Vec<crossbeam_queue::ArrayQueue<Task<W>>>>,
        /// thread-local queues used for tasks that are waiting to be retried after a failure
        #[cfg(feature = "retry")]
        retry_queues: RwLock<Vec<crate::hive::task::delay::DelayQueue<Task<W>>>>,
    }

    impl<W: Worker> LocalQueues<W> for ChannelLocalQueues<W> {}

    impl<W: Worker> Default for ChannelLocalQueues<W> {
        fn default() -> Self {
            Self {
                #[cfg(feature = "batching")]
                batch_queues: Default::default(),
                #[cfg(feature = "retry")]
                retry_queues: Default::default(),
            }
        }
    }
}
