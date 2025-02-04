#[cfg(any(feature = "batching", feature = "retry"))]
pub use channel::ChannelLocalQueues as LocalQueuesImpl;
#[cfg(not(any(feature = "batching", feature = "retry")))]
pub use null::NullLocalQueues as LocalQueuesImpl;

#[cfg(not(any(feature = "batching", feature = "retry")))]
mod null {
    use crate::hive::LocalQueues;
    use crate::bee::Worker;
    use std::marker::PhantomData;

    pub struct NullLocalQueues<W: Worker>(PhantomData<W>);

    impl<W: Worker> LocalQueues<W> for NullLocalQueues<W> {}
}

#[cfg(any(feature = "batching", feature = "retry"))]
mod channel {
    use crate::hive::LocalQueues;

    pub struct ChannelLocalQueues<W: Worker> {
        /// worker thread-specific queues of tasks used when the `batching` feature is enabled
        batch_queues: parking_lot::RwLock<Vec<crossbeam_queue::ArrayQueue<Task<W>>>,
        /// queue used for tasks that are waiting to be retried after a failure
        #[cfg(feature = "retry")]
        retry_queues: parking_lot::RwLock<Vec<delay::DelayQueue<Task<W>>>>,
    }
}
