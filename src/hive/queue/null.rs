use crate::bee::{Queen, Worker};
use crate::hive::{ChannelGlobalQueue, LocalQueues, Shared, Task};
use std::marker::PhantomData;

pub struct LocalQueuesImpl<W: Worker>(PhantomData<fn() -> W>);

impl<W: Worker> LocalQueues<W, ChannelGlobalQueue<W>> for LocalQueuesImpl<W> {
    fn init_for_threads<Q: Queen<Kind = W>>(
        &self,
        _: usize,
        _: usize,
        _: &Shared<W, Q, ChannelGlobalQueue<W>, Self>,
    ) {
    }

    #[inline(always)]
    fn push<Q: Queen<Kind = W>>(
        &self,
        task: Task<W>,
        _: usize,
        shared: &Shared<W, Q, ChannelGlobalQueue<W>, Self>,
    ) {
        shared.push_global(task);
    }

    #[inline(always)]
    fn try_pop<Q: Queen<Kind = W>>(
        &self,
        _: usize,
        _: &Shared<W, Q, ChannelGlobalQueue<W>, Self>,
    ) -> Option<Task<W>> {
        None
    }

    fn drain(&self) -> Vec<Task<W>> {
        Vec::new()
    }
}

impl<W: Worker> Default for LocalQueuesImpl<W> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
