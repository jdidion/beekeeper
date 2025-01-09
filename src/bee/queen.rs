use super::Worker;
use std::marker::PhantomData;

/// A trait for stateful factories that create `Worker`s.
pub trait Queen: Send + Sync + 'static {
    /// The kind of `Worker` created by this factory.
    type Kind: Worker;

    /// Returns a new instance of `Self::Kind`.
    fn create(&mut self) -> Self::Kind;
}

/// A `Queen` that can create a `Worker` type that implements `Default`.
///
/// Note that, for the implementation to be generic, `W` also needs to be `Send` and `Sync`. If you
/// want a `Queen` for a specific `Worker` type that is not `Send/Sync`:
///
/// ```
/// # use beekeeper::bee::{Context, Queen, Worker, WorkerResult};
/// # use std::rc::Rc;
///
/// #[derive(Default, Debug)]
/// struct MyWorker(Rc<u8>); // not `Send` or `Sync`
///
/// impl Worker for MyWorker {
///     type Input = u8;
///     type Output = u8;
///     type Error = ();
///
///     fn apply(&mut self, input: u8, _: &Context) -> WorkerResult<Self> {
///         Ok(self.0.saturating_add(input))
///     }
/// }
///
/// struct MyQueen;
///
/// impl Queen for MyQueen {
///     type Kind = MyWorker;
///
///     fn create(&mut self) -> Self::Kind {
///         MyWorker::default()
///     }
/// }
/// ```
#[derive(Default, Debug)]
pub struct DefaultQueen<W>(PhantomData<W>);

impl<W: Worker + Send + Sync + Default> Queen for DefaultQueen<W> {
    type Kind = W;

    fn create(&mut self) -> Self::Kind {
        Self::Kind::default()
    }
}

/// A `Queen` that can create a `Worker` type that implements `Clone`, by making copies of
/// an existing instance of that `Worker` type.
#[derive(Debug)]
pub struct CloneQueen<W>(W);

impl<W: Worker + Clone> CloneQueen<W> {
    pub fn new(worker: W) -> Self {
        CloneQueen(worker)
    }
}

impl<W: Worker + Send + Sync + Clone> Queen for CloneQueen<W> {
    type Kind = W;

    fn create(&mut self) -> Self::Kind {
        self.0.clone()
    }
}
