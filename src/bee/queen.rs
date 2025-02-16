//! The Queen bee trait.
use super::Worker;
use parking_lot::RwLock;
use std::marker::PhantomData;
use std::ops::Deref;

/// A trait for factories that create `Worker`s.
pub trait Queen: Send + Sync + 'static {
    /// The kind of `Worker` created by this factory.
    type Kind: Worker;

    /// Creates and returns a new instance of `Self::Kind`, *immutably*.
    fn create(&self) -> Self::Kind;
}

/// A trait for mutable factories that create `Worker`s.
pub trait QueenMut: Send + Sync + 'static {
    /// The kind of `Worker` created by this factory.
    type Kind: Worker;

    /// Creates and returns a new instance of `Self::Kind`, *immutably*.
    fn create(&mut self) -> Self::Kind;
}

/// A wrapper for a `MutQueen` that implements `Queen`.
///
/// Interior mutability is enabled using an `RwLock`.
pub struct QueenCell<Q: QueenMut>(RwLock<Q>);

impl<Q: QueenMut> QueenCell<Q> {
    pub fn new(mut_queen: Q) -> Self {
        Self(RwLock::new(mut_queen))
    }

    pub fn get(&self) -> impl Deref<Target = Q> + '_ {
        self.0.read()
    }

    pub fn into_inner(self) -> Q {
        self.0.into_inner()
    }
}

impl<Q: QueenMut> Queen for QueenCell<Q> {
    type Kind = Q::Kind;

    fn create(&self) -> Self::Kind {
        self.0.write().create()
    }
}

impl<Q: QueenMut + Default> Default for QueenCell<Q> {
    fn default() -> Self {
        Self::new(Q::default())
    }
}

impl<Q: QueenMut> From<Q> for QueenCell<Q> {
    fn from(queen: Q) -> Self {
        Self::new(queen)
    }
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

    fn create(&self) -> Self::Kind {
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

    fn create(&self) -> Self::Kind {
        self.0.clone()
    }
}
