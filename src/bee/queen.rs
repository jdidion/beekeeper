//! The Queen bee trait.
use super::Worker;
use derive_more::Debug;
use parking_lot::RwLock;
use std::marker::PhantomData;
use std::ops::Deref;
use std::{any, fmt};

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
    /// Creates a new `QueenCell` with the given `mut_queen`.
    pub fn new(mut_queen: Q) -> Self {
        Self(RwLock::new(mut_queen))
    }

    /// Returns a reference to the wrapped `Queen`.
    pub fn get(&self) -> impl Deref<Target = Q> {
        self.0.read()
    }

    /// Consumes this `QueenCell` and returns the inner `Queen`.
    pub fn into_inner(self) -> Q {
        self.0.into_inner()
    }
}

impl<Q: QueenMut> Queen for QueenCell<Q> {
    type Kind = Q::Kind;

    /// Calls the wrapped `QueenMut::create` method using interior mutability.
    fn create(&self) -> Self::Kind {
        self.0.write().create()
    }
}

impl<Q: QueenMut + fmt::Debug> fmt::Debug for QueenCell<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueenCell")
            .field("queen", &*self.0.read())
            .finish()
    }
}

impl<Q: QueenMut + Clone> Clone for QueenCell<Q> {
    fn clone(&self) -> Self {
        Self(RwLock::new(self.0.read().clone()))
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
///     fn apply(&mut self, input: u8, _: &Context<Self::Input>) -> WorkerResult<Self> {
///         Ok(self.0.saturating_add(input))
///     }
/// }
///
/// struct MyQueen;
///
/// impl Queen for MyQueen {
///     type Kind = MyWorker;
///
///     fn create(&self) -> Self::Kind {
///         MyWorker::default()
///     }
/// }
/// ```
#[derive(Default, Debug)]
#[debug("DefaultQueen<{}>", any::type_name::<W>())]
pub struct DefaultQueen<W>(PhantomData<W>);

impl<W: Worker + Send + Sync + Default> Clone for DefaultQueen<W> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<W: Worker + Send + Sync + Default> Queen for DefaultQueen<W> {
    type Kind = W;

    fn create(&self) -> Self::Kind {
        Self::Kind::default()
    }
}

/// A `Queen` that can create a `Worker` type that implements `Clone`, by making copies of
/// an existing instance of that `Worker` type.
#[derive(Debug)]
#[debug("CloneQueen<{}>", any::type_name::<W>())]
pub struct CloneQueen<W>(W);

impl<W: Worker + Clone> CloneQueen<W> {
    pub fn new(worker: W) -> Self {
        CloneQueen(worker)
    }
}

impl<W: Worker + Send + Sync + Clone> Clone for CloneQueen<W> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<W: Worker + Send + Sync + Default> Default for CloneQueen<W> {
    fn default() -> Self {
        Self(W::default())
    }
}

impl<W: Worker + Send + Sync + Clone> Queen for CloneQueen<W> {
    type Kind = W;

    fn create(&self) -> Self::Kind {
        self.0.clone()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::{CloneQueen, DefaultQueen, Queen, QueenCell, QueenMut};
    use crate::bee::stock::EchoWorker;

    #[derive(Default, Debug, Clone)]
    struct TestQueen(usize);

    impl QueenMut for TestQueen {
        type Kind = EchoWorker<u32>;

        fn create(&mut self) -> Self::Kind {
            self.0 += 1;
            EchoWorker::default()
        }
    }

    #[test]
    fn test_queen_cell() {
        let queen = QueenCell::new(TestQueen(0));
        for _ in 0..10 {
            let _worker = queen.create();
        }
        assert_eq!(queen.get().0, 10);
        assert_eq!(queen.into_inner().0, 10);
    }

    #[test]
    fn test_queen_cell_default() {
        let queen = QueenCell::<TestQueen>::default();
        for _ in 0..10 {
            let _worker = queen.create();
        }
        assert_eq!(queen.get().0, 10);
    }

    #[test]
    fn test_queen_cell_clone() {
        let queen = QueenCell::<TestQueen>::default();
        for _ in 0..10 {
            let _worker = queen.create();
        }
        assert_eq!(queen.clone().get().0, 10);
    }

    #[test]
    fn test_queen_cell_debug() {
        let queen = QueenCell::<TestQueen>::default();
        for _ in 0..10 {
            let _worker = queen.create();
        }
        assert_eq!(format!("{:?}", queen), "QueenCell { queen: TestQueen(10) }");
    }

    #[test]
    fn test_queen_cell_from() {
        let queen = QueenCell::from(TestQueen::default());
        for _ in 0..10 {
            let _worker = queen.create();
        }
        assert_eq!(queen.get().0, 10);
    }

    #[test]
    fn test_default_queen() {
        let queen1 = DefaultQueen::<EchoWorker<u32>>::default();
        let worker1 = queen1.create();
        let queen2 = queen1.clone();
        let worker2 = queen2.create();
        assert_eq!(worker1, worker2);
    }

    #[test]
    fn test_clone_queen() {
        let worker = EchoWorker::<u32>::default();
        let queen = CloneQueen::new(worker);
        let worker1 = queen.create();
        let queen2 = queen.clone();
        let worker2 = queen2.create();
        assert_eq!(worker1, worker2);
    }
}
