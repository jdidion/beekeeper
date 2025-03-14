//! Utilities for pinning worker threads to CPU cores in a `Hive`.
use core_affinity::{self, CoreId};
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashSet;
use std::ops::{BitOr, BitOrAssign, Sub, SubAssign};
use std::sync::LazyLock;

/// The sequence of CPU core IDs.
///
/// This sequence is established the first time an attempt is made to pin threads to CPU cores. The
/// sequence of core IDs is assumed to remain stable throughout the life of the program, e.g., the
/// core ID at index 0 of the sequence must always correspond to the same physical core. If this
/// assumption is violated, it may result in sub-optimal performance (e.g., inability to pin a
/// thread to a core or multiple threads pinned to the same core) but will not cause any panic or
/// undefined behavior.
///
/// If new cores become available during the life of the program, they are immediately available
/// for worker thread scheduling, but they are *not* available for pinning until the
/// `refresh()` function is called.
pub static CORES: LazyLock<CoreIds> = LazyLock::new(|| CoreIds::from_system());

/// Global list of CPU core IDs.
///
/// This is meant to be created at most once, when `CORES` is initialized.
pub struct CoreIds(Mutex<Vec<Core>>);

impl CoreIds {
    fn from_system() -> Self {
        Self::new(
            core_affinity::get_core_ids()
                .map(|core_ids| core_ids.into_iter().map(Core::from).collect())
                .unwrap_or_default(),
        )
    }

    fn new(core_ids: Vec<Core>) -> Self {
        Self(Mutex::new(core_ids))
    }

    fn get(&self, index: usize) -> Option<Core> {
        self.0.lock().get(index).cloned()
    }

    fn update_from(&self, mut new_ids: HashSet<CoreId>) -> usize {
        let mut cur_ids = self.0.lock();
        cur_ids.iter_mut().for_each(|core| {
            if new_ids.contains(&core.id) {
                core.available = true;
                new_ids.remove(&core.id);
            } else {
                core.available = false;
            }
        });
        let num_new_ids = new_ids.len();
        cur_ids.extend(new_ids.into_iter().map(Core::from));
        num_new_ids
    }

    /// Updates `CORES` with the currently available CPU core IDs. The correspondence between the
    /// index in the sequence and the core ID is maintained for any core IDs already in the
    /// sequence. If a previously available core has become unavailable, its `available` flag is
    /// set to `false`. Any new cores are appended to the end of the sequence. Returns the number
    /// of new cores added to the sequence.
    pub fn refresh(&self) -> usize {
        let new_ids: HashSet<_> = core_affinity::get_core_ids()
            .map(|core_ids| core_ids.into_iter().collect())
            .unwrap_or_default();
        self.update_from(new_ids)
    }
}

/// A sequence of CPU core indices. An index in the range `0..num_cpus::get()` may be associated
/// with a CPU core, while an index outside this range will never be associated with a CPU core.
///
/// The mapping between CPU indices and core IDs is platform-specific, but the same index is
/// guaranteed to always refer to the same physical core.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Cores(Vec<usize>);

impl Cores {
    /// Returns a `Cores` set populated with the first `n` CPU indices (up to the number of
    /// available cores).
    pub fn first(n: usize) -> Self {
        Self(Vec::from_iter(0..n.min(num_cpus::get())))
    }

    /// Returns a `Cores` with all CPU indices.
    pub fn all() -> Self {
        Self::from(0..num_cpus::get())
    }

    /// Appends a new CPU core index to the end of the sequence. Returns `true` if the specified
    /// index did not previously exist in the set.
    pub fn append(&mut self, index: usize) -> bool {
        if !self.0.contains(&index) {
            self.0.push(index);
            true
        } else {
            false
        }
    }

    /// Appends to the end of this sequence all the CPU core indices in `other` that are not
    /// already present. Returns the number of new core indices added to the sequence.
    pub fn union(&mut self, other: &Self) -> usize {
        (*other.0)
            .iter()
            .filter_map(|index| self.append(*index).then_some(()))
            .count()
    }

    /// Returns the `Core` associated with the specified index if the index exists and the core
    /// is available, otherwise returns `None`.
    pub fn get(&self, index: usize) -> Option<Core> {
        self.0
            .get(index)
            .and_then(|&index| CORES.get(index))
            .filter(|core| core.available)
    }

    /// Returns an iterator over `(core_index, Option<Core>)`, where `Some(core)` can be used to
    /// set the core affinity of the current thread. The `core` will be `None` for cores that are
    /// not currently available.
    pub fn iter(&self) -> impl Iterator<Item = (usize, Option<Core>)> {
        CoreIter::new(self.0.iter().cloned())
    }
}

impl BitOr for Cores {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        self |= rhs;
        self
    }
}

impl BitOrAssign for Cores {
    fn bitor_assign(&mut self, rhs: Self) {
        let mut rhs_indexes = rhs.0;
        rhs_indexes.retain(|index| !self.0.contains(index));
        self.0.extend(rhs_indexes);
    }
}

impl Sub for Cores {
    type Output = Self;

    fn sub(mut self, rhs: Self) -> Self::Output {
        self -= rhs;
        self
    }
}

impl SubAssign for Cores {
    fn sub_assign(&mut self, rhs: Self) {
        self.0.retain(|i| !rhs.0.contains(i));
    }
}

impl FromIterator<usize> for Cores {
    fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> Self {
        Self(Vec::from_iter(iter))
    }
}

impl<I: IntoIterator<Item = usize>> From<I> for Cores {
    fn from(value: I) -> Self {
        Self(Vec::from_iter(value))
    }
}

/// Iterator over core (index, id) tuples. This itertor holds the `MutexGuard` for the shared
/// global `CoreIds`, so only one thread can iterate at a time.
pub struct CoreIter<'a, I: Iterator<Item = usize>> {
    index_iter: I,
    cores: MutexGuard<'a, Vec<Core>>,
}

impl<'a, I: Iterator<Item = usize>> CoreIter<'a, I> {
    fn new(index_iter: I) -> Self {
        Self {
            index_iter,
            cores: CORES.0.lock(),
        }
    }
}

impl<'a, I: Iterator<Item = usize>> Iterator for CoreIter<'a, I> {
    type Item = (usize, Option<Core>);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index_iter.next()?;
        let core = self.cores.get(index).cloned().filter(|core| core.available);
        Some((index, core))
    }
}

/// Represents a CPU core.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Core {
    /// the OS-specific core ID
    id: CoreId,
    /// whether this core is currently available for pinning threads
    available: bool,
}

impl Core {
    fn new(id: CoreId, available: bool) -> Self {
        Self { id, available }
    }

    /// Attempts to pin the current thread to this CPU core. Returns `true` if the thread was
    /// successfully pinned.
    ///
    /// If the `available` flag is `false`, this immediately returns `false` and does not attempt
    /// to pin the thread.
    pub fn try_pin_current(&self) -> bool {
        self.available && core_affinity::set_for_current(self.id)
    }
}

impl From<CoreId> for Core {
    /// Creates a new `Core` with `available` set to `true`.
    fn from(id: CoreId) -> Self {
        Self::new(id, true)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_core_ids() {
        let core_ids = CoreIds::new((0..10usize).map(|id| Core::from(CoreId { id })).collect());
        assert_eq!(
            (0..10)
                .flat_map(|i| core_ids.get(i).map(|id| id.id))
                .collect::<Vec<_>>(),
            (0..10).map(|id| CoreId { id }).collect::<Vec<_>>()
        );
        assert!(
            (0..10)
                .map(|i| core_ids.get(i).map(|id| id.available).unwrap_or_default())
                .all(std::convert::identity)
        );
        let new_ids: HashSet<CoreId> = vec![10, 11, 1, 3, 5, 7, 9]
            .into_iter()
            .map(|id| CoreId { id })
            .collect();
        let num_added = core_ids.update_from(new_ids);
        assert_eq!(num_added, 2);
        let mut new_core_ids = (0..12)
            .flat_map(|i| core_ids.get(i).map(|id| id.id))
            .collect::<Vec<_>>();
        new_core_ids.sort();
        assert_eq!(
            new_core_ids,
            (0..12).map(|id| CoreId { id }).collect::<Vec<_>>()
        );
        assert_eq!(
            (0..12)
                .flat_map(|i| core_ids.get(i))
                .filter(|id| id.available)
                .count(),
            7
        );
    }

    #[test]
    fn test_empty() {
        assert_eq!(Cores::default().0.len(), 0);
    }

    #[test]
    fn test_first() {
        let max = num_cpus::get();
        for n in 1..=max {
            assert_eq!(Cores::first(n).0.len(), n);
        }
        assert_eq!(Cores::first(max + 1).0.len(), max);
    }

    #[test]
    fn test_all() {
        let max = num_cpus::get();
        assert_eq!(Cores::all().0.len(), max);
    }

    #[test]
    fn test_append() {
        let mut a = Cores::from(0..4);
        a.append(4);
        assert_eq!(a, Cores::from(0..5));
    }

    #[test]
    fn test_union() {
        let mut a = Cores::from(0..4);
        let b = Cores::from(3..6);
        a.union(&b);
        assert_eq!(a, Cores::from(0..6));
    }

    #[test]
    fn test_ops() {
        let a = Cores::from(0..4);
        let b = Cores::from(3..6);
        assert_eq!(a.clone() | b.clone(), Cores::from(0..6));
        assert_eq!(a.clone() - b.clone(), Cores::from(0..3));
        assert_eq!(b.clone() - a.clone(), Cores::from(4..6));
    }

    #[test]
    fn test_assign_ops() {
        let mut a = Cores::from(0..4);
        let b = Cores::from(3..6);
        a |= b;
        assert_eq!(a, Cores::from(0..6));
        let c = Cores::from(0..3);
        a -= c;
        assert_eq!(a, Cores::from(3..6))
    }

    #[test]
    fn test_iter() {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let n = core_ids.len();
        let pairs: Vec<_> = Cores::from(0..n).iter().collect();
        assert_eq!(pairs.len(), n);
    }
}
