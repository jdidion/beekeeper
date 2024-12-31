//! Utilities for pinning worker threads to CPU cores in a `Hive`.
use parking_lot::Mutex;
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
static CORES: LazyLock<Mutex<Vec<Core>>> = LazyLock::new(|| {
    core_affinity::get_core_ids()
        .map(|core_ids| core_ids.into_iter().map(Core::new).collect())
        .or_else(|| Some(Vec::new()))
        .map(Mutex::new)
        .unwrap()
});

/// Updates `CORES` with the currently available CPU core IDs. The correspondence between the
/// index in the sequence and the core ID is maintained for any core IDs already in the sequence.
/// If a previously available core has become unavailable, its `available` flag is set to `false`.
/// Any new cores are appended to the end of the sequence. Returns the number of new cores added to
/// the sequence.
pub fn refresh() -> usize {
    let mut cur_ids = CORES.lock();
    let mut new_ids: HashSet<_> = core_affinity::get_core_ids()
        .map(|core_ids| core_ids.into_iter().collect())
        .unwrap_or_default();
    cur_ids.iter_mut().for_each(|core| {
        if new_ids.contains(&core.id) {
            core.available = true;
            new_ids.remove(&core.id);
        } else {
            core.available = false;
        }
    });
    let num_new_ids = new_ids.len();
    cur_ids.extend(new_ids.into_iter().map(Core::new));
    num_new_ids
}

/// Represents a CPU core.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Core {
    id: core_affinity::CoreId,
    available: bool,
}

impl Core {
    fn new(core_id: core_affinity::CoreId) -> Self {
        Self {
            id: core_id,
            available: true,
        }
    }

    /// Attempts to pin the current thread to this CPU core. Returns `true` if the thread was
    /// successfully pinned.
    pub fn try_pin_current(&self) -> bool {
        self.available && core_affinity::set_for_current(self.id)
    }
}

/// A sequence of CPU core indices. Indices are numbered from `0..num_cpus::get()`. The mapping
/// between CPU indices and core IDs is platform-specific, but the same index is guaranteed to
/// always refer to the same physical core.
#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct Cores(Vec<usize>);

impl Cores {
    /// Returns an empty `Cores`.
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Returns a `Cores` set populated with the first `n` CPU indices.
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
    /// already present.
    pub fn union(&mut self, other: &Self) -> usize {
        (*other.0)
            .iter()
            .filter_map(|index| self.append(*index).then_some(()))
            .count()
    }

    /// Returns the `Core` associated with the specified index if the index exists and the core
    /// is available, otherwise returns `None`.
    pub fn get(&self, index: usize) -> Option<Core> {
        let cores = CORES.lock();
        self.0
            .get(index)
            .and_then(|&index| cores.get(index).cloned())
            .filter(|core| core.available)
    }

    /// Returns an iterator over `(core_index, Option<Core>)`, where `Some(core)` can be used to
    /// set the core affinity of the current thread. The `core` will be `None` for cores that are
    /// not currently available.
    pub fn iter(&self) -> impl Iterator<Item = (usize, Option<Core>)> + '_ {
        let cores = CORES.lock();
        self.0.iter().cloned().map(move |index| {
            (
                index,
                cores.get(index).filter(|core| core.available).cloned(),
            )
        })
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
        Self(Vec::from_iter(value.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        assert_eq!(Cores::empty().0.len(), 0);
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
    fn test_ops() {
        let a = Cores::from(0..4);
        let b = Cores::from(3..6);
        assert_eq!(a.clone() | b.clone(), Cores::from(0..6));
        assert_eq!(a.clone() - b.clone(), Cores::from(0..3));
        assert_eq!(b.clone() - a.clone(), Cores::from(3..6));
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
        let pairs: Vec<_> = Cores::from(0..(n + 1)).iter().collect();
        assert_eq!(pairs.len(), n);
    }
}
