//! Utilities for pinning worker threads to CPU cores in a `Hive`.
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct CoreId(core_affinity::CoreId);

impl CoreId {
    pub fn set_for_current(&self) -> bool {
        core_affinity::set_for_current(self.0)
    }
}

/// A set of CPU core indices. Indices are numbered from `0..num_cpus::get()`. The mapping between
/// CPU indices and core IDs is platform-specific.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Cores(BTreeSet<usize>);

impl Cores {
    /// Returns an empty `Cores` set.
    pub fn empty() -> Self {
        Self(BTreeSet::new())
    }

    /// Returns a `Cores` set populated with the first `n` CPU indices.
    pub fn first(n: usize) -> Self {
        Self(BTreeSet::from_iter(0..n.min(num_cpus::get())))
    }

    /// Returns a `Cores` set with all CPU indices.
    pub fn all() -> Self {
        Self::from(0..num_cpus::get())
    }

    /// Adds a new CPU core index to the set. Returns `true` if the specified index did not
    /// previously exist in the set.
    pub fn insert(&mut self, core: usize) -> bool {
        self.0.insert(core)
    }

    /// Removes a CPU core index from the set if it exists. Returns `true` if the specified index
    /// was present in the set.
    pub fn remove(&mut self, core: &usize) -> bool {
        self.0.remove(core)
    }

    /// Removes all CPU indices in `other` from this set.
    pub fn remove_all(&mut self, other: &Self) {
        self.0.retain(|core| !other.0.contains(core))
    }

    /// Returns an iterator over `(core_index, core_id)`, where `CoreId` can be used to set
    /// the core affinity of the current thread. The iterator only yields items for which the
    /// core is available to be pinned.
    pub fn iter_core_ids(&self) -> impl Iterator<Item = (usize, CoreId)> + '_ {
        let core_ids = core_affinity::get_core_ids().unwrap();
        self.0
            .iter()
            .cloned()
            .filter_map(move |core| core_ids.get(core).map(|core_id| (core, CoreId(*core_id))))
    }
}

impl FromIterator<usize> for Cores {
    fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> Self {
        Self(BTreeSet::from_iter(iter))
    }
}

impl<I: IntoIterator<Item = usize>> From<I> for Cores {
    fn from(value: I) -> Self {
        Self(BTreeSet::from_iter(value.into_iter()))
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
    fn test_remove_all() {
        let mut a = Cores::from(0..4);
        let b = Cores::from(3..6);
        a.remove_all(&b);
        assert_eq!(a, Cores::from(0..3));
    }

    #[test]
    fn test_iter() {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let n = core_ids.len();
        let pairs: Vec<_> = Cores::from(0..n).iter_core_ids().collect();
        assert_eq!(pairs.len(), n);
        let pairs: Vec<_> = Cores::from(0..(n + 1)).iter_core_ids().collect();
        assert_eq!(pairs.len(), n);
    }
}
