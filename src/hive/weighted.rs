use std::ops::Deref;

/// Wraps a value of type `T` and an associated weight.
pub struct Weighted<T> {
    value: T,
    weight: u32,
}

impl<T> Weighted<T> {
    pub fn new(value: T, weight: u32) -> Self {
        Self { value, weight }
    }

    pub fn from_fn<F>(value: T, f: F) -> Self
    where
        F: FnOnce(&T) -> u32,
    {
        let weight = f(&value);
        Self::new(value, weight)
    }

    pub fn weight(&self) -> u32 {
        self.weight
    }

    pub fn into_inner(self) -> T {
        self.value
    }

    pub fn into_parts(self) -> (T, u32) {
        (self.value, self.weight)
    }
}

impl<T: Into<u32> + Clone> Weighted<T> {
    pub fn from_identity(value: T) -> Self {
        let weight = value.clone().into();
        Self::new(value, weight)
    }
}

impl<T> Deref for Weighted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> From<T> for Weighted<T> {
    fn from(value: T) -> Self {
        Self::new(value, 0)
    }
}

impl<T> From<(T, u32)> for Weighted<T> {
    fn from((value, weight): (T, u32)) -> Self {
        Self::new(value, weight)
    }
}

// /// Trait implemented by a type that can be converted into an iterator over `Weighted` items.
// pub trait IntoWeightedIterator {
//     type Item;
//     type IntoIter: Iterator<Item = Weighted<Self::Item>>;

//     fn into_weighted_iter(self) -> Self::IntoIter;
// }

// // Calls to Hive task submission functions should work with iterators of `W::Input` regardless of
// // whether the `local-batch` feature is enabled. This blanket implementation of
// // `IntoWeightedIterator` just gives every task a weight of 0.
// impl<T, I: IntoIterator<Item = T>> IntoWeightedIterator for I {
//     type Item = T;
//     type IntoIter = std::iter::Map<I::IntoIter, fn(T) -> Weighted<T>>;

//     fn into_weighted_iter(self) -> Self::IntoIter {
//         self.into_iter().map(Into::into)
//     }
// }

// struct WeightedIter<T>(Box<dyn Iterator<Item = Weighted<T>>>);

// impl<T: 'static> WeightedIter<T> {
//     fn from_tuples<I>(tuples: I) -> Self
//     where
//         I: IntoIterator<Item = (T, u32)>,
//         I::IntoIter: 'static,
//     {
//         Self(Box::new(tuples.into_iter().map(Into::into)))
//     }

//     fn from_iter<I, W>(items: I, weights: W) -> Self
//     where
//         I: IntoIterator<Item = T>,
//         I::IntoIter: 'static,
//         W: IntoIterator<Item = u32>,
//         W::IntoIter: 'static,
//     {
//         Self(Box::new(
//             items
//                 .into_iter()
//                 .zip(weights.into_iter().chain(std::iter::repeat(0)))
//                 .map(Into::into),
//         ))
//     }

//     fn from_const<I>(items: I, weight: u32) -> Self
//     where
//         I: IntoIterator<Item = T>,
//         I::IntoIter: 'static,
//     {
//         Self::from_iter(items, std::iter::repeat(weight))
//     }

//     fn from_fn<B, F>(items: B, f: F) -> Self
//     where
//         B: IntoIterator<Item = T>,
//         B::IntoIter: 'static,
//         F: Fn(&T) -> u32 + 'static,
//     {
//         Self(Box::new(items.into_iter().map(move |value| {
//             let weight = f(&value);
//             Weighted::new(value, weight)
//         })))
//     }
// }

// impl<T: Into<u32> + Clone + 'static> WeightedIter<T> {
//     fn from_identity<I>(items: I) -> Self
//     where
//         I: IntoIterator<Item = T>,
//         I::IntoIter: 'static,
//     {
//         Self::from_fn(items, |item| item.clone().into())
//     }
// }

// impl<T> IntoWeightedIterator for WeightedIter<T> {
//     type Item = T;
//     type IntoIter = Box<dyn Iterator<Item = Weighted<Self::Item>>>;

//     fn into_weighted_iter(self) -> Self::IntoIter {
//         self.0
//     }
// }

// #[cfg(feature = "local-batch")]
// pub fn map2<B>(&self, batch: B) -> impl Iterator<Item = Outcome<W>> + use<B, W, Q, T>
// where
//     B: IntoWeightedIterator<Item = W::Input>,
// {
//     let (tx, rx) = outcome_channel();
//     let task_ids: Vec<_> = batch
//         .into_weighted_iter()
//         .map(|(task, weight)| self.apply_send(task, &tx))
//         .collect();
//     drop(tx);
//     rx.select_ordered(task_ids)
// }
// #[cfg(test)]
// mod foo {
//     use super::*;
//     use crate::bee::stock::EchoWorker;

//     fn test_foo() {
//         let hive = DefaultHive::<EchoWorker<usize>>::default();
//         let result = hive
//             .map2(Weighted::from_iter(0..10, 0..10))
//             .collect::<Vec<_>>();
//     }
// }
