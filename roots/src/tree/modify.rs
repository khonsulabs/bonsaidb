use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use super::btree_entry::KeyOperation;
use crate::{Buffer, Error};

/// A tree modification.
#[derive(Debug)]
pub struct Modification<'a, T> {
    /// The transaction ID to store with this change.
    pub transaction_id: u64,
    /// The keys to operate upon.
    pub keys: Vec<Buffer<'a>>,
    /// The operation to perform on the keys.
    pub operation: Operation<T>,
}

impl<'a, T> Modification<'a, T> {
    pub(crate) fn reverse(&mut self) -> Result<(), Error> {
        if self.keys.windows(2).all(|w| w[0] < w[1]) {
            self.keys.reverse();
            if let Operation::SetEach(values) = &mut self.operation {
                values.reverse();
            }
            Ok(())
        } else {
            Err(Error::KeysNotOrdered)
        }
    }
}

/// An operation that is performed on a set of keys.
#[derive(Debug)]
pub enum Operation<T> {
    /// Sets all keys to the value.
    Set(T),
    /// Sets each key to the corresponding entry in this value. The number of
    /// keys must match the number of values.
    SetEach(Vec<T>),
    /// Removes the keys.
    Remove,
    /// Executes the `CompareSwap`. The original value (or `None` if not
    /// present) is the only argument.
    CompareSwap(CompareSwap<T>),
}

/// A function that is allowed to check the current value of a key and determine
/// how to operate on it. The first parameter is the key, and the second
/// parameter is the current value, if present.
pub type CompareSwapFn<T> = dyn FnMut(&Buffer<'static>, Option<T>) -> KeyOperation<T>;

/// A wrapper for a [`CompareSwapFn`].
pub struct CompareSwap<T>(Box<CompareSwapFn<T>>);

impl<T> CompareSwap<T> {
    /// Returns a new wrapped callback.
    pub fn new<F: FnMut(&Buffer<'static>, Option<T>) -> KeyOperation<T> + 'static>(
        callback: F,
    ) -> Self {
        Self(Box::new(callback))
    }
}

impl<T> Debug for CompareSwap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompareSwap(dyn FnMut)")
    }
}

impl<T: 'static> Deref for CompareSwap<T> {
    type Target = CompareSwapFn<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: 'static> DerefMut for CompareSwap<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
