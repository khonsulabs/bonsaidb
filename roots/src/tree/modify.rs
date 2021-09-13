use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use crate::Buffer;

#[derive(Debug)]
pub struct Modification<'a, T> {
    pub transaction_id: u64,
    pub keys: Vec<Buffer<'a>>,
    pub operation: Operation<T>,
}

#[derive(Debug)]
pub enum Operation<T> {
    Set(T),
    // SetPerKey(Vec<T>),
    Remove,
    CompareSwap(CompareSwap<T>),
}

pub type CompareSwapFn<T> = dyn FnMut(&Buffer<'static>, Option<T>) -> Option<T>;
pub struct CompareSwap<T>(Box<CompareSwapFn<T>>);

impl<T> CompareSwap<T> {
    pub fn new<F: FnMut(&Buffer<'static>, Option<T>) -> Option<T> + 'static>(callback: F) -> Self {
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
