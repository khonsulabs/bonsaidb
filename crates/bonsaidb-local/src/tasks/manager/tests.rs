use std::{convert::Infallible, fmt::Debug, hash::Hash};

use super::Manager;
use crate::tasks::{Job, Keyed};

#[derive(Debug)]
struct Echo<T>(T);

impl<T> Job for Echo<T>
where
    T: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    type Output = T;
    type Error = Infallible;

    fn execute(&mut self) -> Result<Self::Output, Self::Error> {
        Ok(self.0.clone())
    }
}

impl<T> Keyed<T> for Echo<T>
where
    T: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    fn key(&self) -> T {
        self.0.clone()
    }
}

#[test]
fn simple() -> Result<(), flume::RecvError> {
    let manager = Manager::<usize>::default();
    manager.spawn_worker();
    let handle = manager.enqueue(Echo(1));
    if let Ok(value) = handle.receive()? {
        assert_eq!(value, 1);

        Ok(())
    } else {
        unreachable!()
    }
}

#[test]
fn keyed_simple() {
    let manager = Manager::<usize>::default();
    let handle = manager.lookup_or_enqueue(Echo(1));
    let handle2 = manager.lookup_or_enqueue(Echo(1));
    // Tests that they received the same job id
    assert_eq!(handle.id, handle2.id);
    let handle3 = manager.lookup_or_enqueue(Echo(1));
    assert_eq!(handle3.id, handle.id);

    manager.spawn_worker();

    let result1 = handle.receive().unwrap();
    let result2 = handle2.receive().unwrap();
    // Because they're all the same handle, if those have returned, this one
    // should be available without blocking.
    let result3 = handle3.receive().unwrap();

    for result in [result1, result2, result3] {
        assert_eq!(result.unwrap(), 1);
    }
}
