use flume::TryRecvError;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, hash::Hash, time::Duration};

use crate::{Job, Keyed};
use async_trait::async_trait;

use super::Manager;

#[derive(Debug)]
struct Echo<T>(T);

#[async_trait]
impl<T> Job for Echo<T>
where
    T: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + Debug + Send + Sync + 'static,
{
    type Output = T;

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        Ok(self.0.clone())
    }
}

impl<T> Keyed<T> for Echo<T>
where
    T: Clone + Serialize + for<'de> Deserialize<'de> + Eq + Hash + Debug + Send + Sync + 'static,
{
    fn key(&self) -> T {
        self.0.clone()
    }
}

#[tokio::test]
async fn simple() -> Result<(), flume::RecvError> {
    let manager = Manager::<usize>::default();
    manager.spawn_worker();
    let handle = manager.enqueue(Echo(1)).await;
    if let Ok(value) = handle.receive().await?.as_ref() {
        assert_eq!(value, &1);

        Ok(())
    } else {
        unreachable!()
    }
}

#[tokio::test]
async fn keyed_simple() -> Result<(), flume::RecvError> {
    let manager = Manager::<usize>::default();
    let handle = manager.lookup_or_enqueue(Echo(1)).await;
    let handle2 = manager.lookup_or_enqueue(Echo(1)).await;
    // Tests that they received the same job id
    assert_eq!(handle.id, handle2.id);
    let handle3 = handle.clone().await;
    assert_eq!(handle3.id, handle.id);

    manager.spawn_worker();

    let (result1, result2) = tokio::try_join!(handle.receive(), handle2.receive())?;
    // Because they're all the same handle, if those have returned, this one
    // should be available without blocking.
    let result3 = handle3
        .try_receive()
        .expect("try_receive failed even though other channels were available");

    for result in vec![result1, result2, result3] {
        result
            .as_ref()
            .as_ref()
            .map(|value| {
                assert_eq!(value, &1);
            })
            .unwrap();
    }

    Ok(())
}
