use crate::{Job, Keyed};
use async_trait::async_trait;

use super::Manager;

#[derive(Debug)]
struct TestJob(usize);

#[async_trait]
impl Job for TestJob {
    type Output = usize;

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        Ok(self.0)
    }
}

impl Keyed<usize> for TestJob {
    fn key(&self) -> usize {
        self.0
    }
}

#[tokio::test]
async fn simple() -> Result<(), flume::RecvError> {
    let manager = Manager::<usize>::default();
    manager.spawn_worker();
    let handle = manager.enqueue(TestJob(1)).await;
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
    let handle = manager.lookup_or_enqueue(TestJob(1)).await;
    let handle2 = manager.lookup_or_enqueue(TestJob(1)).await;
    assert_eq!(handle.id, handle2.id);
    manager.spawn_worker();
    let (result1, result2) = tokio::try_join!(handle.receive(), handle2.receive())?;

    for result in vec![result1, result2] {
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
