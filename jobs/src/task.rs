use std::{fmt::Debug, sync::Arc};

use flume::Receiver;
use serde::{Deserialize, Serialize};

use crate::manager::Manager;

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub struct Id(pub(crate) u64);

#[derive(Debug)]
pub struct Handle<T, Key> {
    pub id: Id,
    pub(crate) manager: Manager<Key>,
    pub(crate) receiver: Receiver<Arc<Result<T, anyhow::Error>>>,
}

impl<T, Key> Handle<T, Key>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    pub async fn clone(&self) -> Self {
        let mut jobs = self.manager.jobs.write().await;
        jobs.create_new_task_handle(self.id, self.manager.clone())
    }

    pub async fn receive(&self) -> Result<Arc<Result<T, anyhow::Error>>, flume::RecvError> {
        self.receiver.recv_async().await
    }

    pub fn try_receive(&self) -> Result<Arc<Result<T, anyhow::Error>>, flume::TryRecvError> {
        self.receiver.try_recv()
    }
}
