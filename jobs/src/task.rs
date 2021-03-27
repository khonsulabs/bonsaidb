use std::{fmt::Debug, sync::Arc};

use flume::Receiver;
use serde::{Deserialize, Serialize};

use crate::manager::Manager;

/// he `Id` of an executing task.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub struct Id(pub(crate) u64);

/// References a background task.
#[derive(Debug)]
pub struct Handle<T, Key> {
    /// The task's id.
    pub id: Id,

    pub(crate) manager: Manager<Key>,
    pub(crate) receiver: Receiver<Arc<Result<T, anyhow::Error>>>,
}

impl<T, Key> Handle<T, Key>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    /// Returns a copy of this handle. When the job is completed, both handles
    /// will be able to `receive()` the results.
    pub async fn clone(&self) -> Self {
        let mut jobs = self.manager.jobs.write().await;
        jobs.create_new_task_handle(self.id, self.manager.clone())
    }

    /// Waits for the job to complete and returns the result.
    pub async fn receive(&self) -> Result<Arc<Result<T, anyhow::Error>>, flume::RecvError> {
        self.receiver.recv_async().await
    }

    /// Tries to receive the status of the job. If available, it is returned.
    /// This function will not block.
    pub fn try_receive(&self) -> Result<Arc<Result<T, anyhow::Error>>, flume::TryRecvError> {
        self.receiver.try_recv()
    }
}
