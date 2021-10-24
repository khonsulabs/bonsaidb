use std::{fmt::Debug, sync::Arc};

use flume::Receiver;

use crate::manager::Manager;

/// he `Id` of an executing task.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub struct Id(pub(crate) u64);

/// References a background task.
#[derive(Debug)]
pub struct Handle<T, E, Key> {
    /// The task's id.
    pub id: Id,

    pub(crate) manager: Manager<Key>,
    pub(crate) receiver: Receiver<Result<T, Arc<E>>>,
}

impl<T, E, Key> Handle<T, E, Key>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    /// Returns a copy of this handle. When the job is completed, both handles
    /// will be able to `receive()` the results.
    pub async fn clone(&self) -> Self {
        let mut jobs = self.manager.jobs.write().await;
        jobs.create_new_task_handle(self.id, self.manager.clone())
    }

    /// Waits for the job to complete and returns the result.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is cancelled.
    pub async fn receive(&self) -> Result<Result<T, Arc<E>>, flume::RecvError> {
        self.receiver.recv_async().await
    }

    /// Tries to receive the status of the job. If available, it is returned.
    /// This function will not block.
    ///
    /// # Errors
    ///
    /// Returns an error if the job isn't complete.
    ///
    /// * [`TryRecvError::Disconnected`](flume::TryRecvError::Disconnected): The job has been cancelled.
    /// * [`TryRecvError::Empty`](flume::TryRecvError::Empty): The job has not completed yet.
    pub fn try_receive(&self) -> Result<Result<T, Arc<E>>, flume::TryRecvError> {
        self.receiver.try_recv()
    }
}
