use std::{fmt::Debug, sync::Arc};

use tokio::sync::oneshot;

/// he `Id` of an executing task.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub struct Id(pub(crate) u64);

/// References a background task.
#[derive(Debug)]
pub struct Handle<T, E> {
    /// The task's id.
    pub id: Id,

    pub(crate) receiver: oneshot::Receiver<Result<T, Arc<E>>>,
}

impl<T, E> Handle<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    /// Waits for the job to complete and returns the result.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is cancelled.
    pub async fn receive(
        self,
    ) -> Result<Result<T, Arc<E>>, tokio::sync::oneshot::error::RecvError> {
        self.receiver.await
    }

    // /// Tries to receive the status of the job. If available, it is returned.
    // /// This function will not block.
    // ///
    // /// # Errors
    // ///
    // /// Returns an error if the job isn't complete.
    // ///
    // /// * [`TryRecvError::Disconnected`](flume::TryRecvError::Disconnected): The job has been cancelled.
    // /// * [`TryRecvError::Empty`](flume::TryRecvError::Empty): The job has not completed yet.
    #[cfg(test)]
    pub fn try_receive(
        &mut self,
    ) -> Result<Result<T, Arc<E>>, tokio::sync::oneshot::error::TryRecvError> {
        self.receiver.try_recv()
    }
}
