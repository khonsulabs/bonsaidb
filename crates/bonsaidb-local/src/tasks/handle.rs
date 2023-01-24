use std::fmt::Debug;
use std::sync::Arc;

/// he `Id` of an executing task.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub struct Id(pub(crate) u64);

/// References a background task.
#[derive(Debug)]
pub struct Handle<T, E> {
    /// The task's id.
    pub id: Id,

    pub(crate) receiver: flume::Receiver<Result<T, Arc<E>>>,
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
    pub fn receive(self) -> Result<Result<T, Arc<E>>, flume::RecvError> {
        self.receiver.recv()
    }
}
