use std::fmt::Debug;

use async_trait::async_trait;

/// Defines a background job that can be queued and executed.
#[async_trait]
pub trait Job: Debug + Send + Sync + 'static {
    /// The output type of the job.
    type Output: Clone + Send + Sync + 'static;
    /// The error type of the job.
    type Error: Send + Sync + 'static;

    /// Executes the job and returns the result.
    async fn execute(&mut self) -> Result<Self::Output, Self::Error>;
}

/// Defines a background job that has a unique `key`.
pub trait Keyed<Key>: Job
where
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    /// The unique `key` for this `Job`
    fn key(&self) -> Key;
}

#[async_trait]
pub trait Executable: Send + Sync + Debug {
    async fn execute(&mut self);
}
