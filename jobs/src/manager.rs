use std::{fmt::Debug, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    task::{Handle, Id},
    Job, Keyed,
};

pub(crate) mod jobs;
mod managed_job;
pub(crate) use managed_job::ManagedJob;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct Manager<Key = ()> {
    pub(crate) jobs: Arc<RwLock<jobs::Jobs<Key>>>,
}

impl<Key> Default for Manager<Key>
where
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    fn default() -> Self {
        Self {
            jobs: Arc::new(RwLock::new(jobs::Jobs::new())),
        }
    }
}

impl<Key> Manager<Key>
where
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    pub async fn enqueue<J: Job + 'static>(&self, job: J) -> Handle<J::Output, Key> {
        let mut jobs = self.jobs.write().await;
        jobs.enqueue(job, None, self.clone())
    }

    pub async fn lookup_or_enqueue<J: Keyed<Key>>(
        &self,
        job: J,
    ) -> Handle<<J as Job>::Output, Key> {
        let mut jobs = self.jobs.write().await;
        jobs.lookup_or_enqueue(job, self.clone())
    }

    pub async fn job_completed<T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static>(
        &self,
        id: Id,
        key: Option<&Key>,
        result: Result<T, anyhow::Error>,
    ) {
        let mut jobs = self.jobs.write().await;
        jobs.job_completed(id, key, result).await
    }

    pub fn spawn_worker(&self) {
        let manager = self.clone();
        tokio::spawn(async move { manager.execute_jobs().await });
    }

    async fn execute_jobs(&self) {
        let receiver = {
            let jobs = self.jobs.read().await;
            jobs.queue()
        };
        while let Ok(mut job) = receiver.recv_async().await {
            job.execute().await
        }
    }
}
