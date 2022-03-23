use std::{fmt::Debug, sync::Arc};

use derive_where::derive_where;
use parking_lot::RwLock;

use crate::tasks::{
    handle::{Handle, Id},
    traits::Executable,
    Job, Keyed,
};

pub(crate) mod jobs;
mod managed_job;
pub(crate) use managed_job::ManagedJob;

#[cfg(test)]
mod tests;

/// A background jobs manager.
#[derive(Debug)]
#[derive_where(Clone, Default)]
pub struct Manager<Key = ()> {
    // #[derive_where(default)]
    pub(crate) jobs: Arc<RwLock<jobs::Jobs<Key>>>,
}

impl<Key> Manager<Key>
where
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    /// Pushes a `job` into the queue. Pushing the same job definition twice
    /// will yield two tasks in the queue.
    #[cfg(test)]
    pub fn enqueue<J: Job + 'static>(&self, job: J) -> Handle<J::Output, J::Error> {
        let mut jobs = self.jobs.write();
        jobs.enqueue(job, None, self.clone())
    }

    /// Uses [`Keyed::key`] to ensure no other job with the same `key` is
    /// currently running. If another job is already running that matches, a
    /// clone of that [`Handle`] will be returned. When the job finishes, all
    /// [`Handle`] clones will be notified with a copy of the result.
    pub fn lookup_or_enqueue<J: Keyed<Key>>(
        &self,
        job: J,
    ) -> Handle<<J as Job>::Output, <J as Job>::Error> {
        let mut jobs = self.jobs.write();
        jobs.lookup_or_enqueue(job, self.clone())
    }

    fn job_completed<T: Clone + Send + Sync + 'static, E: Send + Sync + 'static>(
        &self,
        id: Id,
        key: Option<&Key>,
        result: Result<T, E>,
    ) {
        let mut jobs = self.jobs.write();
        jobs.job_completed(id, key, result);
    }

    /// Spawns a worker. In general, you shouldn't need to call this function
    /// directly.
    pub fn spawn_worker(&self) {
        let receiver = {
            let jobs = self.jobs.read();
            jobs.queue()
        };
        std::thread::Builder::new()
            .name(String::from("bonsaidb-tasks"))
            .spawn(move || worker_thread(&receiver))
            .unwrap();
    }
}

fn worker_thread(receiver: &flume::Receiver<Box<dyn Executable>>) {
    while let Ok(mut job) = receiver.recv() {
        job.execute();
    }
}
