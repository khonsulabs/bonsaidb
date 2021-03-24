use crate::{manager::Manager, task::Id, traits::Executable, Job};
use async_trait::async_trait;
use std::fmt::Debug;

#[derive(Debug)]
pub struct ManagedJob<J, Key> {
    pub id: Id,
    pub job: J,
    pub manager: Manager<Key>,
    pub key: Option<Key>,
}

#[async_trait]
impl<J, Key> Executable for ManagedJob<J, Key>
where
    J: Job,
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    async fn execute(&mut self) {
        let result = self.job.execute().await;

        self.manager
            .job_completed(self.id, self.key.as_ref(), result)
            .await;
    }
}
