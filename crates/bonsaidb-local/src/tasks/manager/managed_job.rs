use std::fmt::Debug;

use crate::tasks::{handle::Id, manager::Manager, traits::Executable, Job};

#[derive(Debug)]
pub struct ManagedJob<J, Key> {
    pub id: Id,
    pub job: J,
    pub manager: Manager<Key>,
    pub key: Option<Key>,
}

impl<J, Key> Executable for ManagedJob<J, Key>
where
    J: Job,
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    fn execute(&mut self) {
        let result = self.job.execute();

        self.manager
            .job_completed(self.id, self.key.as_ref(), result);
    }
}
