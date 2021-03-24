use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};

use crate::{
    manager::{ManagedJob, Manager},
    task::{Handle, Id},
    traits::Executable,
    Job, Keyed,
};

#[derive(Debug)]
pub struct Jobs<Key> {
    last_task_id: u64,
    result_senders: HashMap<Id, Vec<Box<dyn AnySender>>>,
    keyed_jobs: HashMap<Key, Id>,
    queuer: Sender<Box<dyn Executable>>,
    queue: Receiver<Box<dyn Executable>>,
}

impl<Key> Jobs<Key>
where
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    pub fn new() -> Self {
        let (queuer, queue) = flume::unbounded();

        Self {
            last_task_id: 0,
            result_senders: HashMap::new(),
            keyed_jobs: HashMap::new(),
            queuer,
            queue,
        }
    }

    pub fn queue(&self) -> Receiver<Box<dyn Executable>> {
        self.queue.clone()
    }

    pub fn enqueue<J: Job + 'static>(
        &mut self,
        job: J,
        key: Option<Key>,
        manager: Manager<Key>,
    ) -> Handle<J::Output, Key> {
        self.last_task_id = self.last_task_id.wrapping_add(1);
        let id = Id(self.last_task_id);
        self.queuer
            .send(Box::new(ManagedJob {
                id,
                job,
                key,
                manager: manager.clone(),
            }))
            .unwrap();

        self.create_new_task_handle(id, manager)
    }

    pub fn create_new_task_handle<
        T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    >(
        &mut self,
        id: Id,
        manager: Manager<Key>,
    ) -> Handle<T, Key> {
        let (sender, receiver) = flume::bounded(1);
        let senders = self.result_senders.entry(id).or_insert_with(Vec::default);
        senders.push(Box::new(sender));

        Handle {
            id,
            manager,
            receiver,
        }
    }

    pub fn lookup_or_enqueue<J: Keyed<Key>>(
        &mut self,
        job: J,
        manager: Manager<Key>,
    ) -> Handle<<J as Job>::Output, Key> {
        if let Some(&id) = self.keyed_jobs.get(job.key()) {
            self.create_new_task_handle(id, manager)
        } else {
            let key = job.key().clone();
            let handle = self.enqueue(job, Some(key.clone()), manager);
            self.keyed_jobs.insert(key, handle.id);
            handle
        }
    }

    pub async fn job_completed<T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static>(
        &mut self,
        id: Id,
        key: Option<&Key>,
        result: Result<T, anyhow::Error>,
    ) {
        if let Some(senders) = self.result_senders.remove(&id) {
            tokio::spawn(async move {
                let result = &Arc::new(result);
                futures::future::join_all(senders.into_iter().map(|handle| async move {
                    let handle = handle
                        .as_any()
                        .downcast_ref::<Sender<Arc<Result<T, anyhow::Error>>>>()
                        .unwrap();
                    handle.send_async(result.clone()).await
                }))
                .await;
            });
        }

        if let Some(key) = key {
            self.keyed_jobs.remove(key);
        }
    }
}

pub trait AnySender: Any + Send + Sync + Debug {
    fn as_any(&self) -> &'_ dyn Any;
}

impl<T> AnySender for Sender<Arc<Result<T, anyhow::Error>>>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    fn as_any(&self) -> &'_ dyn Any {
        self
    }
}
