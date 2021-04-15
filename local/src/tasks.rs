use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use pliantdb_core::{
    connection::Connection,
    schema::{view, CollectionId, Schema},
};
use pliantdb_jobs::{manager::Manager, task::Handle};
use tokio::sync::RwLock;

use crate::{
    storage::Storage,
    views::{
        integrity_scanner::{IntegrityScan, IntegrityScanner},
        mapper::{Map, Mapper},
        Task,
    },
};

#[derive(Debug, Clone)]
pub struct TaskManager {
    pub jobs: Manager<Task>,
    statuses: Arc<RwLock<Statuses>>,
}

#[derive(Default, Debug)]
pub struct Statuses {
    completed_integrity_checks: HashSet<(CollectionId, Cow<'static, str>)>,
    view_update_last_status: HashMap<(CollectionId, Cow<'static, str>), u64>,
}

impl TaskManager {
    pub fn new(jobs: Manager<Task>) -> Self {
        Self {
            jobs,
            statuses: Arc::default(),
        }
    }

    pub async fn update_view_if_needed<DB: Schema>(
        &self,
        view: &dyn view::Serialized,
        storage: &Storage<DB>,
    ) -> Result<(), crate::Error> {
        let view_name = view.name();
        if let Some(job) = self.spawn_integrity_check(view, storage).await? {
            job.receive().await?.map_err(crate::Error::Other)?;
        }

        // If there is no transaction id, there is no data, so the view is "up-to-date"
        if let Some(current_transaction_id) = storage.last_transaction_id().await? {
            let needs_reindex = {
                // When views finish updating, they store the last transaction_id
                // they mapped. If that value is current, we don't need to go
                // through the jobs system at all.
                let statuses = self.statuses.read().await;
                if let Some(last_transaction_indexed) = statuses
                    .view_update_last_status
                    .get(&(view.collection(), view.name()))
                {
                    last_transaction_indexed < &current_transaction_id
                } else {
                    true
                }
            };

            if needs_reindex {
                let wait_for_transaction = current_transaction_id;
                loop {
                    let job = self
                        .jobs
                        .lookup_or_enqueue(Mapper {
                            storage: storage.clone(),
                            map: Map {
                                collection: view.collection(),
                                view_name: view_name.clone(),
                            },
                        })
                        .await;
                    match job.receive().await?.as_ref() {
                        Ok(id) => {
                            if wait_for_transaction <= *id {
                                break;
                            }
                        }
                        Err(err) => {
                            return Err(crate::Error::Other(Arc::new(anyhow::Error::msg(
                                err.to_string(),
                            ))))
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn view_integrity_checked(
        &self,
        collection: CollectionId,
        view_name: Cow<'static, str>,
    ) -> bool {
        let statuses = self.statuses.read().await;
        statuses
            .completed_integrity_checks
            .contains(&(collection.clone(), view_name))
    }

    pub async fn spawn_integrity_check<DB: Schema>(
        &self,
        view: &dyn view::Serialized,
        storage: &Storage<DB>,
    ) -> Result<Option<Handle<(), Task>>, crate::Error> {
        let view_name = view.name();
        if !self
            .view_integrity_checked(view.collection(), view_name.clone())
            .await
        {
            let job = self
                .jobs
                .lookup_or_enqueue(IntegrityScanner {
                    storage: storage.clone(),
                    scan: IntegrityScan {
                        view_version: view.version(),
                        collection: view.collection(),
                        view_name: view_name.clone(),
                    },
                })
                .await;
            return Ok(Some(job));
        }

        Ok(None)
    }

    pub async fn mark_integrity_check_complete(
        &self,
        collection: CollectionId,
        view_name: Cow<'static, str>,
    ) {
        let mut statuses = self.statuses.write().await;
        statuses
            .completed_integrity_checks
            .insert((collection, view_name));
    }

    pub async fn mark_view_updated(
        &self,
        collection: CollectionId,
        view_name: Cow<'static, str>,
        transaction_id: u64,
    ) {
        let mut statuses = self.statuses.write().await;
        statuses
            .view_update_last_status
            .insert((collection, view_name), transaction_id);
    }
}
