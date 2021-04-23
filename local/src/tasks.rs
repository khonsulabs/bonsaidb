use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use pliantdb_core::{
    connection::Connection,
    schema::{view, CollectionName, Schema, ViewName},
};
use pliantdb_jobs::{manager::Manager, task::Handle};
use tokio::sync::RwLock;

use crate::{
    storage::{kv::ExpirationLoader, Storage},
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
    completed_integrity_checks: HashSet<(CollectionName, ViewName)>,
    view_update_last_status: HashMap<(CollectionName, ViewName), u64>,
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
        let view_name = view.view_name();
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
                    .get(&(view.collection()?, view.view_name()?))
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
                                collection: view.collection()?,
                                view_name: view_name.clone()?,
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
        collection: CollectionName,
        view_name: ViewName,
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
        let view_name = view.view_name()?;
        if !self
            .view_integrity_checked(view.collection()?, view_name.clone())
            .await
        {
            let job = self
                .jobs
                .lookup_or_enqueue(IntegrityScanner {
                    storage: storage.clone(),
                    scan: IntegrityScan {
                        view_version: view.version(),
                        collection: view.collection()?,
                        view_name,
                    },
                })
                .await;
            return Ok(Some(job));
        }

        Ok(None)
    }

    pub async fn mark_integrity_check_complete(
        &self,
        collection: CollectionName,
        view_name: ViewName,
    ) {
        let mut statuses = self.statuses.write().await;
        statuses
            .completed_integrity_checks
            .insert((collection, view_name));
    }

    pub async fn mark_view_updated(
        &self,
        collection: CollectionName,
        view_name: ViewName,
        transaction_id: u64,
    ) {
        let mut statuses = self.statuses.write().await;
        statuses
            .view_update_last_status
            .insert((collection, view_name), transaction_id);
    }

    pub async fn spawn_key_value_expiration_loader<DB: Schema>(
        &self,
        storage: &Storage<DB>,
    ) -> Handle<(), Task> {
        self.jobs
            .enqueue(ExpirationLoader {
                storage: storage.clone(),
            })
            .await
    }
}
