use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bonsaidb_core::{
    connection::Connection,
    schema::{view, CollectionName, Schema, ViewName},
};
use bonsaidb_jobs::{manager::Manager, task::Handle};
use tokio::sync::RwLock;

use crate::{
    database::Database,
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

type ViewKey = (Arc<Cow<'static, str>>, CollectionName, ViewName);

#[derive(Default, Debug)]
pub struct Statuses {
    completed_integrity_checks: HashSet<ViewKey>,
    view_update_last_status: HashMap<ViewKey, u64>,
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
        storage: &Database<DB>,
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
                if let Some(last_transaction_indexed) = statuses.view_update_last_status.get(&(
                    storage.data.name.clone(),
                    view.collection()?,
                    view.view_name()?,
                )) {
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
                                database: storage.data.name.clone(),
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
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
    ) -> bool {
        let statuses = self.statuses.read().await;
        statuses
            .completed_integrity_checks
            .contains(&(database, collection.clone(), view_name))
    }

    pub async fn spawn_integrity_check<DB: Schema>(
        &self,
        view: &dyn view::Serialized,
        database: &Database<DB>,
    ) -> Result<Option<Handle<(), Task>>, crate::Error> {
        let view_name = view.view_name()?;
        if !self
            .view_integrity_checked(
                database.data.name.clone(),
                view.collection()?,
                view_name.clone(),
            )
            .await
        {
            let job = self
                .jobs
                .lookup_or_enqueue(IntegrityScanner {
                    database: database.clone(),
                    scan: IntegrityScan {
                        database: database.data.name.clone(),
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
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
    ) {
        let mut statuses = self.statuses.write().await;
        statuses
            .completed_integrity_checks
            .insert((database, collection, view_name));
    }

    pub async fn mark_view_updated(
        &self,
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
        transaction_id: u64,
    ) {
        let mut statuses = self.statuses.write().await;
        statuses
            .view_update_last_status
            .insert((database, collection, view_name), transaction_id);
    }

    #[cfg(feature = "keyvalue")]
    pub async fn spawn_key_value_expiration_loader(
        &self,
        storage: &crate::Storage,
    ) -> Handle<(), Task> {
        self.jobs
            .enqueue(crate::storage::kv::ExpirationLoader {
                storage: storage.clone(),
            })
            .await
    }
}
