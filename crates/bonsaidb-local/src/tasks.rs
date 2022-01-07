use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_lock::RwLock;
use bonsaidb_core::{
    connection::Connection,
    schema::{view, CollectionName, ViewName},
};
use bonsaidb_utils::{fast_async_read, fast_async_write};

use crate::{
    database::Database,
    jobs::{manager::Manager, task::Handle},
    tasks::compactor::Compactor,
    views::{
        integrity_scanner::{IntegrityScan, IntegrityScanner},
        mapper::{Map, Mapper},
    },
    Error,
};

mod compactor;
mod task;

pub use task::Task;

#[derive(Debug, Clone)]
pub struct TaskManager {
    pub jobs: Manager<Task>,
    statuses: Arc<RwLock<Statuses>>,
}

type ViewKey = (Arc<Cow<'static, str>>, CollectionName, ViewName);

#[derive(Default, Debug)]
pub struct Statuses {
    completed_integrity_checks: HashSet<ViewKey>,
    key_value_expiration_loads: HashSet<Arc<Cow<'static, str>>>,
    view_update_last_status: HashMap<ViewKey, u64>,
}

impl TaskManager {
    pub fn new(jobs: Manager<Task>) -> Self {
        Self {
            jobs,
            statuses: Arc::default(),
        }
    }

    pub async fn update_view_if_needed(
        &self,
        view: &dyn view::Serialized,
        database: &Database,
    ) -> Result<(), crate::Error> {
        let view_name = view.view_name();
        if let Some(job) = self.spawn_integrity_check(view, database).await? {
            job.receive().await??;
        }

        // If there is no transaction id, there is no data, so the view is "up-to-date"
        if let Some(current_transaction_id) = database.last_transaction_id().await? {
            let needs_reindex = {
                // When views finish updating, they store the last transaction_id
                // they mapped. If that value is current, we don't need to go
                // through the jobs system at all.
                let statuses = fast_async_read!(self.statuses);
                if let Some(last_transaction_indexed) = statuses.view_update_last_status.get(&(
                    database.data.name.clone(),
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
                            database: database.clone(),
                            map: Map {
                                database: database.data.name.clone(),
                                collection: view.collection()?,
                                view_name: view_name.clone()?,
                            },
                        })
                        .await;
                    let id = job.receive().await??;
                    if wait_for_transaction <= id {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn key_value_expiration_loaded(&self, database: &Arc<Cow<'static, str>>) -> bool {
        let statuses = fast_async_read!(self.statuses);
        statuses.key_value_expiration_loads.contains(database)
    }

    pub async fn view_integrity_checked(
        &self,
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
    ) -> bool {
        let statuses = fast_async_read!(self.statuses);
        statuses
            .completed_integrity_checks
            .contains(&(database, collection.clone(), view_name))
    }

    pub async fn spawn_integrity_check(
        &self,
        view: &dyn view::Serialized,
        database: &Database,
    ) -> Result<Option<Handle<(), Error, Task>>, crate::Error> {
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
        let mut statuses = fast_async_write!(self.statuses);
        statuses
            .completed_integrity_checks
            .insert((database, collection, view_name));
    }

    pub async fn mark_key_value_expiration_loaded(&self, database: Arc<Cow<'static, str>>) {
        let mut statuses = fast_async_write!(self.statuses);
        statuses.key_value_expiration_loads.insert(database);
    }

    pub async fn mark_view_updated(
        &self,
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
        transaction_id: u64,
    ) {
        let mut statuses = fast_async_write!(self.statuses);
        statuses
            .view_update_last_status
            .insert((database, collection, view_name), transaction_id);
    }

    pub async fn spawn_key_value_expiration_loader(
        &self,
        database: &crate::Database,
    ) -> Option<Handle<(), Error, Task>> {
        if self.key_value_expiration_loaded(&database.data.name).await {
            None
        } else {
            Some(
                self.jobs
                    .enqueue(crate::database::keyvalue::ExpirationLoader {
                        database: database.clone(),
                    })
                    .await,
            )
        }
    }

    pub async fn compact_collection(
        &self,
        database: crate::Database,
        collection_name: CollectionName,
    ) -> Result<(), Error> {
        Ok(self
            .jobs
            .lookup_or_enqueue(Compactor::collection(database, collection_name))
            .await
            .receive()
            .await??)
    }

    pub async fn compact_key_value_store(&self, database: crate::Database) -> Result<(), Error> {
        Ok(self
            .jobs
            .lookup_or_enqueue(Compactor::keyvalue(database))
            .await
            .receive()
            .await??)
    }

    pub async fn compact_database(&self, database: crate::Database) -> Result<(), Error> {
        Ok(self
            .jobs
            .lookup_or_enqueue(Compactor::database(database))
            .await
            .receive()
            .await??)
    }
}
