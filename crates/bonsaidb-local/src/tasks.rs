use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bonsaidb_core::{
    connection::Connection,
    keyvalue::Timestamp,
    schema::{view, CollectionName, ViewName},
};
use parking_lot::RwLock;

use crate::{
    database::{keyvalue::ExpirationLoader, Database},
    tasks::{compactor::Compactor, handle::Handle, manager::Manager},
    views::{
        integrity_scanner::{IntegrityScan, IntegrityScanner, OptionalViewMapHandle},
        mapper::{Map, Mapper},
    },
    Error,
};

/// Types related to defining [`Job`]s.
pub mod handle;
/// Types related to the job [`Manager`](manager::Manager).
pub mod manager;
mod traits;

pub use self::traits::{Job, Keyed};

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

    pub fn update_view_if_needed(
        &self,
        view: &dyn view::Serialized,
        database: &Database,
    ) -> Result<(), crate::Error> {
        let view_name = view.view_name();
        if let Some(job) = self.spawn_integrity_check(view, database) {
            job.receive()??;
        }

        // If there is no transaction id, there is no data, so the view is "up-to-date"
        if let Some(current_transaction_id) = database.last_transaction_id()? {
            let needs_reindex = {
                // When views finish updating, they store the last transaction_id
                // they mapped. If that value is current, we don't need to go
                // through the jobs system at all.
                let statuses = self.statuses.read();
                if let Some(last_transaction_indexed) = statuses.view_update_last_status.get(&(
                    database.data.name.clone(),
                    view.collection(),
                    view.view_name(),
                )) {
                    last_transaction_indexed < &current_transaction_id
                } else {
                    true
                }
            };

            if needs_reindex {
                let wait_for_transaction = current_transaction_id;
                loop {
                    let job = self.jobs.lookup_or_enqueue(Mapper {
                        database: database.clone(),
                        map: Map {
                            database: database.data.name.clone(),
                            collection: view.collection(),
                            view_name: view_name.clone(),
                        },
                    });
                    let id = job.receive()??;
                    if wait_for_transaction <= id {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn key_value_expiration_loaded(&self, database: &Arc<Cow<'static, str>>) -> bool {
        let statuses = self.statuses.read();
        statuses.key_value_expiration_loads.contains(database)
    }

    pub fn view_integrity_checked(
        &self,
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
    ) -> bool {
        let statuses = self.statuses.read();
        statuses
            .completed_integrity_checks
            .contains(&(database, collection, view_name))
    }

    pub fn spawn_integrity_check(
        &self,
        view: &dyn view::Serialized,
        database: &Database,
    ) -> Option<Handle<OptionalViewMapHandle, Error>> {
        let view_name = view.view_name();
        if self.view_integrity_checked(
            database.data.name.clone(),
            view.collection(),
            view_name.clone(),
        ) {
            None
        } else {
            let job = self.jobs.lookup_or_enqueue(IntegrityScanner {
                database: database.clone(),
                scan: IntegrityScan {
                    database: database.data.name.clone(),
                    view_version: view.version(),
                    collection: view.collection(),
                    view_name,
                },
            });
            Some(job)
        }
    }

    pub fn mark_integrity_check_complete(
        &self,
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
    ) {
        let mut statuses = self.statuses.write();
        statuses
            .completed_integrity_checks
            .insert((database, collection, view_name));
    }

    pub fn mark_key_value_expiration_loaded(&self, database: Arc<Cow<'static, str>>) {
        let mut statuses = self.statuses.write();
        statuses.key_value_expiration_loads.insert(database);
    }

    pub fn mark_view_updated(
        &self,
        database: Arc<Cow<'static, str>>,
        collection: CollectionName,
        view_name: ViewName,
        transaction_id: u64,
    ) {
        let mut statuses = self.statuses.write();
        statuses
            .view_update_last_status
            .insert((database, collection, view_name), transaction_id);
    }

    pub fn spawn_key_value_expiration_loader(
        &self,
        database: &Database,
    ) -> Option<Handle<(), Error>> {
        if self.key_value_expiration_loaded(&database.data.name) {
            None
        } else {
            Some(self.jobs.lookup_or_enqueue(ExpirationLoader {
                database: database.clone(),
                launched_at: Timestamp::now(),
            }))
        }
    }

    pub fn spawn_compact_target(
        &self,
        database: Database,
        target: compactor::Target,
    ) -> Handle<(), Error> {
        self.jobs
            .lookup_or_enqueue(Compactor::target(database, target))
    }

    pub fn compact_collection(
        &self,
        database: Database,
        collection_name: CollectionName,
    ) -> Result<(), Error> {
        Ok(self
            .jobs
            .lookup_or_enqueue(Compactor::collection(database, collection_name))
            .receive()??)
    }

    pub fn compact_key_value_store(&self, database: Database) -> Result<(), Error> {
        Ok(self
            .jobs
            .lookup_or_enqueue(Compactor::keyvalue(database))
            .receive()??)
    }

    pub fn compact_database(&self, database: Database) -> Result<(), Error> {
        Ok(self
            .jobs
            .lookup_or_enqueue(Compactor::database(database))
            .receive()??)
    }
}
