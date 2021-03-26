use std::{borrow::Cow, collections::HashSet, sync::Arc};

use pliantdb_core::schema::{collection, view, Database};
use pliantdb_jobs::manager::Manager;
use tokio::sync::RwLock;

use crate::{
    views::{
        integrity_scanner::{IntegrityScan, IntegrityScanner},
        mapper::{Map, Mapper},
        view_invalidated_docs_tree_name, Task,
    },
    Storage,
};

#[derive(Debug, Clone)]
pub struct TaskManager {
    pub jobs: Manager<Task>,
    statuses: Arc<RwLock<Statuses>>,
}

#[derive(Default, Debug)]
pub struct Statuses {
    completed_integrity_checks: HashSet<(collection::Id, Cow<'static, str>)>,
}

impl TaskManager {
    pub fn new(jobs: Manager<Task>) -> Self {
        Self {
            jobs,
            statuses: Arc::default(),
        }
    }

    pub async fn update_view_if_needed<DB: Database>(
        &self,
        collection: collection::Id,
        view: &dyn view::Serialized,
        storage: &Storage<DB>,
    ) -> Result<(), crate::Error> {
        let view_name = view.name();
        if !self
            .view_integrity_checked(collection.clone(), view_name.clone())
            .await
        {
            let job = self
                .jobs
                .lookup_or_enqueue(IntegrityScanner {
                    storage: storage.clone(),
                    scan: IntegrityScan {
                        view_version: view.version(),
                        collection: collection.clone(),
                        view_name: view_name.clone(),
                    },
                })
                .await;
            job.receive().await.unwrap();
        }

        let needs_reindex = tokio::task::block_in_place(|| {
            let invalidated_docs = storage.sled.open_tree(view_invalidated_docs_tree_name(
                &collection,
                view_name.as_ref(),
            ));
            invalidated_docs.iter().next().is_some()
        });

        if needs_reindex {
            let wait_for_transaction = storage.last_transaction_id().await?.unwrap();
            loop {
                let job = self
                    .jobs
                    .lookup_or_enqueue(Mapper {
                        storage: storage.clone(),
                        map: Map {
                            collection: collection.clone(),
                            view_name: view_name.clone(),
                        },
                    })
                    .await;
                if let Ok(id) = job.receive().await?.as_ref() {
                    if wait_for_transaction <= *id {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn view_integrity_checked(
        &self,
        collection: collection::Id,
        view_name: Cow<'static, str>,
    ) -> bool {
        let statuses = self.statuses.read().await;
        statuses
            .completed_integrity_checks
            .contains(&(collection.clone(), view_name))
    }
}
