use std::{borrow::Cow, collections::HashSet, hash::Hash};

use async_trait::async_trait;
use pliantdb_core::schema::{collection, view, Key, Schema};
use pliantdb_jobs::{Job, Keyed};
use sled::{IVec, Transactional, Tree};

use super::{
    mapper::{Map, Mapper},
    view_document_map_tree_name, view_invalidated_docs_tree_name, view_versions_tree_name, Task,
};
use crate::storage::{document_tree_name, Storage};

#[derive(Debug)]
pub struct IntegrityScanner<DB> {
    pub storage: Storage<DB>,
    pub scan: IntegrityScan,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct IntegrityScan {
    pub view_version: u64,
    pub collection: collection::Id,
    pub view_name: Cow<'static, str>,
}

#[async_trait]
impl<DB> Job for IntegrityScanner<DB>
where
    DB: Schema,
{
    type Output = ();

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let documents = self
            .storage
            .sled
            .open_tree(document_tree_name(&self.scan.collection))?;

        let view_versions = self
            .storage
            .sled
            .open_tree(view_versions_tree_name(&self.scan.collection))?;

        let document_map = self.storage.sled.open_tree(view_document_map_tree_name(
            &self.scan.collection,
            &self.scan.view_name,
        ))?;

        let invalidated_entries = self
            .storage
            .sled
            .open_tree(view_invalidated_docs_tree_name(
                &self.scan.collection,
                &self.scan.view_name,
            ))?;

        let view_name = self.scan.view_name.clone();
        let view_version = self.scan.view_version;

        let needs_update = tokio::task::spawn_blocking::<_, anyhow::Result<bool>>(move || {
            let document_ids = tree_keys::<u64>(&documents)?;
            let view_is_current_version =
                if let Some(version) = view_versions.get(view_name.as_bytes())? {
                    if let Ok(version) = u64::from_big_endian_bytes(&version) {
                        version == view_version
                    } else {
                        false
                    }
                } else {
                    false
                };

            let missing_entries = if view_is_current_version {
                let stored_document_ids = tree_keys::<u64>(&document_map)?;

                document_ids
                    .difference(&stored_document_ids)
                    .cloned()
                    .collect::<HashSet<_>>()
            } else {
                // The view isn't the current version, queue up all documents.
                document_ids
            };

            if !missing_entries.is_empty() {
                // Add all missing entries to the invalidated list. The view
                // mapping job will update them on the next pass.
                (&invalidated_entries, &view_versions)
                    .transaction(|(invalidated_entries, view_versions)| {
                        view_versions.insert(
                            view_name.as_bytes(),
                            view_version.as_big_endian_bytes().unwrap().as_ref(),
                        )?;
                        for id in &missing_entries {
                            invalidated_entries.insert(
                                id.as_big_endian_bytes().unwrap().as_ref(),
                                IVec::default(),
                            )?;
                        }
                        Ok(())
                    })
                    .map_err(|err| match err {
                        sled::transaction::TransactionError::Abort(err) => err,
                        sled::transaction::TransactionError::Storage(err) => {
                            anyhow::Error::from(err)
                        }
                    })?;

                return Ok(true);
            }

            Ok(false)
        })
        .await??;

        if needs_update {
            let job = self
                .storage
                .tasks
                .jobs
                .lookup_or_enqueue(Mapper {
                    storage: self.storage.clone(),
                    map: Map {
                        collection: self.scan.collection.clone(),
                        view_name: self.scan.view_name.clone(),
                    },
                })
                .await;
            job.receive().await.unwrap();
        }

        self.storage
            .tasks
            .mark_integrity_check_complete(
                self.scan.collection.clone(),
                self.scan.view_name.clone(),
            )
            .await;

        Ok(())
    }
}

fn tree_keys<K: Key + Hash + Eq + Clone>(tree: &Tree) -> Result<HashSet<K>, anyhow::Error> {
    let mut ids = HashSet::new();
    for result in tree.iter() {
        let (key, _) = result?;
        let key = K::from_big_endian_bytes(&key).map_err(view::Error::KeySerialization)?;
        ids.insert(key);
    }

    Ok(ids)
}

impl<DB> Keyed<Task> for IntegrityScanner<DB>
where
    DB: Schema,
{
    fn key(&self) -> Task {
        Task::IntegrityScan(self.scan.clone())
    }
}

// The reason we use jobs like this is to make sure we can tweak how much is
// happening at any given time.
//
// On the Server level, we'll need to cooperate with all the databases in a
// shared pool of workers. So, we need to come up with a design for the view
// updaters to work within this limitation.
//
// Integrity scan is simple: Have a shared structure on Storage that keeps track
// of all integrity scan results. It can check for an existing value and return,
// or make you wait until the job is finished. For views, I suppose the best
// that can be done is a similar approach, but the indexer's output is the last
// transaction id it synced. When a request comes in, a check can be done if
// there are any docs outdated, if so, the client can get the current transaction id
// and ask the ViewScanning service manager to wait until that txid is scanned.
//
// The view can then scan and return the results it finds with confidence it was updated to that time.
// If new requests come in while the current batch is being caught up to,
