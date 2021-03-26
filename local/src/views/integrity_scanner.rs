use std::{borrow::Cow, collections::HashSet, hash::Hash};

use async_trait::async_trait;
use pliantdb_core::schema::{collection, Database, Key};
use pliantdb_jobs::{Job, Keyed};
use sled::{IVec, Tree};

use crate::{storage::document_tree_name, Storage};

use super::{
    mapper::{Map, Mapper},
    view_document_map_tree_name, view_invalidated_docs_tree_name, Task,
};

#[derive(Debug)]
pub struct IntegrityScanner<DB> {
    pub storage: Storage<DB>,
    pub scan: IntegrityScan,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct IntegrityScan {
    pub view_version: usize,
    pub collection: collection::Id,
    pub view_name: Cow<'static, str>,
}

#[async_trait]
impl<DB> Job for IntegrityScanner<DB>
where
    DB: Database,
{
    type Output = ();

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let documents = self
            .storage
            .sled
            .open_tree(document_tree_name(&self.scan.collection))?;

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

        let needs_update = tokio::task::spawn_blocking::<_, anyhow::Result<bool>>(move || {
            let document_ids = tree_keys::<u64>(&documents)?;
            let stored_document_ids = tree_keys::<u64>(&document_map)?;

            let missing_entries = document_ids
                .difference(&stored_document_ids)
                .cloned()
                .collect::<HashSet<_>>();

            // TODO scan for existing view entries that are not up to the current view's version

            if !missing_entries.is_empty() {
                // Add all missing entries to the invalidated list. The view
                // mapping job will update them on the next pass.
                invalidated_entries
                    .transaction::<_, _, anyhow::Error>(|tree| {
                        for id in &missing_entries {
                            tree.insert(id.as_big_endian_bytes().as_ref(), IVec::default())?;
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
            println!("needs update after integrity scan");
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
            let updated_to_transaction = job.receive().await.unwrap();
            println!("Updated to transaction: {:?}", updated_to_transaction);
        }

        Ok(())
    }
}

fn tree_keys<K: Key + Hash + Eq + Clone>(tree: &Tree) -> Result<HashSet<K>, sled::Error> {
    let mut ids = HashSet::new();
    for result in tree.iter() {
        let (key, _) = result?;
        let key = K::from_big_endian_bytes(&key);
        ids.insert(key);
    }

    Ok(ids)
}

impl<DB> Keyed<Task> for IntegrityScanner<DB>
where
    DB: Database,
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
