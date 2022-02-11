use std::{borrow::Cow, collections::HashSet, convert::Infallible, hash::Hash, sync::Arc};

use async_trait::async_trait;
use bonsaidb_core::schema::{view, CollectionName, Key, ViewName};
use nebari::{
    io::any::AnyFile,
    tree::{KeyEvaluation, Operation, Unversioned, Versioned},
    ArcBytes, Tree,
};

use super::{
    mapper::{Map, Mapper},
    view_document_map_tree_name, view_invalidated_docs_tree_name, view_versions_tree_name,
};
use crate::{
    database::{document_tree_name, Database},
    jobs::{Job, Keyed},
    tasks::Task,
    Error,
};

#[derive(Debug)]
pub struct IntegrityScanner {
    pub database: Database,
    pub scan: IntegrityScan,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct IntegrityScan {
    pub view_version: u64,
    pub database: Arc<Cow<'static, str>>,
    pub collection: CollectionName,
    pub view_name: ViewName,
}

#[async_trait]
impl Job for IntegrityScanner {
    type Output = ();
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    #[allow(clippy::too_many_lines)]
    async fn execute(&mut self) -> Result<Self::Output, Self::Error> {
        let documents =
            self.database
                .roots()
                .tree(self.database.collection_tree::<Versioned, _>(
                    &self.scan.collection,
                    document_tree_name(&self.scan.collection),
                )?)?;

        let view_versions_tree = self.database.collection_tree::<Unversioned, _>(
            &self.scan.collection,
            view_versions_tree_name(&self.scan.collection),
        )?;
        let view_versions = self.database.roots().tree(view_versions_tree.clone())?;

        let document_map =
            self.database
                .roots()
                .tree(self.database.collection_tree::<Unversioned, _>(
                    &self.scan.collection,
                    view_document_map_tree_name(&self.scan.view_name),
                )?)?;

        let invalidated_entries_tree = self.database.collection_tree::<Unversioned, _>(
            &self.scan.collection,
            view_invalidated_docs_tree_name(&self.scan.view_name),
        )?;

        let view_name = self.scan.view_name.clone();
        let view_version = self.scan.view_version;
        let roots = self.database.roots().clone();

        let needs_update = tokio::task::spawn_blocking::<_, Result<bool, Error>>(move || {
            let document_ids = tree_keys::<u64, Versioned>(&documents)?;
            let view_is_current_version =
                if let Some(version) = view_versions.get(view_name.to_string().as_bytes())? {
                    if let Ok(version) = u64::from_big_endian_bytes(version.as_slice()) {
                        version == view_version
                    } else {
                        false
                    }
                } else {
                    false
                };

            let missing_entries = if view_is_current_version {
                let stored_document_ids = tree_keys::<u64, Unversioned>(&document_map)?;

                document_ids
                    .difference(&stored_document_ids)
                    .copied()
                    .collect::<HashSet<_>>()
            } else {
                // The view isn't the current version, queue up all documents.
                document_ids
            };

            if !missing_entries.is_empty() {
                // Add all missing entries to the invalidated list. The view
                // mapping job will update them on the next pass.
                let mut transaction =
                    roots.transaction(&[invalidated_entries_tree, view_versions_tree])?;
                let view_versions = transaction.tree::<Unversioned>(1).unwrap();
                view_versions.set(
                    view_name.to_string().as_bytes().to_vec(),
                    view_version.as_big_endian_bytes().unwrap().to_vec(),
                )?;
                let invalidated_entries = transaction.tree::<Unversioned>(0).unwrap();
                let mut missing_entries = missing_entries
                    .into_iter()
                    .map(|id| ArcBytes::from(id.to_be_bytes()))
                    .collect::<Vec<_>>();
                missing_entries.sort();
                invalidated_entries.modify(missing_entries, Operation::Set(ArcBytes::default()))?;
                transaction.commit()?;

                return Ok(true);
            }

            Ok(false)
        })
        .await??;

        if needs_update {
            self.database
                .data
                .storage
                .tasks()
                .jobs
                .lookup_or_enqueue(Mapper {
                    database: self.database.clone(),
                    map: Map {
                        database: self.database.data.name.clone(),
                        collection: self.scan.collection.clone(),
                        view_name: self.scan.view_name.clone(),
                    },
                })
                .await;
        }

        self.database
            .data
            .storage
            .tasks()
            .mark_integrity_check_complete(
                self.database.data.name.clone(),
                self.scan.collection.clone(),
                self.scan.view_name.clone(),
            )
            .await;

        Ok(())
    }
}

fn tree_keys<K: for<'a> Key<'a> + Hash + Eq + Clone, R: nebari::tree::Root>(
    tree: &Tree<R, AnyFile>,
) -> Result<HashSet<K>, crate::Error> {
    let mut ids = Vec::new();
    tree.scan::<Infallible, _, _, _, _>(
        &(..),
        true,
        |_, _, _| true,
        |key, _| {
            ids.push(key.clone());
            KeyEvaluation::Skip
        },
        |_, _, _| unreachable!(),
    )?;

    Ok(ids
        .into_iter()
        .map(|key| K::from_big_endian_bytes(&key).map_err(view::Error::key_serialization))
        .collect::<Result<HashSet<_>, view::Error>>()?)
}

impl Keyed<Task> for IntegrityScanner {
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
// Integrity scan is simple: Have a shared structure on Database that keeps track
// of all integrity scan results. It can check for an existing value and return,
// or make you wait until the job is finished. For views, I suppose the best
// that can be done is a similar approach, but the indexer's output is the last
// transaction id it synced. When a request comes in, a check can be done if
// there are any docs outdated, if so, the client can get the current transaction id
// and ask the ViewScanning service manager to wait until that txid is scanned.
//
// The view can then scan and return the results it finds with confidence it was updated to that time.
// If new requests come in while the current batch is being caught up to,
