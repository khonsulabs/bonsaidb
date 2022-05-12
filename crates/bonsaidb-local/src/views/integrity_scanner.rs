use std::{borrow::Cow, hash::Hash, sync::Arc};

use bonsaidb_core::schema::{CollectionName, ViewName};
use nebari::{io::any::AnyFile, tree::Unversioned, Roots};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::{
    mapper::{Map, Mapper},
    view_versions_tree_name,
};
use crate::{
    database::Database,
    tasks::{handle::Handle, Job, Keyed, Task},
    views::{view_document_map_tree_name, view_entries_tree_name},
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

pub type OptionalViewMapHandle = Option<Arc<Mutex<Option<Handle<u64, Error>>>>>;

impl Job for IntegrityScanner {
    type Output = OptionalViewMapHandle;
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
    #[allow(clippy::too_many_lines)]
    fn execute(&mut self) -> Result<Self::Output, Self::Error> {
        let view_versions_tree = self.database.collection_tree::<Unversioned, _>(
            &self.scan.collection,
            view_versions_tree_name(&self.scan.collection),
        )?;
        let view_versions = self.database.roots().tree(view_versions_tree.clone())?;

        let view_name = self.scan.view_name.clone();
        let view_version = self.scan.view_version;
        let roots = self.database.roots().clone();
        let version = view_versions
            .get(view_name.to_string().as_bytes())?
            .and_then(|version| ViewVersion::from_bytes(&version).ok())
            .unwrap_or_default();

        // Remove any old files that are no longer used.
        version.cleanup(&roots, &view_name)?;

        let task = if version.is_current(view_version) {
            None
        } else {
            // When a version is updated, we can make no guarantees about
            // existing keys. The best we can do is delete the existing files so
            // that the view starts fresh.
            roots.delete_tree(view_entries_tree_name(&self.scan.view_name))?;
            roots.delete_tree(view_document_map_tree_name(&self.scan.view_name))?;

            let transaction = roots.transaction(&[view_versions_tree])?;
            {
                let mut view_versions = transaction.tree::<Unversioned>(0).unwrap();
                view_versions.set(
                    view_name.to_string().as_bytes().to_vec(),
                    ViewVersion::current_for(view_version).to_vec()?,
                )?;
            }
            transaction.commit()?;

            Some(Arc::new(Mutex::new(Some(
                self.database
                    .storage
                    .instance
                    .tasks()
                    .jobs
                    .lookup_or_enqueue(Mapper {
                        database: self.database.clone(),
                        map: Map {
                            database: self.database.data.name.clone(),
                            collection: self.scan.collection.clone(),
                            view_name: self.scan.view_name.clone(),
                        },
                    }),
            ))))
        };

        self.database
            .storage
            .instance
            .tasks()
            .mark_integrity_check_complete(
                self.database.data.name.clone(),
                self.scan.collection.clone(),
                self.scan.view_name.clone(),
            );

        Ok(task)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ViewVersion {
    internal_version: u8,
    schema_version: u64,
}

impl ViewVersion {
    const CURRENT_VERSION: u8 = 4;
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::Error> {
        match pot::from_slice(bytes) {
            Ok(version) => Ok(version),
            Err(err) if matches!(err, pot::Error::NotAPot) && bytes.len() == 8 => {
                let mut be_bytes = [0_u8; 8];
                be_bytes.copy_from_slice(bytes);
                let schema_version = u64::from_be_bytes(be_bytes);
                Ok(Self {
                    internal_version: 0,
                    schema_version,
                })
            }
            Err(err) => Err(crate::Error::from(err)),
        }
    }

    pub fn to_vec(&self) -> Result<Vec<u8>, crate::Error> {
        pot::to_vec(self).map_err(crate::Error::from)
    }

    pub fn current_for(schema_version: u64) -> Self {
        Self {
            internal_version: Self::CURRENT_VERSION,
            schema_version,
        }
    }

    pub fn is_current(&self, schema_version: u64) -> bool {
        self.internal_version == Self::CURRENT_VERSION && self.schema_version == schema_version
    }

    pub fn cleanup(&self, roots: &Roots<AnyFile>, view: &ViewName) -> Result<(), crate::Error> {
        if self.internal_version < 2 {
            // omitted entries was removed
            roots.delete_tree(format!("view.{:#}.omitted", view))?;
        }
        if self.internal_version < 4 {
            // omitted entries was removed
            roots.delete_tree(format!("view.{:#}.invalidated", view))?;
        }
        Ok(())
    }
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
