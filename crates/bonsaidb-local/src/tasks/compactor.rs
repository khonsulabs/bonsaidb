use async_trait::async_trait;
use bonsaidb_core::schema::{CollectionName, Schema, ViewName};
use nebari::tree::{Root, Unversioned, Versioned};

use crate::{
    database::document_tree_name,
    jobs::{Job, Keyed},
    tasks::Task,
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name, view_versions_tree_name,
    },
    Database, Error,
};

#[derive(Debug)]
pub struct Compactor<DB: Schema> {
    pub database: Database<DB>,
    pub compaction: Compaction,
}

impl<DB: Schema> Compactor<DB> {
    pub fn new(database: Database<DB>, collection: CollectionName) -> Self {
        Self {
            compaction: Compaction {
                database_name: database.name().to_string(),
                collection,
            },
            database,
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Compaction {
    pub database_name: String,
    pub collection: CollectionName,
}

#[async_trait]
impl<DB: Schema> Job for Compactor<DB> {
    type Output = ();

    type Error = Error;

    async fn execute(&mut self) -> Result<Self::Output, Error> {
        let database = self.database.clone();
        let collection = self.compaction.collection.clone();
        tokio::task::spawn_blocking::<_, Result<(), Error>>(move || {
            // Compact the main database file
            compact_tree::<DB, Versioned>(&database, document_tree_name(&collection))?;

            // Compact the views
            if let Some(views) = database.data.schema.views_in_collection(&collection) {
                for view in views {
                    compact_view(&database, &view.view_name()?)?;
                }
            }
            compact_tree::<DB, Unversioned>(&database, view_versions_tree_name(&collection))?;

            Ok(())
        })
        .await??;
        Ok(())
    }
}

impl<DB: Schema> Keyed<Task> for Compactor<DB> {
    fn key(&self) -> Task {
        Task::Compaction(self.compaction.clone())
    }
}

fn compact_view<DB: Schema>(database: &Database<DB>, name: &ViewName) -> Result<(), Error> {
    compact_tree::<DB, Unversioned>(database, view_entries_tree_name(name))?;
    compact_tree::<DB, Unversioned>(database, view_document_map_tree_name(name))?;
    compact_tree::<DB, Unversioned>(database, view_invalidated_docs_tree_name(name))?;
    compact_tree::<DB, Unversioned>(database, view_omitted_docs_tree_name(name))?;

    Ok(())
}

fn compact_tree<DB: Schema, R: Root>(database: &Database<DB>, name: String) -> Result<(), Error> {
    let documents = database.roots().tree(R::tree(name))?;
    documents.compact()?;
    Ok(())
}
