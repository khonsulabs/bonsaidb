use std::borrow::Cow;

use async_trait::async_trait;
use bonsaidb_core::schema::{CollectionName, Schema, ViewName};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use nebari::tree::{Root, Unversioned, Versioned};

use crate::{
    database::{document_tree_name, kv::KEY_TREE},
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
    pub fn collection(database: Database<DB>, collection: CollectionName) -> Self {
        Self {
            compaction: Compaction {
                database_name: database.name().to_string(),
                target: Target::Collection(collection),
            },
            database,
        }
    }
    pub fn database(database: Database<DB>) -> Self {
        Self {
            compaction: Compaction {
                database_name: database.name().to_string(),
                target: Target::Database,
            },
            database,
        }
    }
    pub fn keyvalue(database: Database<DB>) -> Self {
        Self {
            compaction: Compaction {
                database_name: database.name().to_string(),
                target: Target::KeyValue,
            },
            database,
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Compaction {
    database_name: String,
    target: Target,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum Target {
    Collection(CollectionName),
    KeyValue,
    Database,
}

impl Target {
    async fn compact<DB: Schema>(self, database: &Database<DB>) -> Result<(), Error> {
        match self {
            Target::Collection(collection) => {
                let database = database.clone();
                tokio::task::spawn_blocking(move || compact_collection(&database, &collection))
                    .await?
            }
            Target::KeyValue => {
                let database = database.clone();
                tokio::task::spawn_blocking(move || {
                    compact_tree::<DB, Unversioned, _>(&database, KEY_TREE)
                })
                .await?
            }
            Target::Database => {
                let mut handles = FuturesUnordered::new();
                for collection in database.schematic().collections() {
                    handles.push(
                        database
                            .storage()
                            .tasks()
                            .compact_collection(database.clone(), collection)
                            .boxed(),
                    );
                }
                handles.push(
                    database
                        .storage()
                        .tasks()
                        .compact_key_value_store(database.clone())
                        .boxed(),
                );
                while let Some(result) = handles.next().await {
                    result?;
                }
                Ok(())
            }
        }
    }
}

#[async_trait]
impl<DB: Schema> Job for Compactor<DB> {
    type Output = ();

    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn execute(&mut self) -> Result<Self::Output, Error> {
        self.compaction.target.clone().compact(&self.database).await
    }
}

impl<DB: Schema> Keyed<Task> for Compactor<DB> {
    fn key(&self) -> Task {
        Task::Compaction(self.compaction.clone())
    }
}
fn compact_collection<DB: Schema>(
    database: &Database<DB>,
    collection: &CollectionName,
) -> Result<(), Error> {
    // Compact the main database file
    compact_tree::<DB, Versioned, _>(database, document_tree_name(collection))?;

    // Compact the views
    if let Some(views) = database.data.schema.views_in_collection(collection) {
        for view in views {
            compact_view(database, &view.view_name()?)?;
        }
    }
    compact_tree::<DB, Unversioned, _>(database, view_versions_tree_name(collection))?;
    Ok(())
}

fn compact_view<DB: Schema>(database: &Database<DB>, name: &ViewName) -> Result<(), Error> {
    compact_tree::<DB, Unversioned, _>(database, view_entries_tree_name(name))?;
    compact_tree::<DB, Unversioned, _>(database, view_document_map_tree_name(name))?;
    compact_tree::<DB, Unversioned, _>(database, view_invalidated_docs_tree_name(name))?;
    compact_tree::<DB, Unversioned, _>(database, view_omitted_docs_tree_name(name))?;

    Ok(())
}

fn compact_tree<DB: Schema, R: Root, S: Into<Cow<'static, str>>>(
    database: &Database<DB>,
    name: S,
) -> Result<(), Error> {
    let documents = database.roots().tree(R::tree(name))?;
    documents.compact()?;
    Ok(())
}
