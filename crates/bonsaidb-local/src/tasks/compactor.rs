use std::borrow::Cow;

use async_trait::async_trait;
use bonsaidb_core::schema::{CollectionName, ViewName};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use nebari::tree::{Root, Unversioned, Versioned};

use crate::{
    database::{document_tree_name, keyvalue::KEY_TREE},
    tasks::{Job, Keyed, Task},
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name, view_versions_tree_name,
    },
    Database, Error,
};

#[derive(Debug)]
pub struct Compactor {
    pub database: Database,
    pub compaction: Compaction,
}

impl Compactor {
    pub fn collection(database: Database, collection: CollectionName) -> Self {
        Self {
            compaction: Compaction {
                database_name: database.name().to_string(),
                target: Target::Collection(collection),
            },
            database,
        }
    }
    pub fn database(database: Database) -> Self {
        Self {
            compaction: Compaction {
                database_name: database.name().to_string(),
                target: Target::Database,
            },
            database,
        }
    }
    pub fn keyvalue(database: Database) -> Self {
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
    async fn compact(self, database: &Database) -> Result<(), Error> {
        match self {
            Target::Collection(collection) => {
                let database = database.clone();
                compact_collection(database.clone(), &collection).await
            }
            Target::KeyValue => {
                let database = database.clone();
                tokio::task::spawn_blocking(move || {
                    compact_tree::<Unversioned, _>(&database, KEY_TREE)
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
impl Job for Compactor {
    type Output = ();

    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn execute(&mut self) -> Result<Self::Output, Error> {
        self.compaction.target.clone().compact(&self.database).await
    }
}

impl Keyed<Task> for Compactor {
    fn key(&self) -> Task {
        Task::Compaction(self.compaction.clone())
    }
}
async fn compact_collection(database: Database, collection: &CollectionName) -> Result<(), Error> {
    // Compact the main database file
    let mut handles = FuturesUnordered::new();
    let task_db = database.clone();
    let document_tree_name = document_tree_name(collection);
    handles.push(tokio::task::spawn_blocking(move || {
        compact_tree::<Versioned, _>(&task_db, document_tree_name)
    }));

    // Compact the views
    if let Some(views) = database.data.schema.views_in_collection(collection) {
        for view in views {
            let task_db = database.clone();
            let view_name = view.view_name();
            handles.push(tokio::task::spawn_blocking(move || {
                compact_view(&task_db, &view_name)
            }));
        }
    }

    let task_db = database.clone();
    let view_versions_tree_name = view_versions_tree_name(collection);
    handles.push(tokio::task::spawn_blocking(move || {
        compact_tree::<Unversioned, _>(&task_db, view_versions_tree_name)
    }));

    while let Some(result) = handles.next().await {
        result??;
    }

    Ok(())
}

fn compact_view(database: &Database, name: &ViewName) -> Result<(), Error> {
    compact_tree::<Unversioned, _>(database, view_entries_tree_name(name))?;
    compact_tree::<Unversioned, _>(database, view_document_map_tree_name(name))?;
    compact_tree::<Unversioned, _>(database, view_invalidated_docs_tree_name(name))?;
    compact_tree::<Unversioned, _>(database, view_omitted_docs_tree_name(name))?;

    Ok(())
}

fn compact_tree<R: Root, S: Into<Cow<'static, str>>>(
    database: &Database,
    name: S,
) -> Result<(), Error> {
    let documents = database.roots().tree(R::tree(name))?;
    documents.compact()?;
    Ok(())
}
