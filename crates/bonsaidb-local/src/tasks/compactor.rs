use std::borrow::Cow;

use bonsaidb_core::{
    connection::Connection,
    schema::{CollectionName, ViewName},
};
use nebari::tree::{ByIdIndexer, Root, Unversioned};

use crate::{
    database::{document_tree_name, keyvalue::KEY_TREE, DatabaseNonBlocking, DocumentsTree},
    tasks::{Job, Keyed, Task},
    views::{view_entries_tree_name, view_versions_tree_name, ViewEntries, ViewIndexer},
    Database, Error,
};

#[derive(Debug)]
pub struct Compactor {
    pub database: Database,
    pub compaction: Compaction,
}

impl Compactor {
    pub fn target(database: Database, target: Target) -> Self {
        Self {
            compaction: Compaction {
                database_name: database.name().to_string(),
                target,
            },
            database,
        }
    }

    pub fn collection(database: Database, collection: CollectionName) -> Self {
        Self::target(database, Target::Collection(collection))
    }

    pub fn database(database: Database) -> Self {
        Self::target(database, Target::Database)
    }

    pub fn keyvalue(database: Database) -> Self {
        Self::target(database, Target::KeyValue)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Compaction {
    database_name: String,
    target: Target,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Target {
    DocumentsTree(String),
    ViewEntriesTree { view: ViewName, tree_name: String },
    UnversionedTree(String),
    Collection(CollectionName),
    KeyValue,
    Database,
}

impl Target {
    fn compact(self, database: &Database) -> Result<(), Error> {
        match self {
            Target::UnversionedTree(name) => compact_tree::<Unversioned, _>(database, name),
            Target::ViewEntriesTree { tree_name, view } => {
                compact_tree_with_reducer::<ViewEntries, _>(
                    database,
                    tree_name,
                    ByIdIndexer(ViewIndexer::new(
                        database.schematic().view_by_name(&view)?.clone(),
                    )),
                )
            }
            Target::DocumentsTree(name) => compact_tree::<DocumentsTree, _>(database, name),
            Target::Collection(collection) => {
                let mut trees = Vec::new();
                gather_collection_trees(database, &collection, &mut trees);
                compact_trees(database, trees)
            }
            Target::KeyValue => compact_tree::<Unversioned, _>(database, KEY_TREE),
            Target::Database => {
                let mut trees = Vec::new();
                for collection in database.schematic().collections() {
                    gather_collection_trees(database, &collection, &mut trees);
                }
                trees.push(Target::KeyValue);
                compact_trees(database, trees)
            }
        }
    }
}

impl Job for Compactor {
    type Output = ();

    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
    fn execute(&mut self) -> Result<Self::Output, Error> {
        self.compaction.target.clone().compact(&self.database)
    }
}

impl Keyed<Task> for Compactor {
    fn key(&self) -> Task {
        Task::Compaction(self.compaction.clone())
    }
}

fn gather_collection_trees(
    database: &Database,
    collection: &CollectionName,
    trees: &mut Vec<Target>,
) {
    trees.push(Target::DocumentsTree(document_tree_name(collection)));
    trees.push(Target::UnversionedTree(view_versions_tree_name(collection)));

    if let Some(views) = database.data.schema.views_in_collection(collection) {
        for view in views {
            let name = view.view_name();
            trees.push(Target::ViewEntriesTree {
                view: view.view_name(),
                tree_name: view_entries_tree_name(&name),
            });
        }
    }
}

fn compact_trees(database: &Database, targets: Vec<Target>) -> Result<(), Error> {
    // Enqueue all the jobs
    let handles = targets
        .into_iter()
        .map(|target| {
            database
                .storage()
                .instance
                .tasks()
                .spawn_compact_target(database.clone(), target)
        })
        .collect::<Vec<_>>();
    // Wait for them to finish.
    for handle in handles {
        handle.receive()??;
    }
    Ok(())
}

fn compact_tree<R: Root, S: Into<Cow<'static, str>>>(
    database: &Database,
    name: S,
) -> Result<(), Error>
where
    R::Reducer: Default,
{
    let documents = database.roots().tree(R::tree(name))?;
    todo!();
    // documents.compact()?;
    Ok(())
}

fn compact_tree_with_reducer<R: Root, S: Into<Cow<'static, str>>>(
    database: &Database,
    name: S,
    reducer: R::Reducer,
) -> Result<(), Error> {
    let documents = database.roots().tree(R::tree_with_reducer(name, reducer))?;
    todo!();
    // documents.compact()?;
    Ok(())
}
