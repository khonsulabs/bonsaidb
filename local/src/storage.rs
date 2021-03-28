use std::{borrow::Cow, collections::HashMap, marker::PhantomData, path::Path, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
use pliantdb_core::{
    connection::{Collection, Connection, QueryKey, View},
    document::{Document, Header},
    schema::{self, collection, map, view, Database, Key, Schema},
    transaction::{self, ChangedDocument, Command, Operation, OperationResult, Transaction},
};
use pliantdb_jobs::manager::Manager;
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, TransactionalTree},
    IVec, Transactional, Tree,
};

use crate::{
    error::ResultExt as _,
    open_trees::OpenTrees,
    tasks::TaskManager,
    views::{view_entries_tree_name, view_invalidated_docs_tree_name, ViewEntry},
    Configuration, Error,
};

/// The maximum number of results allowed to be returned from `list_executed_transactions`.
pub const LIST_TRANSACTIONS_MAX_RESULTS: usize = 1000;
/// If no `result_limit` is specified, this value is the limit used by default.
pub const LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT: usize = 100;

/// A local, file-based database.
#[derive(Debug)]
pub struct Storage<DB> {
    pub(crate) sled: sled::Db,
    pub(crate) schema: Arc<Schema>,
    pub(crate) tasks: TaskManager,
    _schema: PhantomData<DB>,
}

impl<DB> Clone for Storage<DB> {
    fn clone(&self) -> Self {
        Self {
            sled: self.sled.clone(),
            schema: self.schema.clone(),
            tasks: self.tasks.clone(),
            _schema: PhantomData::default(),
        }
    }
}

impl<DB> Storage<DB>
where
    DB: Database,
{
    /// Opens a local file as a pliantdb.
    ///
    /// # Panics
    ///
    /// Panics if `tokio::task::spawn_blocking` fails
    pub async fn open_local<P: AsRef<Path> + Send>(
        path: P,
        configuration: &Configuration,
    ) -> Result<Self, Error> {
        let owned_path = path.as_ref().to_owned();

        let manager = Manager::default();
        for _ in 0..configuration.workers.worker_count {
            manager.spawn_worker();
        }
        let tasks = TaskManager::new(manager);

        let storage = tokio::task::spawn_blocking(move || {
            let mut collections = Schema::default();
            DB::define_collections(&mut collections);

            sled::open(owned_path)
                .map(|sled| Self {
                    sled,
                    schema: Arc::new(collections),
                    tasks,
                    _schema: PhantomData::default(),
                })
                .map_err(Error::from)
        })
        .await
        .unwrap()?;

        if configuration.views.check_integrity_on_open {
            for view in storage.schema.views() {
                storage.tasks.spawn_integrity_check(view, &storage).await?;
            }
        }

        Ok(storage)
    }

    /// Fetches the last transaction id that has been committed, if any.
    pub async fn last_transaction_id(&self) -> Result<Option<u64>, Error> {
        tokio::task::block_in_place(|| {
            let tree = self.sled.open_tree(TRANSACTION_TREE_NAME)?;
            if let Some((key, _)) = tree.last()? {
                Ok(Some(
                    u64::from_big_endian_bytes(&key).map_err(view::Error::KeySerialization)?,
                ))
            } else {
                Ok(None)
            }
        })
    }
}

#[async_trait]
impl<'a, DB> Connection<'a> for Storage<DB>
where
    DB: Database,
{
    fn collection<C: schema::Collection + 'static>(
        &'a self,
    ) -> Result<Collection<'a, Self, C>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        if self.schema.contains::<C>() {
            Ok(Collection::new(self))
        } else {
            Err(pliantdb_core::Error::CollectionNotFound)
        }
    }

    async fn insert<C: schema::Collection>(
        &self,
        contents: Vec<u8>,
    ) -> Result<Header, pliantdb_core::Error> {
        let mut tx = Transaction::default();
        tx.push(Operation {
            collection: C::id(),
            command: Command::Insert {
                contents: Cow::from(contents),
            },
        });
        let results = self.apply_transaction(tx).await?;
        if let OperationResult::DocumentUpdated { header, .. } = &results[0] {
            Ok(header.clone())
        } else {
            unreachable!(
                "apply_transaction on a single insert should yield a single DocumentUpdated entry"
            )
        }
    }

    async fn update(&self, doc: &mut Document<'_>) -> Result<(), pliantdb_core::Error> {
        let mut tx = Transaction::default();
        tx.push(Operation {
            collection: doc.collection.clone(),
            command: Command::Update {
                header: Cow::Owned(doc.header.as_ref().clone()),
                contents: Cow::Owned(doc.contents.to_vec()),
            },
        });
        let results = self.apply_transaction(tx).await?;
        if let OperationResult::DocumentUpdated { header, .. } = &results[0] {
            doc.header = Cow::Owned(header.clone());
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, pliantdb_core::Error> {
        let sled = self.sled.clone();
        let schema = self.schema.clone();
        tokio::task::spawn_blocking(move || {
            let mut open_trees = OpenTrees::default();
            open_trees
                .open_tree(&sled, TRANSACTION_TREE_NAME)
                .map_err_to_core()?;
            for op in &transaction.operations {
                match &op.command {
                    Command::Update { .. } | Command::Insert { .. } => {
                        open_trees
                            .open_trees_for_document_change(&sled, &op.collection, &schema)
                            .map_err_to_core()?;
                    }
                }
            }

            match open_trees.trees.transaction(|trees| {
                let mut results = Vec::new();
                let mut changed_documents = Vec::new();
                for op in &transaction.operations {
                    let result = execute_operation(op, trees, &open_trees.trees_index_by_name)?;

                    if let OperationResult::DocumentUpdated { header, collection } = &result {
                        changed_documents.push(ChangedDocument {
                            collection: collection.clone(),
                            id: header.id,
                        });
                    }

                    results.push(result);
                }

                // Insert invalidations for each record changed
                for (collection, changed_documents) in &changed_documents
                    .iter()
                    .group_by(|doc| doc.collection.clone())
                {
                    if let Some(views) = schema.views_in_collection(&collection) {
                        let changed_documents = changed_documents.collect::<Vec<_>>();
                        for view in views {
                            let view_name = view.name();
                            for changed_document in &changed_documents {
                                let invalidated_docs = &trees[open_trees.trees_index_by_name
                                    [&view_invalidated_docs_tree_name(
                                        &collection,
                                        view_name.as_ref(),
                                    )]];
                                invalidated_docs.insert(
                                    changed_document.id.as_big_endian_bytes().unwrap().as_ref(),
                                    IVec::default(),
                                )?;
                            }
                        }
                    }
                }

                // Save a record of the transaction we just completed.
                let tree = &trees[open_trees.trees_index_by_name[TRANSACTION_TREE_NAME]];
                let executed = transaction::Executed {
                    id: tree.generate_id()?,
                    changed_documents: Cow::from(changed_documents),
                };
                let serialized: Vec<u8> = bincode::serialize(&executed)
                    .map_err_to_core()
                    .map_err(ConflictableTransactionError::Abort)?;
                tree.insert(&executed.id.to_be_bytes(), serialized)?;

                trees.iter().for_each(TransactionalTree::flush);
                Ok(results)
            }) {
                Ok(results) => Ok(results),
                Err(err) => match err {
                    TransactionError::Abort(err) => Err(err),
                    TransactionError::Storage(err) => {
                        Err(pliantdb_core::Error::Storage(err.to_string()))
                    }
                },
            }
        })
        .await
        .unwrap()
    }

    async fn get<C: schema::Collection>(
        &self,
        id: u64,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error> {
        tokio::task::block_in_place(|| {
            let tree = self
                .sled
                .open_tree(document_tree_name(&C::id()))
                .map_err_to_core()?;
            if let Some(vec) = tree
                .get(
                    id.as_big_endian_bytes()
                        .map_err(view::Error::KeySerialization)
                        .map_err_to_core()?,
                )
                .map_err_to_core()?
            {
                Ok(Some(
                    bincode::deserialize::<Document<'_>>(&vec)
                        .map_err_to_core()?
                        .to_owned(),
                ))
            } else {
                Ok(None)
            }
        })
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed<'static>>, pliantdb_core::Error> {
        let result_limit = result_limit
            .unwrap_or(LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT)
            .min(LIST_TRANSACTIONS_MAX_RESULTS);
        if result_limit > 0 {
            tokio::task::block_in_place(|| {
                let tree = self
                    .sled
                    .open_tree(TRANSACTION_TREE_NAME)
                    .map_err_to_core()?;
                let iter = if let Some(starting_id) = starting_id {
                    tree.range(starting_id.to_be_bytes()..=u64::MAX.to_be_bytes())
                } else {
                    tree.iter()
                };

                #[allow(clippy::cast_possible_truncation)] // this value is limited above
                let mut results = Vec::with_capacity(result_limit);
                for row in iter {
                    let (_, vec) = row.map_err_to_core()?;
                    results.push(
                        bincode::deserialize::<transaction::Executed<'_>>(&vec)
                            .map_err_to_core()?
                            .to_owned(),
                    );

                    if results.len() >= result_limit {
                        break;
                    }
                }
                Ok(results)
            })
        } else {
            // A request was made to return an empty result? This should probably be
            // an error, but technically this is a correct response.
            Ok(Vec::default())
        }
    }

    #[must_use]
    async fn query<'k, V: schema::View>(
        &self,
        query: View<'a, Self, V>,
    ) -> Result<Vec<map::Serialized>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        let View { key, .. } = query;
        let view = self
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");
        self.tasks
            .update_view_if_needed(view, self)
            .await
            .map_err_to_core()?;

        let view_entries = self
            .sled
            .open_tree(view_entries_tree_name(
                &view.collection(),
                view.name().as_ref(),
            ))
            .map_err(Error::Sled)
            .map_err_to_core()?;

        let iterator = create_view_iterator(&view_entries, key).map_err_to_core()?;
        let entries = iterator
            .collect::<Result<Vec<_>, sled::Error>>()
            .map_err(Error::Sled)
            .map_err_to_core()?;

        let mut results = Vec::new();
        for (key, entry) in entries {
            let entry = bincode::deserialize::<ViewEntry>(&entry)
                .map_err(Error::InternalSerialization)
                .map_err_to_core()?;

            for entry in entry.mappings {
                results.push(map::Serialized {
                    source: entry.source,
                    key: key.to_vec(),
                    value: entry.value,
                });
            }
        }

        Ok(results)
    }
}

type ViewIterator<'a> = Box<dyn Iterator<Item = Result<(IVec, IVec), sled::Error>> + 'a>;

fn create_view_iterator<'a, K: Key + 'a>(
    view_entries: &'a Tree,
    key: Option<QueryKey<K>>,
) -> Result<ViewIterator<'a>, view::Error> {
    if let Some(key) = key {
        match key {
            QueryKey::Range(range) => {
                let start = range
                    .start
                    .as_big_endian_bytes()
                    .map_err(view::Error::KeySerialization)?;
                let end = range
                    .end
                    .as_big_endian_bytes()
                    .map_err(view::Error::KeySerialization)?;
                Ok(Box::new(view_entries.range(start..end)))
            }
            QueryKey::Matches(key) => {
                let key = key
                    .as_big_endian_bytes()
                    .map_err(view::Error::KeySerialization)?;
                let range_end = key.clone();
                Ok(Box::new(view_entries.range(key..=range_end)))
            }
            QueryKey::Multiple(list) => {
                let list = list
                    .into_iter()
                    .map(|key| {
                        key.as_big_endian_bytes()
                            .map(|bytes| bytes.to_vec())
                            .map_err(view::Error::KeySerialization)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let iterators = list.into_iter().flat_map(move |key| {
                    let range_end = key.clone();
                    view_entries.range(key..=range_end)
                });

                Ok(Box::new(iterators))
            }
        }
    } else {
        Ok(Box::new(view_entries.iter()))
    }
}

fn execute_operation(
    operation: &Operation<'_>,
    trees: &[TransactionalTree],
    tree_index_map: &HashMap<String, usize>,
) -> Result<OperationResult, ConflictableTransactionError<pliantdb_core::Error>> {
    let tree = &trees[tree_index_map[&document_tree_name(&operation.collection)]];
    match &operation.command {
        Command::Insert { contents } => {
            let doc = Document::new(
                tree.generate_id()?,
                Cow::Borrowed(contents),
                operation.collection.clone(),
            );
            save_doc(tree, &doc)?;
            let serialized: Vec<u8> = bincode::serialize(&doc)
                .map_err_to_core()
                .map_err(ConflictableTransactionError::Abort)?;
            tree.insert(
                doc.header.id.as_big_endian_bytes().unwrap().as_ref(),
                serialized,
            )?;

            Ok(OperationResult::DocumentUpdated {
                collection: operation.collection.clone(),
                header: doc.header.as_ref().clone(),
            })
        }
        Command::Update { header, contents } => {
            if let Some(vec) = tree.get(&header.id.as_big_endian_bytes().unwrap())? {
                let doc = bincode::deserialize::<Document<'_>>(&vec)
                    .map_err_to_core()
                    .map_err(ConflictableTransactionError::Abort)?;
                if &doc.header == header {
                    if let Some(updated_doc) = doc.create_new_revision(contents.clone()) {
                        save_doc(tree, &updated_doc)?;
                        Ok(OperationResult::DocumentUpdated {
                            collection: operation.collection.clone(),
                            header: updated_doc.header.as_ref().clone(),
                        })
                    } else {
                        // If no new revision was made, it means an attempt to
                        // save a document with the same contents was made.
                        // We'll return a success but not actually give a new
                        // version
                        Ok(OperationResult::DocumentUpdated {
                            collection: operation.collection.clone(),
                            header: doc.header.as_ref().clone(),
                        })
                    }
                } else {
                    Err(ConflictableTransactionError::Abort(
                        pliantdb_core::Error::DocumentConflict(
                            operation.collection.clone(),
                            header.id,
                        ),
                    ))
                }
            } else {
                Err(ConflictableTransactionError::Abort(
                    pliantdb_core::Error::DocumentNotFound(operation.collection.clone(), header.id),
                ))
            }
        }
    }
}

fn save_doc(
    tree: &TransactionalTree,
    doc: &Document<'_>,
) -> Result<(), ConflictableTransactionError<pliantdb_core::Error>> {
    let serialized: Vec<u8> = bincode::serialize(doc)
        .map_err_to_core()
        .map_err(ConflictableTransactionError::Abort)?;
    tree.insert(
        doc.header.id.as_big_endian_bytes().unwrap().as_ref(),
        serialized,
    )?;
    Ok(())
}

const TRANSACTION_TREE_NAME: &str = "transactions";

pub fn document_tree_name(collection: &collection::Id) -> String {
    collection.0.to_string()
}
