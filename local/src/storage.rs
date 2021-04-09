use std::{borrow::Cow, collections::HashMap, marker::PhantomData, path::Path, sync::Arc, u8};

use async_trait::async_trait;
use itertools::Itertools;
use pliantdb_core::{
    connection::{AccessPolicy, Connection, QueryKey},
    document::Document,
    limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
    schema::{self, collection, map::MappedDocument, view, Key, Map, Schema, Schematic},
    transaction::{self, ChangedDocument, Command, Operation, OperationResult, Transaction},
};
use pliantdb_jobs::manager::Manager;
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, TransactionalTree},
    IVec, Transactional, Tree,
};

use crate::{
    config::Configuration,
    error::{Error, ResultExt as _},
    open_trees::OpenTrees,
    tasks::TaskManager,
    views::{view_entries_tree_name, view_invalidated_docs_tree_name, ViewEntry},
};

/// A local, file-based database.
#[derive(Debug)]
pub struct Storage<DB> {
    /// The [`Schematic`] of `DB`.
    pub schema: Arc<Schematic>,
    pub(crate) sled: sled::Db,
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
    DB: Schema,
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
            sled::open(owned_path)
                .map(|sled| Self {
                    sled,
                    schema: Arc::new(DB::schematic()),
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

    /// Iterate over each view entry matching `key`.
    pub async fn for_each_in_view<
        F: FnMut(IVec, ViewEntry) -> Result<(), pliantdb_core::Error> + Send + Sync,
    >(
        &self,
        view: &dyn view::Serialized,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        mut callback: F,
    ) -> Result<(), pliantdb_core::Error> {
        if matches!(access_policy, AccessPolicy::UpdateBefore) {
            self.tasks
                .update_view_if_needed(view, self)
                .await
                .map_err_to_core()?;
        }

        let view_entries = self
            .sled
            .open_tree(view_entries_tree_name(
                &view.collection(),
                view.name().as_ref(),
            ))
            .map_err(Error::Sled)
            .map_err_to_core()?;

        {
            let iterator = create_view_iterator(&view_entries, key).map_err_to_core()?;
            let entries = iterator
                .collect::<Result<Vec<_>, sled::Error>>()
                .map_err(Error::Sled)
                .map_err_to_core()?;

            for (key, entry) in entries {
                let entry = bincode::deserialize::<ViewEntry>(&entry)
                    .map_err(Error::InternalSerialization)
                    .map_err_to_core()?;
                callback(key, entry)?;
            }
        }

        if matches!(access_policy, AccessPolicy::UpdateAfter) {
            let storage = self.clone();
            let view_name = view.name();
            tokio::task::spawn(async move {
                let view = storage
                    .schema
                    .view_by_name(&view_name)
                    .expect("query made with view that isn't registered with this database");
                storage.tasks.update_view_if_needed(view, &storage).await
            });
        }

        Ok(())
    }

    /// Iterate over each view entry matching `key`.
    pub async fn for_each_view_entry<
        V: schema::View,
        F: FnMut(IVec, ViewEntry) -> Result<(), pliantdb_core::Error> + Send + Sync,
    >(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
        callback: F,
    ) -> Result<(), pliantdb_core::Error> {
        let view = self
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");

        self.for_each_in_view(
            view,
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
            callback,
        )
        .await
    }

    /// Retrieves document `id` from the specified `collection`.
    pub async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &collection::Id,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error> {
        tokio::task::block_in_place(|| {
            let tree = self
                .sled
                .open_tree(document_tree_name(collection))
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

    /// Retrieves document `id` from the specified `collection`.
    pub async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &collection::Id,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error> {
        tokio::task::block_in_place(|| {
            let tree = self
                .sled
                .open_tree(document_tree_name(collection))
                .map_err_to_core()?;
            let mut found_docs = Vec::new();
            for id in ids {
                if let Some(vec) = tree
                    .get(
                        id.as_big_endian_bytes()
                            .map_err(view::Error::KeySerialization)
                            .map_err_to_core()?,
                    )
                    .map_err_to_core()?
                {
                    found_docs.push(
                        bincode::deserialize::<Document<'_>>(&vec)
                            .map_err_to_core()?
                            .to_owned(),
                    );
                }
            }

            Ok(found_docs)
        })
    }

    /// Reduce view `view_name`.
    pub async fn reduce_in_view(
        &self,
        view_name: &str,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error> {
        let view = self
            .schema
            .view_by_name(view_name)
            .ok_or(pliantdb_core::Error::CollectionNotFound)?;
        let mut mappings = Vec::new();
        self.for_each_in_view(view, key, access_policy, |key, entry| {
            mappings.push((key, entry.reduced_value));
            Ok(())
        })
        .await?;

        // An unfortunate side effect of interacting with the view over a
        // typeless interface is that we're getting a serialized version back.
        // This is wasteful. We should be able to use Any to get a full
        // reference to the view here so that we can call reduce directly.
        let result = view
            .reduce(
                &mappings
                    .iter()
                    .map(|(key, value)| (key.as_ref(), value.as_ref()))
                    .collect::<Vec<_>>(),
                true,
            )
            .map_err(Error::View)
            .map_err_to_core()?;

        Ok(result)
    }
}

#[async_trait]
impl<'a, DB> Connection<'a> for Storage<DB>
where
    DB: Schema,
{
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
                if !schema.contains_collection_id(&op.collection) {
                    return Err(pliantdb_core::Error::CollectionNotFound);
                }

                match &op.command {
                    Command::Update { .. } | Command::Insert { .. } | Command::Delete { .. } => {
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

                    match &result {
                        OperationResult::DocumentUpdated { header, collection } => {
                            changed_documents.push(ChangedDocument {
                                collection: collection.clone(),
                                id: header.id,
                                deleted: false,
                            });
                        }
                        OperationResult::DocumentDeleted { id, collection } => changed_documents
                            .push(ChangedDocument {
                                collection: collection.clone(),
                                id: *id,
                                deleted: true,
                            }),
                        OperationResult::Success => {}
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
        self.get_from_collection_id(id, &C::collection_id()).await
    }

    async fn get_multiple<C: schema::Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error> {
        self.get_multiple_from_collection_id(ids, &C::collection_id())
            .await
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
    async fn query<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<view::Map<V::Key, V::Value>>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        let mut results = Vec::new();
        self.for_each_view_entry::<V, _>(key, access_policy, |key, entry| {
            let key = <V::Key as Key>::from_big_endian_bytes(&key)
                .map_err(view::Error::KeySerialization)
                .map_err(Error::from)?;
            for entry in entry.mappings {
                results.push(Map {
                    source: entry.source,
                    key: key.clone(),
                    value: serde_cbor::from_slice(&entry.value).map_err(Error::Serialization)?,
                });
            }
            Ok(())
        })
        .await?;

        Ok(results)
    }

    async fn query_with_docs<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedDocument<V::Key, V::Value>>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        let results = self.query::<V>(key, access_policy).await?;

        let mut documents = self
            .get_multiple::<V::Collection>(&results.iter().map(|m| m.source).collect::<Vec<_>>())
            .await?
            .into_iter()
            .map(|doc| (doc.header.id, doc))
            .collect::<HashMap<_, _>>();

        Ok(results
            .into_iter()
            .filter_map(|map| {
                if let Some(document) = documents.remove(&map.source) {
                    Some(MappedDocument {
                        key: map.key,
                        value: map.value,
                        document,
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn reduce<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, pliantdb_core::Error>
    where
        Self: Sized,
    {
        let view = self
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");

        let result = self
            .reduce_in_view(
                &view.name(),
                key.map(|key| key.serialized()).transpose()?,
                access_policy,
            )
            .await?;
        let value = serde_cbor::from_slice(&result)
            .map_err(Error::Serialization)
            .map_err_to_core()?;

        Ok(value)
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error> {
        tokio::task::block_in_place(|| {
            let tree = self
                .sled
                .open_tree(TRANSACTION_TREE_NAME)
                .map_err_to_core()?;
            if let Some((key, _)) = tree.last().map_err_to_core()? {
                Ok(Some(
                    u64::from_big_endian_bytes(&key)
                        .map_err(view::Error::KeySerialization)
                        .map_err_to_core()?,
                ))
            } else {
                Ok(None)
            }
        })
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
        Command::Delete { header } => {
            let document_id = header.id.as_big_endian_bytes().unwrap();
            if let Some(vec) = tree.get(&document_id)? {
                let doc = bincode::deserialize::<Document<'_>>(&vec)
                    .map_err_to_core()
                    .map_err(ConflictableTransactionError::Abort)?;
                if &doc.header == header {
                    tree.remove(document_id.as_ref())?;
                    Ok(OperationResult::DocumentDeleted {
                        collection: operation.collection.clone(),
                        id: header.id,
                    })
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
    format!("collection::{}", collection)
}
