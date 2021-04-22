use std::{
    borrow::Cow,
    collections::HashMap,
    marker::PhantomData,
    path::Path,
    sync::{Arc, RwLock},
    u8,
};

use async_trait::async_trait;
use itertools::Itertools;
use pliantdb_core::{
    circulate::{Relay, Subscriber},
    connection::{AccessPolicy, Connection, QueryKey},
    document::Document,
    limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
    pubsub::PubSub,
    schema::{
        self, view, CollectionName, Key, Map, MappedDocument, MappedValue, Schema, Schematic,
        ViewName,
    },
    transaction::{self, ChangedDocument, Command, Operation, OperationResult, Transaction},
};
use pliantdb_jobs::manager::Manager;
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, TransactionalTree},
    IVec, Transactional, Tree,
};

use self::kv::ExpirationUpdate;
use crate::{
    config::Configuration,
    error::{Error, ResultExt as _},
    open_trees::OpenTrees,
    tasks::TaskManager,
    views::{view_entries_tree_name, view_invalidated_docs_tree_name, ViewEntry},
};

pub mod kv;

/// A local, file-based database.
#[derive(Debug)]
pub struct Storage<DB> {
    pub(crate) data: Arc<Data<DB>>,
}

#[derive(Debug)]
pub struct Data<DB> {
    pub(crate) schema: Arc<Schematic>,
    pub(crate) sled: sled::Db,
    pub(crate) tasks: TaskManager,
    relay: Relay,
    kv_expirer: RwLock<Option<flume::Sender<kv::ExpirationUpdate>>>,
    _schema: PhantomData<DB>,
}

impl<DB> Clone for Storage<DB> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl<DB> Storage<DB>
where
    DB: Schema,
{
    /// Opens a local file as a pliantdb.
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
        let schema = Arc::new(DB::schematic()?);

        let storage = tokio::task::spawn_blocking(move || {
            sled::open(owned_path)
                .map(|sled| Self {
                    data: Arc::new(Data {
                        sled,
                        schema,
                        tasks,
                        kv_expirer: RwLock::default(),
                        relay: Relay::default(),
                        _schema: PhantomData::default(),
                    }),
                })
                .map_err(Error::from)
        })
        .await
        .map_err(|err| pliantdb_core::Error::Storage(err.to_string()))??;

        if configuration.views.check_integrity_on_open {
            for view in storage.data.schema.views() {
                storage
                    .data
                    .tasks
                    .spawn_integrity_check(view, &storage)
                    .await?;
            }
        }

        storage
            .data
            .tasks
            .spawn_key_value_expiration_loader(&storage)
            .await;

        Ok(storage)
    }

    /// Returns the [`Schematic`] for `DB`.
    #[must_use]
    pub fn schematic(&self) -> &'_ Schematic {
        &self.data.schema
    }

    fn update_key_expiration(&self, update: ExpirationUpdate) {
        {
            let sender = self.data.kv_expirer.read().unwrap();
            if let Some(sender) = sender.as_ref() {
                let _ = sender.send(update);
                return;
            }
        }

        // If we fall through, we need to initialize the expirer task
        let mut sender = self.data.kv_expirer.write().unwrap();
        if sender.is_none() {
            let (kv_sender, kv_expirer_receiver) = flume::unbounded();
            let thread_sled = self.data.sled.clone();
            tokio::task::spawn_blocking(move || {
                kv::expiration_thread(kv_expirer_receiver, thread_sled)
            });
            *sender = Some(kv_sender);
        }

        let _ = sender.as_ref().unwrap().send(update);
    }
}

#[async_trait]
impl<'a, DB> Connection for Storage<DB>
where
    DB: Schema,
{
    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, pliantdb_core::Error> {
        let sled = self.data.sled.clone();
        let schema = self.data.schema.clone();
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
                            let view_name = view
                                .view_name()
                                .map_err_to_core()
                                .map_err(ConflictableTransactionError::Abort)?;
                            for changed_document in &changed_documents {
                                let invalidated_docs = &trees[open_trees.trees_index_by_name
                                    [&view_invalidated_docs_tree_name(&collection, &view_name)]];
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
        .map_err(|err| pliantdb_core::Error::Storage(err.to_string()))?
    }

    async fn get<C: schema::Collection>(
        &self,
        id: u64,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error> {
        self.get_from_collection_id(id, &C::collection_name()?)
            .await
    }

    async fn get_multiple<C: schema::Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error> {
        self.get_multiple_from_collection_id(ids, &C::collection_name()?)
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
                    .data
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
    ) -> Result<Vec<Map<V::Key, V::Value>>, pliantdb_core::Error>
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
            .data
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");

        let result = self
            .reduce_in_view(
                &view.view_name()?,
                key.map(|key| key.serialized()).transpose()?,
                access_policy,
            )
            .await?;
        let value = serde_cbor::from_slice(&result)
            .map_err(Error::Serialization)
            .map_err_to_core()?;

        Ok(value)
    }

    async fn reduce_grouped<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        let view = self
            .data
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");

        let results = self
            .grouped_reduce_in_view(
                &view.view_name()?,
                key.map(|key| key.serialized()).transpose()?,
                access_policy,
            )
            .await?;
        results
            .into_iter()
            .map(|map| {
                Ok(MappedValue {
                    key: V::Key::from_big_endian_bytes(&map.key)
                        .map_err(view::Error::KeySerialization)
                        .map_err_to_core()?,
                    value: serde_cbor::from_slice(&map.value)?,
                })
            })
            .collect::<Result<Vec<_>, pliantdb_core::Error>>()
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error> {
        tokio::task::block_in_place(|| {
            let tree = self
                .data
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

#[async_trait]
impl<DB> PubSub for Storage<DB>
where
    DB: Schema,
{
    type Subscriber = Subscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, pliantdb_core::Error> {
        Ok(self.data.relay.create_subscriber().await)
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), pliantdb_core::Error> {
        self.data.relay.publish(topic, payload).await?;
        Ok(())
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

pub fn document_tree_name(collection: &CollectionName) -> String {
    format!("collection::{}", collection)
}

#[doc(hidden)]
/// These methods are internal methods used inside pliantdb-server. Changes to
/// these methods will not affect semver the same way changes to other APIs
/// will.
#[async_trait]
pub trait Internal {
    /// Iterate over each view entry matching `key`.
    async fn for_each_in_view<
        F: FnMut(IVec, ViewEntry) -> Result<(), pliantdb_core::Error> + Send + Sync,
    >(
        &self,
        view: &dyn view::Serialized,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        mut callback: F,
    ) -> Result<(), pliantdb_core::Error>;

    /// Iterate over each view entry matching `key`.
    async fn for_each_view_entry<
        V: schema::View,
        F: FnMut(IVec, ViewEntry) -> Result<(), pliantdb_core::Error> + Send + Sync,
    >(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
        callback: F,
    ) -> Result<(), pliantdb_core::Error>;

    /// Retrieves document `id` from the specified `collection`.
    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error>;

    /// Retrieves document `id` from the specified `collection`.
    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error>;

    /// Reduce view `view_name`.
    async fn reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error>;

    /// Reduce view `view_name`, grouping by unique keys.
    async fn grouped_reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, pliantdb_core::Error>;
}

#[async_trait]
impl<DB> Internal for Storage<DB>
where
    DB: Schema,
{
    async fn for_each_in_view<
        F: FnMut(IVec, ViewEntry) -> Result<(), pliantdb_core::Error> + Send + Sync,
    >(
        &self,
        view: &dyn view::Serialized,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        mut callback: F,
    ) -> Result<(), pliantdb_core::Error> {
        if matches!(access_policy, AccessPolicy::UpdateBefore) {
            self.data
                .tasks
                .update_view_if_needed(view, self)
                .await
                .map_err_to_core()?;
        }

        let view_entries = self
            .data
            .sled
            .open_tree(view_entries_tree_name(
                &view.collection()?,
                &view.view_name()?,
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
            let view_name = view.view_name()?;
            tokio::task::spawn(async move {
                let view = storage
                    .data
                    .schema
                    .view_by_name(&view_name)
                    .expect("query made with view that isn't registered with this database");
                storage
                    .data
                    .tasks
                    .update_view_if_needed(view, &storage)
                    .await
            });
        }

        Ok(())
    }

    async fn for_each_view_entry<
        V: schema::View,
        F: FnMut(IVec, ViewEntry) -> Result<(), pliantdb_core::Error> + Send + Sync,
    >(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
        callback: F,
    ) -> Result<(), pliantdb_core::Error> {
        let view = self
            .data
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

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error> {
        tokio::task::block_in_place(|| {
            let tree = self
                .data
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

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error> {
        tokio::task::block_in_place(|| {
            let tree = self
                .data
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

    #[allow(clippy::missing_panics_doc)] // the only unwrap is impossible to fail
    async fn reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view_name)
            .ok_or(pliantdb_core::Error::CollectionNotFound)?;
        let mut mappings = self
            .grouped_reduce_in_view(view_name, key, access_policy)
            .await?;

        let result = if mappings.len() == 1 {
            mappings.pop().unwrap().value // reason for missing_panics_doc
        } else {
            view.reduce(
                &mappings
                    .iter()
                    .map(|map| (map.key.as_ref(), map.value.as_ref()))
                    .collect::<Vec<_>>(),
                true,
            )
            .map_err(Error::View)
            .map_err_to_core()?
        };

        Ok(result)
    }

    #[allow(clippy::missing_panics_doc)] // the only unwrap is impossible to fail
    async fn grouped_reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, pliantdb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view_name)
            .ok_or(pliantdb_core::Error::CollectionNotFound)?;
        let mut mappings = Vec::new();
        self.for_each_in_view(view, key, access_policy, |key, entry| {
            mappings.push(MappedValue {
                key: key.to_vec(),
                value: entry.reduced_value,
            });
            Ok(())
        })
        .await?;

        Ok(mappings)
    }
}
