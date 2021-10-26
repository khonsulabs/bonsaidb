use std::{
    any::Any, borrow::Cow, collections::HashMap, convert::Infallible, marker::PhantomData,
    path::Path, sync::Arc, u8,
};

use async_trait::async_trait;
use bonsaidb_core::{
    connection::{AccessPolicy, Connection, QueryKey, ServerConnection},
    document::{Document, Header, KeyId},
    kv::{KeyOperation, Kv, Output},
    limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
    permissions::Permissions,
    schema::{
        self,
        view::{self, map},
        CollectionName, Key, Map, MappedDocument, MappedValue, Schema, Schematic, ViewName,
    },
    transaction::{
        self, ChangedDocument, Command, Executed, Operation, OperationResult, Transaction,
    },
};
use byteorder::{BigEndian, ByteOrder};
use itertools::Itertools;
use nebari::{
    io::fs::StdFile,
    tree::{AnyTreeRoot, KeyEvaluation, Root, TreeRoot, Unversioned, Versioned},
    Buffer, ExecutingTransaction, Roots, Tree,
};
use ranges::GenericRange;

use crate::{
    config::Configuration,
    error::Error,
    open_trees::OpenTrees,
    storage::OpenDatabase,
    vault::TreeVault,
    views::{
        mapper::{self, ViewEntryCollection},
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name, ViewEntry,
    },
    Storage,
};
pub mod kv;

pub mod pubsub;

/// A local, file-based database.
#[derive(Debug)]
pub struct Database<DB> {
    pub(crate) data: Arc<Data<DB>>,
}

#[derive(Debug)]
pub struct Data<DB> {
    pub name: Arc<Cow<'static, str>>,
    context: Context,
    pub(crate) storage: Storage,
    pub(crate) schema: Arc<Schematic>,
    pub(crate) effective_permissions: Option<Permissions>,
    _schema: PhantomData<DB>,
}
impl<DB> Clone for Database<DB> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl<DB> Database<DB>
where
    DB: Schema,
{
    /// Opens a local file as a bonsaidb.
    pub(crate) async fn new<S: Into<Cow<'static, str>> + Send>(
        name: S,
        context: Context,
        storage: Storage,
    ) -> Result<Self, Error> {
        let name = name.into();
        let schema = Arc::new(DB::schematic()?);
        let db = Self {
            data: Arc::new(Data {
                name: Arc::new(name),
                context,
                storage: storage.clone(),
                schema,
                effective_permissions: None,
                _schema: PhantomData::default(),
            }),
        };

        if db.data.storage.check_view_integrity_on_database_open() {
            for view in db.data.schema.views() {
                db.data
                    .storage
                    .tasks()
                    .spawn_integrity_check(view, &db)
                    .await?;
            }
        }

        storage.tasks().spawn_key_value_expiration_loader(&db).await;

        Ok(db)
    }

    /// Returns a clone with `effective_permissions`. Replaces any previously applied permissions.
    ///
    /// # Unstable
    ///
    /// See [this issue](https://github.com/khonsulabs/bonsaidb/issues/68).
    #[doc(hidden)]
    #[must_use]
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Self {
        Self {
            data: Arc::new(Data {
                name: self.data.name.clone(),
                context: self.data.context.clone(),
                storage: self.data.storage.clone(),
                schema: self.data.schema.clone(),
                effective_permissions: Some(effective_permissions),
                _schema: PhantomData::default(),
            }),
        }
    }

    /// Returns the name of the database.
    #[must_use]
    pub fn name(&self) -> &str {
        self.data.name.as_ref()
    }

    /// Creates a `Storage` with a single-database named "default" with its data stored at `path`.
    pub async fn open_local<P: AsRef<Path> + Send>(
        path: P,
        configuration: Configuration,
    ) -> Result<Self, Error> {
        let storage = Storage::open_local(path, configuration).await?;
        storage.register_schema::<DB>().await?;

        match storage.create_database::<DB>("default").await {
            Ok(_) | Err(bonsaidb_core::Error::DatabaseNameAlreadyTaken(_)) => {}
            err => err?,
        }

        Ok(storage.database("default").await?)
    }

    /// Returns the [`Storage`] that this database belongs to.
    #[must_use]
    pub fn storage(&self) -> &'_ Storage {
        &self.data.storage
    }

    /// Returns the [`Schematic`] for `DB`.
    #[must_use]
    pub fn schematic(&self) -> &'_ Schematic {
        &self.data.schema
    }

    pub(crate) fn roots(&self) -> &'_ nebari::Roots<StdFile> {
        &self.data.context.roots
    }

    async fn for_each_in_view<
        F: FnMut(ViewEntryCollection) -> Result<(), bonsaidb_core::Error> + Send + Sync,
    >(
        &self,
        view: &dyn view::Serialized,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        mut callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
        if matches!(access_policy, AccessPolicy::UpdateBefore) {
            self.data
                .storage
                .tasks()
                .update_view_if_needed(view, self)
                .await?;
        }

        let view_entries = self
            .roots()
            .tree(self.collection_tree(
                &view.collection()?,
                view_entries_tree_name(&view.view_name()?),
            ))
            .map_err(Error::from)?;

        {
            for entry in Self::create_view_iterator(&view_entries, key)? {
                callback(entry)?;
            }
        }

        if matches!(access_policy, AccessPolicy::UpdateAfter) {
            let db = self.clone();
            let view_name = view.view_name()?;
            tokio::task::spawn(async move {
                let view = db
                    .data
                    .schema
                    .view_by_name(&view_name)
                    .expect("query made with view that isn't registered with this database");
                db.data
                    .storage
                    .tasks()
                    .update_view_if_needed(view, &db)
                    .await
            });
        }

        Ok(())
    }

    async fn for_each_view_entry<
        V: schema::View,
        F: FnMut(ViewEntryCollection) -> Result<(), bonsaidb_core::Error> + Send + Sync,
    >(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
        callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
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
    ) -> Result<Option<Document<'static>>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree =
                task_self
                    .data
                    .context
                    .roots
                    .tree(task_self.collection_tree::<Versioned, _>(
                        &collection,
                        document_tree_name(&collection),
                    ))
                    .map_err(Error::from)?;
            if let Some(vec) = tree
                .get(
                    &id.as_big_endian_bytes()
                        .map_err(view::Error::key_serialization)?,
                )
                .map_err(Error::from)?
            {
                Ok(Some(deserialize_document(&vec)?.to_owned()))
            } else {
                Ok(None)
            }
        })
        .await
        .unwrap()
    }

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
    ) -> Result<Vec<Document<'static>>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let ids = ids.to_vec();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree =
                task_self
                    .data
                    .context
                    .roots
                    .tree(task_self.collection_tree::<Versioned, _>(
                        &collection,
                        document_tree_name(&collection),
                    ))
                    .map_err(Error::from)?;
            let mut found_docs = Vec::new();
            for id in ids {
                if let Some(vec) = tree
                    .get(
                        &id.as_big_endian_bytes()
                            .map_err(view::Error::key_serialization)?,
                    )
                    .map_err(Error::from)?
                {
                    found_docs.push(deserialize_document(&vec)?.to_owned());
                }
            }

            Ok(found_docs)
        })
        .await
        .unwrap()
    }

    async fn reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view_name)
            .ok_or(bonsaidb_core::Error::CollectionNotFound)?;
        let mut mappings = self
            .grouped_reduce_in_view(view_name, key, access_policy)
            .await?;

        let result = if mappings.len() == 1 {
            mappings.pop().unwrap().value
        } else {
            view.reduce(
                &mappings
                    .iter()
                    .map(|map| (map.key.as_ref(), map.value.as_ref()))
                    .collect::<Vec<_>>(),
                true,
            )
            .map_err(Error::View)?
        };

        Ok(result)
    }

    async fn grouped_reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view_name)
            .ok_or(bonsaidb_core::Error::CollectionNotFound)?;
        let mut mappings = Vec::new();
        self.for_each_in_view(view, key, access_policy, |entry| {
            let entry = ViewEntry::from(entry);
            mappings.push(MappedValue {
                key: entry.key,
                value: entry.reduced_value,
            });
            Ok(())
        })
        .await?;

        Ok(mappings)
    }

    fn execute_operation(
        &self,
        operation: &Operation<'_>,
        transaction: &mut ExecutingTransaction<StdFile>,
        tree_index_map: &HashMap<String, usize>,
    ) -> Result<OperationResult, Error> {
        match &operation.command {
            Command::Insert { contents } => {
                self.execute_insert(operation, transaction, tree_index_map, contents.clone())
            }
            Command::Update { header, contents } => self.execute_update(
                operation,
                transaction,
                tree_index_map,
                header,
                contents.clone(),
            ),
            Command::Delete { header } => {
                self.execute_delete(operation, transaction, tree_index_map, header)
            }
        }
    }

    fn execute_update(
        &self,
        operation: &Operation<'_>,
        transaction: &mut ExecutingTransaction<StdFile>,
        tree_index_map: &HashMap<String, usize>,
        header: &Header,
        contents: Cow<'_, [u8]>,
    ) -> Result<OperationResult, crate::Error> {
        let documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let document_id = header.id.as_big_endian_bytes().unwrap();
        // TODO switch to compare_swap

        if let Some(vec) = documents.get(document_id.as_ref())? {
            let doc = deserialize_document(&vec)?;
            if doc.header.revision == header.revision {
                if let Some(updated_doc) = doc.create_new_revision(contents) {
                    documents.set(
                        updated_doc
                            .header
                            .id
                            .as_big_endian_bytes()
                            .unwrap()
                            .as_ref()
                            .to_vec(),
                        serialize_document(&updated_doc)?,
                    )?;

                    self.update_unique_views(&document_id, operation, transaction, tree_index_map)?;

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
                Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                    operation.collection.clone(),
                    header.id,
                )))
            }
        } else {
            Err(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                operation.collection.clone(),
                header.id,
            )))
        }
    }

    fn execute_insert(
        &self,
        operation: &Operation<'_>,
        transaction: &mut ExecutingTransaction<StdFile>,
        tree_index_map: &HashMap<String, usize>,
        contents: Cow<'_, [u8]>,
    ) -> Result<OperationResult, Error> {
        let documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let last_key = documents
            .last_key()?
            .map(|bytes| BigEndian::read_u64(&bytes))
            .unwrap_or_default();
        let doc = Document::new(last_key + 1, contents);
        let serialized: Vec<u8> = serialize_document(&doc)?;
        let document_id = Buffer::from(doc.header.id.as_big_endian_bytes().unwrap().to_vec());
        documents.set(document_id.clone(), serialized)?;

        self.update_unique_views(&document_id, operation, transaction, tree_index_map)?;

        Ok(OperationResult::DocumentUpdated {
            collection: operation.collection.clone(),
            header: doc.header.as_ref().clone(),
        })
    }

    fn execute_delete(
        &self,
        operation: &Operation<'_>,
        transaction: &mut ExecutingTransaction<StdFile>,
        tree_index_map: &HashMap<String, usize>,
        header: &Header,
    ) -> Result<OperationResult, Error> {
        let documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let document_id = header.id.as_big_endian_bytes().unwrap();
        if let Some(vec) = documents.remove(&document_id)? {
            let doc = deserialize_document(&vec)?;
            if doc.header.as_ref() == header {
                self.update_unique_views(
                    document_id.as_ref(),
                    operation,
                    transaction,
                    tree_index_map,
                )?;

                Ok(OperationResult::DocumentDeleted {
                    collection: operation.collection.clone(),
                    id: header.id,
                })
            } else {
                Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                    operation.collection.clone(),
                    header.id,
                )))
            }
        } else {
            Err(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                operation.collection.clone(),
                header.id,
            )))
        }
    }

    fn update_unique_views(
        &self,
        document_id: &[u8],
        operation: &Operation<'_>,
        transaction: &mut ExecutingTransaction<StdFile>,
        tree_index_map: &HashMap<String, usize>,
    ) -> Result<(), Error> {
        if let Some(unique_views) = self
            .data
            .schema
            .unique_views_in_collection(&operation.collection)
        {
            for view in unique_views {
                let name = view.view_name().map_err(bonsaidb_core::Error::from)?;
                mapper::DocumentRequest {
                    database: self,
                    document_id,
                    map_request: &mapper::Map {
                        database: self.data.name.clone(),
                        collection: operation.collection.clone(),
                        view_name: name.clone(),
                    },
                    transaction,
                    document_map_index: tree_index_map[&view_document_map_tree_name(&name)],
                    documents_index: tree_index_map[&document_tree_name(&operation.collection)],
                    omitted_entries_index: tree_index_map[&view_omitted_docs_tree_name(&name)],
                    view_entries_index: tree_index_map[&view_entries_tree_name(&name)],
                    view,
                }
                .map()?;
            }
        }

        Ok(())
    }

    fn create_view_iterator<'a, K: Key + 'a>(
        view_entries: &'a Tree<Unversioned, StdFile>,
        key: Option<QueryKey<K>>,
    ) -> Result<Vec<ViewEntryCollection>, Error> {
        let mut values = Vec::new();
        if let Some(key) = key {
            match key {
                QueryKey::Range(range) => {
                    let start = Buffer::from(
                        range
                            .start
                            .as_big_endian_bytes()
                            .map_err(view::Error::key_serialization)?
                            .to_vec(),
                    );
                    let end = Buffer::from(
                        range
                            .end
                            .as_big_endian_bytes()
                            .map_err(view::Error::key_serialization)?
                            .to_vec(),
                    );
                    view_entries.scan::<Infallible, _, _, _, _>(
                        start..end,
                        true,
                        |_, _, _| true,
                        |_, _| KeyEvaluation::ReadData,
                        |_key, _index, value| {
                            values.push(value);
                            Ok(())
                        },
                    )?;
                }
                QueryKey::Matches(key) => {
                    let key = key
                        .as_big_endian_bytes()
                        .map_err(view::Error::key_serialization)?
                        .to_vec();

                    values.extend(view_entries.get(&key)?);
                }
                QueryKey::Multiple(list) => {
                    let list = list
                        .into_iter()
                        .map(|key| {
                            key.as_big_endian_bytes()
                                .map(|bytes| bytes.to_vec())
                                .map_err(view::Error::key_serialization)
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    values.extend(
                        view_entries
                            .get_multiple(&list.iter().map(Vec::as_slice).collect::<Vec<_>>())?
                            .into_iter()
                            .map(|(_, value)| value),
                    );
                }
            }
        } else {
            view_entries.scan::<Infallible, _, _, _, _>(
                ..,
                true,
                |_, _, _| true,
                |_, _| KeyEvaluation::ReadData,
                |_, _, value| {
                    values.push(value);
                    Ok(())
                },
            )?;
        }

        values
            .into_iter()
            .map(|value| bincode::deserialize(&value).map_err(Error::from))
            .collect::<Result<Vec<_>, Error>>()
    }

    pub(crate) fn collection_encryption_key(&self, collection: &CollectionName) -> Option<&KeyId> {
        self.schematic()
            .encryption_key_for_collection(collection)
            .or_else(|| self.storage().default_encryption_key())
    }

    pub(crate) fn collection_tree<R: Root, S: Into<Cow<'static, str>>>(
        &self,
        collection: &CollectionName,
        name: S,
    ) -> TreeRoot<R, StdFile> {
        let mut tree = R::tree(name);
        if let Some(key) = self.collection_encryption_key(collection) {
            tree = tree.with_vault(TreeVault {
                key: key.clone(),
                vault: self.storage().vault().clone(),
            });
        }
        tree
    }

    pub(crate) fn update_key_expiration(&self, update: kv::ExpirationUpdate) {
        self.data.context.update_key_expiration(update);
    }
}

pub(crate) fn deserialize_document<'a>(
    bytes: &'a [u8],
) -> Result<Document<'a>, bonsaidb_core::Error> {
    let document = bincode::deserialize::<Document<'_>>(bytes).map_err(Error::from)?;
    Ok(document)
}

fn serialize_document(document: &Document<'_>) -> Result<Vec<u8>, bonsaidb_core::Error> {
    bincode::serialize(document)
        .map_err(Error::from)
        .map_err(bonsaidb_core::Error::from)
}

#[async_trait]
impl<'a, DB> Connection for Database<DB>
where
    DB: Schema,
{
    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        let task_self = self.clone();
        tokio::task::spawn_blocking::<_, Result<Vec<OperationResult>, Error>>(move || {
            let mut open_trees = OpenTrees::default();
            for op in &transaction.operations {
                if !task_self.data.schema.contains_collection_id(&op.collection) {
                    return Err(Error::Core(bonsaidb_core::Error::CollectionNotFound));
                }

                match &op.command {
                    Command::Update { .. } | Command::Insert { .. } | Command::Delete { .. } => {
                        open_trees.open_trees_for_document_change(
                            &op.collection,
                            &task_self.data.schema,
                            task_self.collection_encryption_key(&op.collection),
                            task_self.storage().vault(),
                        )?;
                    }
                }
            }

            let mut roots_transaction = task_self
                .data
                .context
                .roots
                .transaction::<_, dyn AnyTreeRoot<StdFile>>(&open_trees.trees)?;

            let mut results = Vec::new();
            let mut changed_documents = Vec::new();
            for op in &transaction.operations {
                let result = task_self.execute_operation(
                    op,
                    &mut roots_transaction,
                    &open_trees.trees_index_by_name,
                )?;

                match &result {
                    OperationResult::DocumentUpdated { header, collection } => {
                        changed_documents.push(ChangedDocument {
                            collection: collection.clone(),
                            id: header.id,
                            deleted: false,
                        });
                    }
                    OperationResult::DocumentDeleted { id, collection } => {
                        changed_documents.push(ChangedDocument {
                            collection: collection.clone(),
                            id: *id,
                            deleted: true,
                        });
                    }
                    OperationResult::Success => {}
                }
                results.push(result);
            }

            // Insert invalidations for each record changed
            for (collection, changed_documents) in &changed_documents
                .iter()
                .group_by(|doc| doc.collection.clone())
            {
                if let Some(views) = task_self.data.schema.views_in_collection(&collection) {
                    let changed_documents = changed_documents.collect::<Vec<_>>();
                    for view in views {
                        if !view.unique() {
                            let view_name = view.view_name().map_err(bonsaidb_core::Error::from)?;
                            for changed_document in &changed_documents {
                                let invalidated_docs = roots_transaction
                                    .tree::<Unversioned>(
                                        open_trees.trees_index_by_name
                                            [&view_invalidated_docs_tree_name(&view_name)],
                                    )
                                    .unwrap();
                                invalidated_docs.set(
                                    changed_document.id.as_big_endian_bytes().unwrap().to_vec(),
                                    b"",
                                )?;
                            }
                        }
                    }
                }
            }

            roots_transaction
                .entry_mut()
                .set_data(bincode::serialize(&changed_documents)?)?;

            roots_transaction.commit()?;

            Ok(results)
        })
        .await
        .map_err(|err| bonsaidb_core::Error::Database(err.to_string()))?
        .map_err(bonsaidb_core::Error::from)
    }

    async fn get<C: schema::Collection>(
        &self,
        id: u64,
    ) -> Result<Option<Document<'static>>, bonsaidb_core::Error> {
        self.get_from_collection_id(id, &C::collection_name()?)
            .await
    }

    async fn get_multiple<C: schema::Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Document<'static>>, bonsaidb_core::Error> {
        self.get_multiple_from_collection_id(ids, &C::collection_name()?)
            .await
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed<'static>>, bonsaidb_core::Error> {
        let result_limit = result_limit
            .unwrap_or(LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT)
            .min(LIST_TRANSACTIONS_MAX_RESULTS);
        if result_limit > 0 {
            let task_self = self.clone();
            tokio::task::spawn_blocking::<_, Result<Vec<transaction::Executed<'static>>, Error>>(
                move || {
                    let range = if let Some(starting_id) = starting_id {
                        GenericRange::from(starting_id..)
                    } else {
                        GenericRange::from(..)
                    };

                    let mut entries = Vec::new();
                    task_self.roots().transactions().scan(range, |entry| {
                        entries.push(entry);
                        entries.len() < result_limit
                    })?;

                    entries
                        .into_iter()
                        .map(|entry| {
                            let changed_documents = bincode::deserialize(entry.data().unwrap())?;
                            Ok(transaction::Executed {
                                id: entry.id,
                                changed_documents,
                            })
                        })
                        .collect::<Result<Vec<_>, Error>>()
                },
            )
            .await
            .unwrap()
            .map_err(bonsaidb_core::Error::from)
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
    ) -> Result<Vec<Map<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let mut results = Vec::new();
        self.for_each_view_entry::<V, _>(key, access_policy, |collection| {
            let entry = ViewEntry::from(collection);
            let key = <V::Key as Key>::from_big_endian_bytes(&entry.key)
                .map_err(view::Error::key_serialization)
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
    ) -> Result<Vec<MappedDocument<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let results = Connection::query::<V>(self, key, access_policy).await?;

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
    ) -> Result<V::Value, bonsaidb_core::Error>
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
        let value = serde_cbor::from_slice(&result).map_err(Error::Serialization)?;

        Ok(value)
    }

    async fn reduce_grouped<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, bonsaidb_core::Error>
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
                        .map_err(view::Error::key_serialization)?,
                    value: serde_cbor::from_slice(&map.value)?,
                })
            })
            .collect::<Result<Vec<_>, bonsaidb_core::Error>>()
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self.roots().transactions().current_transaction_id())
    }
}

type ViewIterator<'a> =
    Box<dyn Iterator<Item = Result<(Buffer<'static>, Buffer<'static>), Error>> + 'a>;

struct ViewEntryCollectionIterator<'a> {
    iterator: ViewIterator<'a>,
}

impl<'a> Iterator for ViewEntryCollectionIterator<'a> {
    type Item = Result<ViewEntryCollection, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|item| {
            item.map_err(crate::Error::from)
                .and_then(|(_, value)| bincode::deserialize(&value).map_err(Error::from))
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Context {
    pub(crate) roots: Roots<StdFile>,
    kv_expirer: Arc<std::sync::RwLock<Option<flume::Sender<kv::ExpirationUpdate>>>>,
}

impl Context {
    pub(crate) fn new(roots: Roots<StdFile>) -> Self {
        Self {
            roots,
            kv_expirer: Arc::default(),
        }
    }

    pub(crate) fn shutdown(&self) {
        {
            let mut expirer = self.kv_expirer.write().unwrap();
            *expirer = None;
        }
    }

    pub(crate) fn update_key_expiration(&self, update: kv::ExpirationUpdate) {
        {
            let sender = self.kv_expirer.read().unwrap();
            if let Some(sender) = sender.as_ref() {
                drop(sender.send(update));
                return;
            }
        }

        // If we fall through, we need to initialize the expirer task
        let mut sender = self.kv_expirer.write().unwrap();
        if let Some(kv_sender) = sender.as_ref() {
            drop(kv_sender.send(update));
        } else {
            let (kv_sender, kv_expirer_receiver) = flume::unbounded();
            kv_sender.send(update).unwrap();
            let context = self.clone();
            tokio::task::spawn_blocking(move || {
                kv::expiration_thread(context, kv_expirer_receiver)
            });
            *sender = Some(kv_sender);
        }
    }
}

pub fn document_tree_name(collection: &CollectionName) -> String {
    format!("collection.{}", collection)
}

#[async_trait]
impl<DB> OpenDatabase for Database<DB>
where
    DB: Schema,
{
    fn as_any(&self) -> &'_ dyn Any {
        self
    }

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
        permissions: &Permissions,
    ) -> Result<Option<Document<'static>>, bonsaidb_core::Error> {
        self.with_effective_permissions(permissions.clone())
            .get_from_collection_id(id, collection)
            .await
    }

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
        permissions: &Permissions,
    ) -> Result<Vec<Document<'static>>, bonsaidb_core::Error> {
        self.with_effective_permissions(permissions.clone())
            .get_multiple_from_collection_id(ids, collection)
            .await
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
        permissions: &Permissions,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        <Self as Connection>::apply_transaction(
            &self.with_effective_permissions(permissions.clone()),
            transaction,
        )
        .await
    }

    async fn query(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, bonsaidb_core::Error> {
        if let Some(view) = self.schematic().view_by_name(view) {
            let mut results = Vec::new();
            self.for_each_in_view(view, key, access_policy, |collection| {
                let entry = ViewEntry::from(collection);
                for mapping in entry.mappings {
                    results.push(map::Serialized {
                        source: mapping.source,
                        key: entry.key.clone(),
                        value: mapping.value,
                    });
                }
                Ok(())
            })
            .await?;

            Ok(results)
        } else {
            Err(bonsaidb_core::Error::CollectionNotFound)
        }
    }

    async fn query_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        permissions: &Permissions,
    ) -> Result<Vec<map::MappedSerialized>, bonsaidb_core::Error> {
        let results = OpenDatabase::query(self, view, key, access_policy).await?;
        let view = self.schematic().view_by_name(view).unwrap(); // query() will fail if it's not present

        let mut documents = self
            .with_effective_permissions(permissions.clone())
            .get_multiple_from_collection_id(
                &results.iter().map(|m| m.source).collect::<Vec<_>>(),
                &view.collection()?,
            )
            .await?
            .into_iter()
            .map(|doc| (doc.header.id, doc))
            .collect::<HashMap<_, _>>();

        Ok(results
            .into_iter()
            .filter_map(|map| {
                if let Some(source) = documents.remove(&map.source) {
                    Some(map::MappedSerialized {
                        key: map.key,
                        value: map.value,
                        source,
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn reduce(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        self.reduce_in_view(view, key, access_policy).await
    }

    async fn reduce_grouped(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, bonsaidb_core::Error> {
        self.grouped_reduce_in_view(view, key, access_policy).await
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed<'static>>, bonsaidb_core::Error> {
        Connection::list_executed_transactions(self, starting_id, result_limit).await
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Connection::last_transaction_id(self).await
    }

    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        Kv::execute_key_operation(self, op).await
    }
}
