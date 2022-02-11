use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    ops::Deref,
    sync::Arc,
    u8,
};

use async_lock::Mutex;
use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::{serde::Bytes, ArcBytes},
    connection::{AccessPolicy, Connection, QueryKey, Range, Sort, StorageConnection},
    document::{BorrowedDocument, Document, Header, KeyId, OwnedDocument},
    keyvalue::{KeyOperation, Output, Timestamp},
    limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
    permissions::Permissions,
    schema::{
        self,
        view::{
            self,
            map::{MappedDocuments, MappedSerializedValue},
        },
        Collection, CollectionName, Key, Map, MappedValue, Schema, Schematic, ViewName,
    },
    transaction::{
        self, ChangedDocument, Changes, Command, Operation, OperationResult, Transaction,
    },
};
use bonsaidb_utils::fast_async_lock;
use byteorder::{BigEndian, ByteOrder};
use itertools::Itertools;
use nebari::{
    io::any::AnyFile,
    tree::{
        AnyTreeRoot, BorrowByteRange, KeyEvaluation, Root, TreeRoot, U64Range, Unversioned,
        Versioned,
    },
    AbortError, ExecutingTransaction, Roots, Tree,
};
use tokio::sync::watch;

#[cfg(feature = "encryption")]
use crate::vault::TreeVault;
use crate::{
    config::{Builder, KeyValuePersistence, StorageConfiguration},
    error::Error,
    open_trees::OpenTrees,
    views::{
        mapper::{self, ViewEntryCollection},
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name, ViewEntry,
    },
    Storage,
};

pub mod keyvalue;

pub mod pubsub;

/// A local, file-based database.
#[derive(Debug)]
pub struct Database {
    pub(crate) data: Arc<Data>,
}

#[derive(Debug)]
pub struct Data {
    pub name: Arc<Cow<'static, str>>,
    context: Context,
    pub(crate) storage: Storage,
    pub(crate) schema: Arc<Schematic>,
    #[allow(dead_code)] // This code was previously used, it works, but is currently unused.
    pub(crate) effective_permissions: Option<Permissions>,
}
impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl Database {
    /// Opens a local file as a bonsaidb.
    pub(crate) async fn new<DB: Schema, S: Into<Cow<'static, str>> + Send>(
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
            }),
        }
    }

    /// Returns the name of the database.
    #[must_use]
    pub fn name(&self) -> &str {
        self.data.name.as_ref()
    }

    /// Creates a `Storage` with a single-database named "default" with its data stored at `path`.
    pub async fn open<DB: Schema>(configuration: StorageConfiguration) -> Result<Self, Error> {
        let storage = Storage::open(configuration.with_schema::<DB>()?).await?;

        storage.create_database::<DB>("default", true).await?;

        Ok(storage.database::<DB>("default").await?)
    }

    /// Returns the [`Storage`] that this database belongs to.
    #[must_use]
    pub fn storage(&self) -> &'_ Storage {
        &self.data.storage
    }

    /// Returns the [`Schematic`] for the schema for this database.
    #[must_use]
    pub fn schematic(&self) -> &'_ Schematic {
        &self.data.schema
    }

    pub(crate) fn roots(&self) -> &'_ nebari::Roots<AnyFile> {
        &self.data.context.roots
    }

    async fn for_each_in_view<
        F: FnMut(ViewEntryCollection) -> Result<(), bonsaidb_core::Error> + Send + Sync,
    >(
        &self,
        view: &dyn view::Serialized,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
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
                &view.collection(),
                view_entries_tree_name(&view.view_name()),
            )?)
            .map_err(Error::from)?;

        {
            for entry in Self::create_view_iterator(&view_entries, key, order, limit)? {
                callback(entry)?;
            }
        }

        if matches!(access_policy, AccessPolicy::UpdateAfter) {
            let db = self.clone();
            let view_name = view.view_name();
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
        order: Sort,
        limit: Option<usize>,
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
            order,
            limit,
            access_policy,
            callback,
        )
        .await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn internal_get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        self.get_from_collection_id(id, collection).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn list_from_collection(
        &self,
        ids: Range<u64>,
        order: Sort,
        limit: Option<usize>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.list(ids, order, limit, collection).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn internal_get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.get_multiple_from_collection_id(ids, collection).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_collection(self.clone(), collection)
            .await?;
        Ok(())
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<bonsaidb_core::schema::view::map::Serialized>, bonsaidb_core::Error> {
        if let Some(view) = self.schematic().view_by_name(view) {
            let mut results = Vec::new();
            self.for_each_in_view(view, key, order, limit, access_policy, |collection| {
                let entry = ViewEntry::from(collection);
                for mapping in entry.mappings {
                    results.push(bonsaidb_core::schema::view::map::Serialized {
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

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<bonsaidb_core::schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error>
    {
        let results = self
            .query_by_name(view, key, order, limit, access_policy)
            .await?;
        let view = self.schematic().view_by_name(view).unwrap(); // query() will fail if it's not present

        let documents = self
            .get_multiple_from_collection_id(
                &results.iter().map(|m| m.source.id).collect::<Vec<_>>(),
                &view.collection(),
            )
            .await?
            .into_iter()
            .map(|doc| (doc.header.id, doc))
            .collect::<BTreeMap<_, _>>();

        Ok(
            bonsaidb_core::schema::view::map::MappedSerializedDocuments {
                mappings: results,
                documents,
            },
        )
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        self.reduce_in_view(view, key, access_policy).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        self.grouped_reduce_in_view(view, key, access_policy).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view)
            .ok_or(bonsaidb_core::Error::CollectionNotFound)?;
        let collection = view.collection();
        let mut transaction = Transaction::default();
        self.for_each_in_view(view, key, Sort::Ascending, None, access_policy, |entry| {
            let entry = ViewEntry::from(entry);

            for mapping in entry.mappings {
                transaction.push(Operation::delete(collection.clone(), mapping.source));
            }

            Ok(())
        })
        .await?;

        let results = Connection::apply_transaction(self, transaction).await?;

        Ok(results.len() as u64)
    }

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree = task_self
                .data
                .context
                .roots
                .tree(task_self.collection_tree::<Versioned, _>(
                    &collection,
                    document_tree_name(&collection),
                )?)
                .map_err(Error::from)?;
            if let Some(vec) = tree
                .get(
                    &id.as_big_endian_bytes()
                        .map_err(view::Error::key_serialization)?,
                )
                .map_err(Error::from)?
            {
                Ok(Some(deserialize_document(&vec)?.into_owned()))
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
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let ids = ids.iter().map(|id| id.to_be_bytes()).collect::<Vec<_>>();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree = task_self
                .data
                .context
                .roots
                .tree(task_self.collection_tree::<Versioned, _>(
                    &collection,
                    document_tree_name(&collection),
                )?)
                .map_err(Error::from)?;
            let mut ids = ids.iter().map(|id| &id[..]).collect::<Vec<_>>();
            ids.sort();
            let keys_and_values = tree.get_multiple(&ids).map_err(Error::from)?;

            keys_and_values
                .into_iter()
                .map(|(_, value)| deserialize_document(&value).map(BorrowedDocument::into_owned))
                .collect()
        })
        .await
        .unwrap()
    }

    pub(crate) async fn list(
        &self,
        ids: Range<u64>,
        sort: Sort,
        limit: Option<usize>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree = task_self
                .data
                .context
                .roots
                .tree(task_self.collection_tree::<Versioned, _>(
                    &collection,
                    document_tree_name(&collection),
                )?)
                .map_err(Error::from)?;
            let mut found_docs = Vec::new();
            let mut keys_read = 0;
            let ids = U64Range::new(ids);
            tree.scan(
                &ids.borrow_as_bytes(),
                match sort {
                    Sort::Ascending => true,
                    Sort::Descending => false,
                },
                |_, _, _| true,
                |_, _| {
                    if let Some(limit) = limit {
                        if keys_read >= limit {
                            return KeyEvaluation::Stop;
                        }

                        keys_read += 1;
                    }
                    KeyEvaluation::ReadData
                },
                |_, _, doc| {
                    found_docs.push(
                        deserialize_document(&doc)
                            .map(BorrowedDocument::into_owned)
                            .map_err(AbortError::Other)?,
                    );
                    Ok(())
                },
            )
            .map_err(|err| match err {
                AbortError::Other(err) => err,
                AbortError::Nebari(err) => bonsaidb_core::Error::from(crate::Error::from(err)),
            })
            .unwrap();

            Ok(found_docs)
        })
        .await
        .unwrap()
    }

    async fn reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Bytes>>,
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
            mappings.pop().unwrap().value.into_vec()
        } else {
            view.reduce(
                &mappings
                    .iter()
                    .map(|map| (map.key.as_ref(), map.value.as_ref()))
                    .collect::<Vec<_>>(),
                true,
            )
            .map_err(Error::from)?
        };

        Ok(result)
    }

    async fn grouped_reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view_name)
            .ok_or(bonsaidb_core::Error::CollectionNotFound)?;
        let mut mappings = Vec::new();
        self.for_each_in_view(view, key, Sort::Ascending, None, access_policy, |entry| {
            let entry = ViewEntry::from(entry);
            mappings.push(MappedSerializedValue {
                key: entry.key,
                value: entry.reduced_value,
            });
            Ok(())
        })
        .await?;

        Ok(mappings)
    }

    fn apply_transaction_to_roots(
        &self,
        transaction: &Transaction,
    ) -> Result<Vec<OperationResult>, Error> {
        let mut open_trees = OpenTrees::default();
        for op in &transaction.operations {
            if !self.data.schema.contains_collection_id(&op.collection) {
                return Err(Error::Core(bonsaidb_core::Error::CollectionNotFound));
            }

            open_trees.open_trees_for_document_change(
                &op.collection,
                &self.data.schema,
                self.collection_encryption_key(&op.collection),
                #[cfg(feature = "encryption")]
                self.storage().vault(),
            )?;
        }

        let mut roots_transaction = self
            .data
            .context
            .roots
            .transaction::<_, dyn AnyTreeRoot<AnyFile>>(&open_trees.trees)?;

        let mut results = Vec::new();
        let mut changed_documents = Vec::new();
        for op in &transaction.operations {
            let result = self.execute_operation(
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
            if let Some(views) = self.data.schema.views_in_collection(&collection) {
                let changed_documents = changed_documents.collect::<Vec<_>>();
                for view in views {
                    if !view.unique() {
                        let view_name = view.view_name();
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
            .set_data(pot::to_vec(&Changes::Documents(changed_documents))?)?;

        roots_transaction.commit()?;

        Ok(results)
    }

    fn execute_operation(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
    ) -> Result<OperationResult, Error> {
        match &operation.command {
            Command::Insert { id, contents } => self.execute_insert(
                operation,
                transaction,
                tree_index_map,
                *id,
                contents.to_vec(),
            ),
            Command::Update { header, contents } => self.execute_update(
                operation,
                transaction,
                tree_index_map,
                header,
                contents.to_vec(),
            ),
            Command::Delete { header } => {
                self.execute_delete(operation, transaction, tree_index_map, header)
            }
        }
    }

    fn execute_update(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        header: &Header,
        contents: Vec<u8>,
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
                        header: updated_doc.header,
                    })
                } else {
                    // If no new revision was made, it means an attempt to
                    // save a document with the same contents was made.
                    // We'll return a success but not actually give a new
                    // version
                    Ok(OperationResult::DocumentUpdated {
                        collection: operation.collection.clone(),
                        header: doc.header,
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
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        id: Option<u64>,
        contents: Vec<u8>,
    ) -> Result<OperationResult, Error> {
        let documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let id = if let Some(id) = id {
            id
        } else {
            let last_key = documents
                .last_key()?
                .map(|bytes| BigEndian::read_u64(&bytes))
                .unwrap_or_default();
            last_key + 1
        };

        let doc = BorrowedDocument::new(id, contents);
        let serialized: Vec<u8> = serialize_document(&doc)?;
        let document_id = ArcBytes::from(doc.header.id.as_big_endian_bytes().unwrap().to_vec());
        if documents
            .replace(document_id.clone(), serialized)?
            .is_some()
        {
            Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                operation.collection.clone(),
                id,
            )))
        } else {
            self.update_unique_views(&document_id, operation, transaction, tree_index_map)?;

            Ok(OperationResult::DocumentUpdated {
                collection: operation.collection.clone(),
                header: doc.header,
            })
        }
    }

    fn execute_delete(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        header: &Header,
    ) -> Result<OperationResult, Error> {
        let documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let document_id = header.id.as_big_endian_bytes().unwrap();
        if let Some(vec) = documents.remove(&document_id)? {
            let doc = deserialize_document(&vec)?;
            if &doc.header == header {
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
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
    ) -> Result<(), Error> {
        if let Some(unique_views) = self
            .data
            .schema
            .unique_views_in_collection(&operation.collection)
        {
            for view in unique_views {
                let name = view.view_name();
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

    fn create_view_iterator<'a, K: for<'k> Key<'k> + 'a>(
        view_entries: &'a Tree<Unversioned, AnyFile>,
        key: Option<QueryKey<K>>,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Vec<ViewEntryCollection>, Error> {
        let mut values = Vec::new();
        let forwards = match order {
            Sort::Ascending => true,
            Sort::Descending => false,
        };
        let mut values_read = 0;
        if let Some(key) = key {
            match key {
                QueryKey::Range(range) => {
                    let range = range
                        .as_big_endian_bytes()
                        .map_err(view::Error::key_serialization)?;
                    view_entries.scan::<Infallible, _, _, _, _>(
                        &range.map_ref(|bytes| &bytes[..]),
                        forwards,
                        |_, _, _| true,
                        |_, _| {
                            if let Some(limit) = limit {
                                if values_read >= limit {
                                    return KeyEvaluation::Stop;
                                }
                                values_read += 1;
                            }
                            KeyEvaluation::ReadData
                        },
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
                    let mut list = list
                        .into_iter()
                        .map(|key| {
                            key.as_big_endian_bytes()
                                .map(|bytes| bytes.to_vec())
                                .map_err(view::Error::key_serialization)
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    list.sort();

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
                &(..),
                forwards,
                |_, _, _| true,
                |_, _| {
                    if let Some(limit) = limit {
                        if values_read >= limit {
                            return KeyEvaluation::Stop;
                        }
                        values_read += 1;
                    }
                    KeyEvaluation::ReadData
                },
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

    #[cfg_attr(
        not(feature = "encryption"),
        allow(unused_mut, unused_variables, clippy::let_and_return)
    )]
    #[cfg_attr(feature = "encryption", allow(clippy::unnecessary_wraps))]
    pub(crate) fn collection_tree<R: Root, S: Into<Cow<'static, str>>>(
        &self,
        collection: &CollectionName,
        name: S,
    ) -> Result<TreeRoot<R, AnyFile>, Error> {
        let mut tree = R::tree(name);

        if let Some(key) = self.collection_encryption_key(collection) {
            #[cfg(feature = "encryption")]
            {
                tree = tree.with_vault(TreeVault {
                    key: key.clone(),
                    vault: self.storage().vault().clone(),
                });
            }

            #[cfg(not(feature = "encryption"))]
            {
                return Err(Error::EncryptionDisabled);
            }
        }

        Ok(tree)
    }

    pub(crate) async fn update_key_expiration_async<'key>(
        &self,
        tree_key: impl Into<Cow<'key, str>>,
        expiration: Option<Timestamp>,
    ) {
        self.data
            .context
            .update_key_expiration_async(tree_key, expiration)
            .await;
    }
}

pub(crate) fn deserialize_document(
    bytes: &[u8],
) -> Result<BorrowedDocument<'_>, bonsaidb_core::Error> {
    let document = bincode::deserialize::<BorrowedDocument<'_>>(bytes).map_err(Error::from)?;
    Ok(document)
}

fn serialize_document(document: &BorrowedDocument<'_>) -> Result<Vec<u8>, bonsaidb_core::Error> {
    bincode::serialize(document)
        .map_err(Error::from)
        .map_err(bonsaidb_core::Error::from)
}

#[async_trait]
impl Connection for Database {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(transaction)))]
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let mut unique_view_tasks = Vec::new();
        for collection_name in transaction
            .operations
            .iter()
            .map(|op| &op.collection)
            .collect::<HashSet<_>>()
        {
            if let Some(views) = self.data.schema.views_in_collection(collection_name) {
                for view in views {
                    if view.unique() {
                        if let Some(task) = self
                            .data
                            .storage
                            .tasks()
                            .spawn_integrity_check(view, self)
                            .await?
                        {
                            unique_view_tasks.push(task);
                        }
                    }
                }
            }
        }
        for task in unique_view_tasks {
            task.receive()
                .await
                .map_err(Error::from)?
                .map_err(Error::from)?;
        }

        tokio::task::spawn_blocking(move || task_self.apply_transaction_to_roots(&transaction))
            .await
            .map_err(|err| bonsaidb_core::Error::Database(err.to_string()))?
            .map_err(bonsaidb_core::Error::from)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(id)))]
    async fn get<C: schema::Collection>(
        &self,
        id: u64,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        self.get_from_collection_id(id, &C::collection_name()).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids)))]
    async fn get_multiple<C: schema::Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.get_multiple_from_collection_id(ids, &C::collection_name())
            .await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids, order, limit)))]
    async fn list<C: schema::Collection, R: Into<Range<u64>> + Send>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.list(ids.into(), order, limit, &C::collection_name())
            .await
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(starting_id, result_limit))
    )]
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed>, bonsaidb_core::Error> {
        let result_limit = result_limit
            .unwrap_or(LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT)
            .min(LIST_TRANSACTIONS_MAX_RESULTS);
        if result_limit > 0 {
            let task_self = self.clone();
            tokio::task::spawn_blocking::<_, Result<Vec<transaction::Executed>, Error>>(move || {
                let range = if let Some(starting_id) = starting_id {
                    Range::from(starting_id..)
                } else {
                    Range::from(..)
                };

                let mut entries = Vec::new();
                task_self.roots().transactions().scan(range, |entry| {
                    entries.push(entry);
                    entries.len() < result_limit
                })?;

                entries
                    .into_iter()
                    .map(|entry| {
                        if let Some(data) = entry.data() {
                            let changes = match pot::from_slice(data) {
                                Ok(changes) => changes,
                                Err(pot::Error::NotAPot) => {
                                    Changes::Documents(bincode::deserialize(entry.data().unwrap())?)
                                }
                                other => other?,
                            };
                            Ok(Some(transaction::Executed {
                                id: entry.id,
                                changes,
                            }))
                        } else {
                            Ok(None)
                        }
                    })
                    .filter_map(Result::transpose)
                    .collect::<Result<Vec<_>, Error>>()
            })
            .await
            .unwrap()
            .map_err(bonsaidb_core::Error::from)
        } else {
            // A request was made to return an empty result? This should probably be
            // an error, but technically this is a correct response.
            Ok(Vec::default())
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(key, order, limit, access_policy))
    )]
    #[must_use]
    async fn query<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let mut results = Vec::new();
        self.for_each_view_entry::<V, _>(key, order, limit, access_policy, |collection| {
            let entry = ViewEntry::from(collection);
            let key = <V::Key as Key>::from_big_endian_bytes(&entry.key)
                .map_err(view::Error::key_serialization)
                .map_err(Error::from)?;
            for entry in entry.mappings {
                results.push(Map::new(
                    entry.source,
                    key.clone(),
                    V::deserialize(&entry.value)?,
                ));
            }
            Ok(())
        })
        .await?;

        Ok(results)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(key, order, limit, access_policy))
    )]
    async fn query_with_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let results = Connection::query::<V>(self, key, order, limit, access_policy).await?;

        let documents = self
            .get_multiple::<V::Collection>(&results.iter().map(|m| m.source.id).collect::<Vec<_>>())
            .await?
            .into_iter()
            .map(|doc| (doc.header.id, doc))
            .collect::<BTreeMap<u64, _>>();

        Ok(MappedDocuments {
            mappings: results,
            documents,
        })
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(key, access_policy)))]
    async fn reduce<V: schema::SerializedView>(
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
                &view.view_name(),
                key.map(|key| key.serialized()).transpose()?,
                access_policy,
            )
            .await?;
        let value = V::deserialize(&result)?;

        Ok(value)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(key, access_policy)))]
    async fn reduce_grouped<V: schema::SerializedView>(
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
                &view.view_name(),
                key.map(|key| key.serialized()).transpose()?,
                access_policy,
            )
            .await?;
        results
            .into_iter()
            .map(|map| {
                Ok(MappedValue::new(
                    V::Key::from_big_endian_bytes(&map.key)
                        .map_err(view::Error::key_serialization)?,
                    V::deserialize(&map.value)?,
                ))
            })
            .collect::<Result<Vec<_>, bonsaidb_core::Error>>()
    }

    async fn delete_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let collection = <V::Collection as Collection>::collection_name();
        let mut transaction = Transaction::default();
        self.for_each_view_entry::<V, _>(
            key,
            Sort::Ascending,
            None,
            access_policy,
            |entry_collection| {
                let entry = ViewEntry::from(entry_collection);

                for mapping in entry.mappings {
                    transaction.push(Operation::delete(collection.clone(), mapping.source));
                }

                Ok(())
            },
        )
        .await?;

        let results = Connection::apply_transaction(self, transaction).await?;

        Ok(results.len() as u64)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self.roots().transactions().current_transaction_id())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact_collection<C: schema::Collection>(&self) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_collection(self.clone(), C::collection_name())
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_database(self.clone())
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_key_value_store(self.clone())
            .await?;
        Ok(())
    }
}

type ViewIterator<'a> =
    Box<dyn Iterator<Item = Result<(ArcBytes<'static>, ArcBytes<'static>), Error>> + 'a>;

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
    data: Arc<ContextData>,
}

impl Deref for Context {
    type Target = ContextData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug)]
pub(crate) struct ContextData {
    pub(crate) roots: Roots<AnyFile>,
    key_value_state: Arc<Mutex<keyvalue::KeyValueState>>,
    runtime: tokio::runtime::Handle,
}

impl Borrow<Roots<AnyFile>> for Context {
    fn borrow(&self) -> &Roots<AnyFile> {
        &self.data.roots
    }
}

impl Context {
    pub(crate) fn new(roots: Roots<AnyFile>, key_value_persistence: KeyValuePersistence) -> Self {
        let (background_sender, background_receiver) = watch::channel(None);
        let key_value_state = Arc::new(Mutex::new(keyvalue::KeyValueState::new(
            key_value_persistence,
            roots.clone(),
            background_sender,
        )));
        let context = Self {
            data: Arc::new(ContextData {
                roots,
                key_value_state: key_value_state.clone(),
                runtime: tokio::runtime::Handle::current(),
            }),
        };
        tokio::task::spawn(keyvalue::background_worker(
            key_value_state,
            background_receiver,
        ));
        context
    }

    pub(crate) async fn perform_kv_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        let mut state = fast_async_lock!(self.data.key_value_state);
        state
            .perform_kv_operation(op, &self.data.key_value_state)
            .await
    }

    pub(crate) async fn update_key_expiration_async<'key>(
        &self,
        tree_key: impl Into<Cow<'key, str>>,
        expiration: Option<Timestamp>,
    ) {
        let mut state = fast_async_lock!(self.data.key_value_state);
        state.update_key_expiration(tree_key, expiration);
    }
}

impl Drop for ContextData {
    fn drop(&mut self) {
        let key_value_state = self.key_value_state.clone();
        self.runtime.spawn(async move {
            let mut state = fast_async_lock!(key_value_state);
            state.shutdown(&key_value_state).await
        });
    }
}

pub fn document_tree_name(collection: &CollectionName) -> String {
    format!("collection.{:#}", collection)
}
