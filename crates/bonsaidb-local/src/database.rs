use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    io::ErrorKind,
    ops::{self, Deref},
    sync::Arc,
    u8,
};

#[cfg(any(feature = "encryption", feature = "compression"))]
use bonsaidb_core::document::KeyId;
use bonsaidb_core::{
    arc_bytes::{serde::Bytes, ArcBytes},
    connection::{
        self, AccessPolicy, Connection, HasSchema, HasSession, LowLevelConnection, Range,
        SerializedQueryKey, Session, Sort, StorageConnection,
    },
    document::{DocumentId, Header, OwnedDocument, Revision},
    keyvalue::{KeyOperation, Output, Timestamp},
    limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
    permissions::{
        bonsai::{
            collection_resource_name, database_resource_name, document_resource_name,
            kv_resource_name, view_resource_name, BonsaiAction, DatabaseAction, DocumentAction,
            TransactionAction, ViewAction,
        },
        Permissions,
    },
    schema::{
        self,
        view::{self, map::MappedSerializedValue},
        CollectionName, Schema, Schematic, ViewName,
    },
    transaction::{
        self, ChangedDocument, Changes, Command, DocumentChanges, Operation, OperationResult,
        Transaction,
    },
};
use nebari::{
    io::any::AnyFile,
    transaction::TransactionId,
    tree::{
        btree::{Indexer, Reducer},
        AnyTreeRoot, BorrowByteRange, BorrowedRange, ByIdIndexer, CompareSwap, EmbeddedIndex,
        Entry, PersistenceMode, Root, ScanEvaluation, SequenceId, Serializable, TreeRoot,
        ValueIndex, VersionedByIdIndex, VersionedTreeRoot,
    },
    AbortError, ExecutingTransaction, Roots, Tree,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use watchable::Watchable;

#[cfg(feature = "encryption")]
use crate::storage::TreeVault;
use crate::{
    config::{Builder, KeyValuePersistence, StorageConfiguration},
    database::keyvalue::BackgroundWorkerProcessTarget,
    error::Error,
    open_trees::OpenTrees,
    storage::StorageLock,
    views::{mapper, view_entries_tree_name, ViewEntries, ViewEntry, ViewIndexer},
    Storage,
};

pub mod keyvalue;

pub(crate) mod compat;
pub mod pubsub;

/// A database stored in BonsaiDb. This type blocks the current thread when
/// used. See [`AsyncDatabase`](crate::AsyncDatabase) for this type's async counterpart.
///
/// ## Converting between Blocking and Async Types
///
/// [`AsyncDatabase`](crate::AsyncDatabase) and [`Database`] can be converted to and from each other
/// using:
///
/// - [`AsyncDatabase::into_blocking()`](crate::AsyncDatabase::into_blocking)
/// - [`AsyncDatabase::to_blocking()`](crate::AsyncDatabase::to_blocking)
/// - [`AsyncDatabase::as_blocking()`](crate::AsyncDatabase::as_blocking)
/// - [`Database::into_async()`]
/// - [`Database::to_async()`]
/// - [`Database::into_async_with_runtime()`]
/// - [`Database::to_async_with_runtime()`]
///
/// ## Using `Database` to create a single database
///
/// `Database`provides an easy mechanism to open and access a single database:
///
/// ```rust
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_local::core`.
/// use bonsaidb_core::schema::Collection;
/// // `bonsaidb_local` is re-exported to `bonsaidb::local` if using the omnibus crate.
/// use bonsaidb_local::{
///     config::{Builder, StorageConfiguration},
///     Database,
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, Collection)]
/// #[collection(name = "blog-posts")]
/// # #[collection(core = bonsaidb_core)]
/// struct BlogPost {
///     pub title: String,
///     pub contents: String,
/// }
///
/// # fn test() -> Result<(), bonsaidb_local::Error> {
/// let db = Database::open::<BlogPost>(StorageConfiguration::new("my-db.bonsaidb"))?;
/// # Ok(())
/// # }
/// ```
///
/// Under the hood, this initializes a [`Storage`] instance pointing at
/// "./my-db.bonsaidb". It then returns (or creates) a database named "default"
/// with the schema `BlogPost`.
///
/// In this example, `BlogPost` implements the [`Collection`](schema::Collection) trait, and all
/// collections can be used as a [`Schema`].
#[derive(Debug, Clone)]
pub struct Database {
    pub(crate) data: Arc<Data>,
    pub(crate) storage: Storage,
}

#[derive(Debug)]
pub struct Data {
    pub name: Arc<Cow<'static, str>>,
    context: Context,
    pub(crate) schema: Arc<Schematic>,
}

impl Database {
    /// Opens a local file as a bonsaidb.
    pub(crate) fn new<DB: Schema, S: Into<Cow<'static, str>> + Send>(
        name: S,
        context: Context,
        storage: &Storage,
    ) -> Result<Self, Error> {
        let name = name.into();
        let schema = Arc::new(DB::schematic()?);
        let db = Self {
            storage: storage.clone(),
            data: Arc::new(Data {
                name: Arc::new(name),
                context,
                schema,
            }),
        };

        if storage.instance.check_view_integrity_on_database_open() {
            for view in db.data.schema.views() {
                storage.instance.tasks().spawn_integrity_check(view, &db);
            }
        }

        storage
            .instance
            .tasks()
            .spawn_key_value_expiration_loader(&db);

        Ok(db)
    }

    /// Restricts an unauthenticated instance to having `effective_permissions`.
    /// Returns `None` if a session has already been established.
    #[must_use]
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Option<Self> {
        self.storage
            .with_effective_permissions(effective_permissions)
            .map(|storage| Self {
                storage,
                data: self.data.clone(),
            })
    }

    /// Creates a `Storage` with a single-database named "default" with its data
    /// stored at `path`. This requires exclusive access to the storage location
    /// configured. Attempting to open the same path multiple times concurrently
    /// will lead to errors.
    ///
    /// Using this method is perfect if only one database is being used.
    /// However, if multiple databases are needed, it is much better to store
    /// multiple databases in a single [`Storage`] instance rather than creating
    /// multiple independent databases using this method.
    ///
    /// When opening multiple databases using this function, each database will
    /// have its own thread pool, cache, task worker pool, and more. By using a
    /// single [`Storage`] instance, BonsaiDb will use less resources and likely
    /// perform better.
    pub fn open<DB: Schema>(configuration: StorageConfiguration) -> Result<Self, Error> {
        let storage = Storage::open(configuration.with_schema::<DB>()?)?;

        Ok(storage.create_database::<DB>("default", true)?)
    }

    /// Returns the [`Schematic`] for the schema for this database.
    #[must_use]
    pub fn schematic(&self) -> &'_ Schematic {
        &self.data.schema
    }

    pub(crate) fn roots(&self) -> &'_ nebari::Roots<AnyFile> {
        &self.data.context.roots
    }

    pub(crate) fn context(&self) -> &Context {
        &self.data.context
    }

    fn access_view<
        F: FnOnce(&Tree<ViewEntries, AnyFile>) -> Result<(), bonsaidb_core::Error> + Send + Sync,
    >(
        &self,
        view: &Arc<dyn view::Serialized>,
        access_policy: AccessPolicy,
        callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
        if matches!(access_policy, AccessPolicy::UpdateBefore) {
            self.storage
                .instance
                .tasks()
                .update_view_if_needed(view, self, true)?;
        } else if let Some(integrity_check) = self
            .storage
            .instance
            .tasks()
            .spawn_integrity_check(view, self)
        {
            integrity_check
                .receive()
                .map_err(Error::from)?
                .map_err(Error::from)?;
        }

        let view_entries = self
            .roots()
            .tree(self.collection_tree_with_reducer(
                &view.collection(),
                view_entries_tree_name(&view.view_name()),
                ByIdIndexer(ViewIndexer::new(view.clone())),
            )?)
            .map_err(Error::from)?;

        callback(&view_entries)?;

        if matches!(access_policy, AccessPolicy::UpdateAfter) {
            let db = self.clone();
            let view_name = view.view_name();
            let view = db
                .data
                .schema
                .view_by_name(&view_name)
                .expect("query made with view that isn't registered with this database");
            db.storage
                .instance
                .tasks()
                .update_view_if_needed(view, &db, false)?;
        }

        Ok(())
    }

    fn for_each_in_view<F: FnMut(ViewEntry) -> Result<(), bonsaidb_core::Error> + Send + Sync>(
        &self,
        view: &Arc<dyn view::Serialized>,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
        mut callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
        self.access_view(view, access_policy, |view_entries| {
            for entry in Self::create_view_iterator(view_entries, key, order, limit)? {
                callback(entry)?;
            }
            Ok(())
        })
    }

    fn apply_transaction_to_roots(
        &self,
        transaction: &Transaction,
    ) -> Result<Vec<OperationResult>, Error> {
        let mut open_trees = OpenTrees::default();
        for op in &transaction.operations {
            if !self.data.schema.contains_collection_name(&op.collection) {
                return Err(Error::Core(bonsaidb_core::Error::CollectionNotFound));
            }

            #[cfg(any(feature = "encryption", feature = "compression"))]
            let vault = if let Some(encryption_key) =
                self.collection_encryption_key(&op.collection).cloned()
            {
                #[cfg(feature = "encryption")]
                if let Some(mut vault) = self.storage().tree_vault().cloned() {
                    vault.key = Some(encryption_key);
                    Some(vault)
                } else {
                    TreeVault::new_if_needed(
                        Some(encryption_key),
                        self.storage().vault(),
                        #[cfg(feature = "compression")]
                        None,
                    )
                }

                #[cfg(not(feature = "encryption"))]
                {
                    drop(encryption_key);
                    return Err(Error::EncryptionDisabled);
                }
            } else {
                self.storage().tree_vault().cloned()
            };

            open_trees.open_trees_for_document_change(
                &op.collection,
                &self.data.schema,
                #[cfg(any(feature = "encryption", feature = "compression"))]
                vault,
            );
        }

        let mut roots_transaction = self
            .data
            .context
            .roots
            .transaction::<_, dyn AnyTreeRoot<AnyFile>>(&open_trees.trees)?;

        let mut results = Vec::new();
        let mut changed_documents = Vec::new();
        let mut collection_indexes = HashMap::new();
        let mut collections = Vec::new();
        for op in &transaction.operations {
            let result = self.execute_operation(
                op,
                &mut roots_transaction,
                &open_trees.trees_index_by_name,
            )?;

            if let Some((collection, id, deleted)) = match &result {
                OperationResult::DocumentUpdated { header, collection } => {
                    Some((collection, header.id.clone(), false))
                }
                OperationResult::DocumentDeleted { id, collection } => {
                    Some((collection, id.clone(), true))
                }
                OperationResult::Success => None,
            } {
                let collection = match collection_indexes.get(collection) {
                    Some(index) => *index,
                    None => {
                        if let Ok(id) = u16::try_from(collections.len()) {
                            collection_indexes.insert(collection.clone(), id);
                            collections.push(collection.clone());
                            id
                        } else {
                            return Err(Error::TransactionTooLarge);
                        }
                    }
                };
                changed_documents.push(ChangedDocument {
                    collection,
                    id,
                    deleted,
                });
            }
            results.push(result);
        }

        roots_transaction
            .entry_mut()
            .set_data(compat::serialize_executed_transaction_changes(
                &Changes::Documents(DocumentChanges {
                    collections,
                    documents: changed_documents,
                }),
            )?)?;

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
            Command::Insert { id, contents } => {
                self.execute_insert(operation, transaction, tree_index_map, id.clone(), contents)
            }
            Command::Update { header, contents } => self.execute_update(
                operation,
                transaction,
                tree_index_map,
                &header.id,
                Some(&header.revision),
                contents,
            ),
            Command::Overwrite { id, contents } => {
                self.execute_update(operation, transaction, tree_index_map, id, None, contents)
            }
            Command::Delete { header } => {
                self.execute_delete(operation, transaction, tree_index_map, header)
            }
            Command::Check { id, revision } => Self::execute_check(
                operation,
                transaction,
                tree_index_map,
                id.clone(),
                *revision,
            ),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self, operation, transaction, tree_index_map, contents),
            fields(
                database = self.name(),
                collection.name = operation.collection.name.as_ref(),
                collection.authority = operation.collection.authority.as_ref()
            )
        )
    )]
    fn execute_update(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        id: &DocumentId,
        check_revision: Option<&Revision>,
        contents: &[u8],
    ) -> Result<OperationResult, crate::Error> {
        let mut documents = transaction
            .tree::<DocumentsTree>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let document_id = ArcBytes::from(id.to_vec());
        let mut existing_revision = None;
        let mut conflicting_header = None;
        let results = documents.modify(
            vec![document_id.clone()],
            nebari::tree::Operation::CompareSwap(CompareSwap::new(&mut |_key,
                                                                        index: Option<
                &VersionedByIdIndex<DocumentHash, ArcBytes<'static>>,
            >,
                                                                        value: Option<
                ArcBytes<'_>,
            >| {
                if let (Some(old), Some(index)) = (value, index) {
                    let last_revision = Revision {
                        id: index.sequence_id.0,
                        sha256: index.embedded.hash,
                    };
                    existing_revision = Some(last_revision);
                    let passes_revision_check = match check_revision {
                        Some(revision) => revision == &last_revision,
                        None => true,
                    };
                    if passes_revision_check {
                        if contents != old.as_ref() {
                            return nebari::tree::btree::KeyOperation::Set(ArcBytes::from(
                                contents.to_vec(),
                            ));
                        }
                    } else {
                        conflicting_header = Some(Header {
                            id: id.clone(),
                            revision: Revision {
                                id: index.sequence_id.0,
                                sha256: index.embedded.hash,
                            },
                        });
                    }
                } else if check_revision.is_none() {
                    return nebari::tree::btree::KeyOperation::Set(ArcBytes::from(
                        contents.to_vec(),
                    ));
                }
                nebari::tree::btree::KeyOperation::Skip
            })),
        )?;
        drop(documents);

        if let Some(header) = conflicting_header {
            Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                operation.collection.clone(),
                Box::new(header),
            )))
        } else if let Some(result) = results.into_iter().next() {
            let index = result.index.expect("no removal possible");
            self.update_collection_for_sequence(
                index.sequence_id,
                operation,
                transaction,
                tree_index_map,
            )?;
            Ok(OperationResult::DocumentUpdated {
                collection: operation.collection.clone(),
                header: Header {
                    id: id.clone(),
                    revision: Revision {
                        id: index.sequence_id.0,
                        sha256: index.embedded.hash,
                    },
                },
            })
        } else if let Some(existing_revision) = existing_revision {
            // The document contents didn't change. We return a DocumentUpdated
            // result, but the header is the same as it was before. This code
            // block can only be reached if the revision check succeeded.
            Ok(OperationResult::DocumentUpdated {
                collection: operation.collection.clone(),
                header: Header {
                    id: id.clone(),
                    revision: existing_revision,
                },
            })
        } else {
            Err(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                operation.collection.clone(),
                Box::new(id.clone()),
            )))
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            skip(self, operation, transaction, tree_index_map, contents),
            fields(
                database = self.name(),
                collection.name = operation.collection.name.as_ref(),
                collection.authority = operation.collection.authority.as_ref()
            )
        )
    )]
    fn execute_insert(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        id: Option<DocumentId>,
        contents: &[u8],
    ) -> Result<OperationResult, Error> {
        let mut documents = transaction
            .tree::<DocumentsTree>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let id = if let Some(id) = id {
            id
        } else if let Some(last_key) = documents.last_key()? {
            let id = DocumentId::try_from(last_key.as_slice())?;
            self.data
                .schema
                .next_id_for_collection(&operation.collection, Some(id))?
        } else {
            self.data
                .schema
                .next_id_for_collection(&operation.collection, None)?
        };

        let document_id = ArcBytes::from(id.as_ref().to_vec());
        match documents.replace(document_id.clone(), ArcBytes::from(contents.to_vec()))? {
            (Some(_), index) => Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                operation.collection.clone(),
                Box::new(Header {
                    id,
                    revision: Revision {
                        id: index.sequence_id.0,
                        sha256: index.embedded.hash,
                    },
                }),
            ))),
            (None, index) => {
                drop(documents);
                self.update_collection_for_sequence(
                    index.sequence_id,
                    operation,
                    transaction,
                    tree_index_map,
                )?;

                Ok(OperationResult::DocumentUpdated {
                    collection: operation.collection.clone(),
                    header: Header {
                        id,
                        revision: Revision {
                            id: index.sequence_id.0,
                            sha256: index.embedded.hash,
                        },
                    },
                })
            }
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, operation, transaction, tree_index_map),
        fields(
            database = self.name(),
            collection.name = operation.collection.name.as_ref(),
            collection.authority = operation.collection.authority.as_ref()
        )
    ))]
    fn execute_delete(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        header: &Header,
    ) -> Result<OperationResult, Error> {
        let mut documents = transaction
            .tree::<DocumentsTree>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        if let Some(ValueIndex { index, .. }) = documents.remove(header.id.as_ref())? {
            let update_sequence = documents.current_sequence_id();
            drop(documents);
            let removed_revision = Revision {
                id: index.sequence_id.0,
                sha256: index.embedded.hash,
            };
            if removed_revision == header.revision {
                self.update_collection_for_sequence(
                    update_sequence,
                    operation,
                    transaction,
                    tree_index_map,
                )?;

                Ok(OperationResult::DocumentDeleted {
                    collection: operation.collection.clone(),
                    id: header.id.clone(),
                })
            } else {
                Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                    operation.collection.clone(),
                    Box::new(Header {
                        id: header.id.clone(),
                        revision: removed_revision,
                    }),
                )))
            }
        } else {
            Err(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                operation.collection.clone(),
                Box::new(header.id.clone()),
            )))
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, operation, transaction, tree_index_map),
        fields(
            database = self.name(),
            collection.name = operation.collection.name.as_ref(),
            collection.authority = operation.collection.authority.as_ref()
        )
    ))]
    fn update_collection_for_sequence(
        &self,
        sequence_id: SequenceId,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
    ) -> Result<(), Error> {
        self.data
            .context
            .update_collection_sequence(operation.collection.clone(), sequence_id);
        if let Some(eager_views) = self
            .data
            .schema
            .eager_views_in_collection(&operation.collection)
        {
            let documents = transaction
                .unlocked_tree(tree_index_map[&document_tree_name(&operation.collection)])
                .unwrap();
            for view in eager_views {
                let name = view.view_name();
                let view_entries = transaction
                    .unlocked_tree(tree_index_map[&view_entries_tree_name(&name)])
                    .unwrap();
                mapper::DocumentRequest {
                    sequence_ids: vec![sequence_id],
                    database: self,
                    map_request: &mapper::Map {
                        database: self.data.name.clone(),
                        collection: operation.collection.clone(),
                        view_name: name.clone(),
                    },
                    documents: &mut documents.lock().tree,
                    view_entries: &mut view_entries.lock().tree,
                    view,
                    persistence_mode: PersistenceMode::Transactional(transaction.entry().id),
                }
                .map()?;
            }
        }

        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(operation, transaction, tree_index_map),
        fields(
            collection.name = operation.collection.name.as_ref(),
            collection.authority = operation.collection.authority.as_ref(),
        ),
    ))]
    fn execute_check(
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        id: DocumentId,
        revision: Option<Revision>,
    ) -> Result<OperationResult, Error> {
        let mut documents = transaction
            .tree::<DocumentsTree>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        if let Some(index) = documents.get_index(id.as_ref())? {
            drop(documents);

            if let Some(revision) = revision {
                let retrieved_revision = Revision {
                    id: index.sequence_id.0,
                    sha256: index.embedded.hash,
                };
                if retrieved_revision != revision {
                    return Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                        operation.collection.clone(),
                        Box::new(Header { id, revision }),
                    )));
                }
            }

            Ok(OperationResult::Success)
        } else {
            Err(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                operation.collection.clone(),
                Box::new(id),
            )))
        }
    }

    fn create_view_iterator(
        view_entries: &Tree<ViewEntries, AnyFile>,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<ViewEntry>, Error> {
        let mut values = Vec::new();
        let forwards = match order {
            Sort::Ascending => true,
            Sort::Descending => false,
        };
        let mut values_read = 0;
        if let Some(key) = key {
            match key {
                SerializedQueryKey::Range(range) => {
                    view_entries.scan::<Infallible, _, _, _, _>(
                        &range.map_ref(|bytes| &bytes[..]),
                        forwards,
                        |_, _, _| ScanEvaluation::ReadData,
                        |_, _| {
                            if let Some(limit) = limit {
                                if values_read >= limit {
                                    return ScanEvaluation::Stop;
                                }
                                values_read += 1;
                            }
                            ScanEvaluation::ReadData
                        },
                        |key, index, value| {
                            values.push((Bytes::from(key), index.clone(), value));
                            Ok(())
                        },
                    )?;
                }
                SerializedQueryKey::Matches(key) => {
                    if let Some(ValueIndex { value, index }) = view_entries.get_with_index(&key)? {
                        values.push((key, index, value));
                    }
                }
                SerializedQueryKey::Multiple(mut list) => {
                    list.sort();

                    values.extend(
                        view_entries
                            .get_multiple_with_indexes(list.iter().map(|bytes| bytes.as_slice()))?
                            .into_iter()
                            .map(|Entry { key, value, index }| (Bytes::from(key), index, value)),
                    );
                }
            }
        } else {
            view_entries.scan::<Infallible, _, _, _, _>(
                &(..),
                forwards,
                |_, _, _| ScanEvaluation::ReadData,
                |_, _| {
                    if let Some(limit) = limit {
                        if values_read >= limit {
                            return ScanEvaluation::Stop;
                        }
                        values_read += 1;
                    }
                    ScanEvaluation::ReadData
                },
                |key, index, value| {
                    values.push((Bytes::from(key), index.clone(), value));
                    Ok(())
                },
            )?;
        }

        Ok(values
            .into_iter()
            .map(|(key, index, value)| ViewEntry {
                key,
                reduced_value: Bytes::from(index.embedded.value.unwrap_or_default()),
                mappings: value.mappings,
            })
            .collect::<Vec<_>>())
    }

    #[cfg(any(feature = "encryption", feature = "compression"))]
    pub(crate) fn collection_encryption_key(&self, collection: &CollectionName) -> Option<&KeyId> {
        self.schematic()
            .encryption_key_for_collection(collection)
            .or_else(|| self.storage.default_encryption_key())
    }

    pub(crate) fn collection_tree<R: Root, S: Into<Cow<'static, str>>>(
        &self,
        collection: &CollectionName,
        name: S,
    ) -> Result<TreeRoot<R, AnyFile>, Error>
    where
        R::Reducer: Default,
    {
        self.collection_tree_with_reducer(collection, name, <R::Reducer as Default>::default())
    }

    #[cfg_attr(
        not(feature = "encryption"),
        allow(
            unused_mut,
            unused_variables,
            clippy::unused_self,
            clippy::let_and_return
        )
    )]
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn collection_tree_with_reducer<R: Root, S: Into<Cow<'static, str>>>(
        &self,
        collection: &CollectionName,
        name: S,
        reducer: R::Reducer,
    ) -> Result<TreeRoot<R, AnyFile>, Error> {
        let mut tree = R::tree_with_reducer(name, reducer);

        #[cfg(any(feature = "encryption", feature = "compression"))]
        match (
            self.collection_encryption_key(collection),
            self.storage().tree_vault().cloned(),
        ) {
            (Some(override_key), Some(mut vault)) => {
                #[cfg(feature = "encryption")]
                {
                    vault.key = Some(override_key.clone());
                    tree = tree.with_vault(vault);
                }

                #[cfg(not(feature = "encryption"))]
                {
                    return Err(Error::EncryptionDisabled);
                }
            }
            (None, Some(vault)) => {
                tree = tree.with_vault(vault);
            }
            (key, None) => {
                #[cfg(feature = "encryption")]
                if let Some(vault) = TreeVault::new_if_needed(
                    key.cloned(),
                    self.storage().vault(),
                    #[cfg(feature = "compression")]
                    None,
                ) {
                    tree = tree.with_vault(vault);
                }

                #[cfg(not(feature = "encryption"))]
                if key.is_some() {
                    return Err(Error::EncryptionDisabled);
                }
            }
        }

        Ok(tree)
    }

    pub(crate) fn update_key_expiration<'key>(
        &self,
        tree_key: impl Into<Cow<'key, str>>,
        expiration: Option<Timestamp>,
    ) {
        self.data
            .context
            .update_key_expiration(tree_key, expiration);
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async. The returned instance uses the current Tokio runtime
    /// handle to spawn blocking tasks.
    ///
    /// # Panics
    ///
    /// Panics if called outside the context of a Tokio runtime.
    #[cfg(feature = "async")]
    #[must_use]
    pub fn into_async(self) -> crate::AsyncDatabase {
        self.into_async_with_runtime(tokio::runtime::Handle::current())
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async. The returned instance uses the provided runtime
    /// handle to spawn blocking tasks.
    #[cfg(feature = "async")]
    #[must_use]
    pub fn into_async_with_runtime(self, runtime: tokio::runtime::Handle) -> crate::AsyncDatabase {
        crate::AsyncDatabase {
            database: self,
            runtime: Arc::new(runtime),
        }
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async. The returned instance uses the current Tokio runtime
    /// handle to spawn blocking tasks.
    ///
    /// # Panics
    ///
    /// Panics if called outside the context of a Tokio runtime.
    #[cfg(feature = "async")]
    #[must_use]
    pub fn to_async(&self) -> crate::AsyncDatabase {
        self.clone().into_async()
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async. The returned instance uses the provided runtime
    /// handle to spawn blocking tasks.
    #[cfg(feature = "async")]
    #[must_use]
    pub fn to_async_with_runtime(&self, runtime: tokio::runtime::Handle) -> crate::AsyncDatabase {
        self.clone().into_async_with_runtime(runtime)
    }
}
#[derive(Serialize, Deserialize)]
struct LegacyHeader {
    id: u64,
    revision: Revision,
}
#[derive(Serialize, Deserialize)]
struct LegacyDocument<'a> {
    header: LegacyHeader,
    #[serde(borrow)]
    contents: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub enum DocumentHashAlgorithm {
    Sha256 = 0,
    Blake3 = 1,
}

impl Default for DocumentHashAlgorithm {
    fn default() -> Self {
        Self::Blake3
    }
}

impl TryFrom<u8> for DocumentHashAlgorithm {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Sha256),
            1 => Ok(Self::Blake3),
            _ => Err(Error::Core(bonsaidb_core::Error::other(
                "bonsaidb-local",
                "unknown document hash algorithm",
            ))),
        }
    }
}

impl HasSession for Database {
    fn session(&self) -> Option<&Session> {
        self.storage.session()
    }
}

impl Connection for Database {
    type Storage = Storage;

    fn storage(&self) -> Self::Storage {
        self.storage.clone()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self),
        fields(
            database = self.name(),
        )
    ))]
    fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<transaction::Executed>, bonsaidb_core::Error> {
        self.check_permission(
            database_resource_name(self.name()),
            &BonsaiAction::Database(DatabaseAction::Transaction(TransactionAction::ListExecuted)),
        )?;
        let result_limit = usize::try_from(
            result_limit
                .unwrap_or(LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT)
                .min(LIST_TRANSACTIONS_MAX_RESULTS),
        )
        .unwrap();
        if result_limit > 0 {
            let range = if let Some(starting_id) = starting_id {
                Range::from(TransactionId(starting_id)..)
            } else {
                Range::from(..)
            };

            let mut entries = Vec::new();
            self.roots()
                .transactions()
                .scan(range, |entry| {
                    if entry.data().is_some() {
                        entries.push(entry);
                    }
                    entries.len() < result_limit
                })
                .map_err(Error::from)?;

            entries
                .into_iter()
                .map(|entry| {
                    if let Some(data) = entry.data() {
                        let changes = compat::deserialize_executed_transaction_changes(data)?;
                        Ok(Some(transaction::Executed {
                            id: entry.id.0,
                            changes,
                        }))
                    } else {
                        Ok(None)
                    }
                })
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>, Error>>()
                .map_err(bonsaidb_core::Error::from)
        } else {
            // A request was made to return an empty result? This should probably be
            // an error, but technically this is a correct response.
            Ok(Vec::default())
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self),
        fields(
            database = self.name(),
        )
    ))]
    fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        self.check_permission(
            database_resource_name(self.name()),
            &BonsaiAction::Database(DatabaseAction::Transaction(TransactionAction::GetLastId)),
        )?;
        Ok(self
            .roots()
            .transactions()
            .current_transaction_id()
            .map(|id| id.0))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self),
        fields(
            database = self.name(),
        )
    ))]
    fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.check_permission(
            database_resource_name(self.name()),
            &BonsaiAction::Database(DatabaseAction::Compact),
        )?;
        self.storage()
            .instance
            .tasks()
            .compact_database(self.clone())?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self),
        fields(
            database = self.name(),
        )
    ))]
    fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.check_permission(
            kv_resource_name(self.name()),
            &BonsaiAction::Database(DatabaseAction::Compact),
        )?;
        self.storage()
            .instance
            .tasks()
            .compact_key_value_store(self.clone())?;
        Ok(())
    }
}

impl LowLevelConnection for Database {
    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self,  transaction),
        fields(
            database = self.name(),
        )
    ))]
    fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        for op in &transaction.operations {
            let (resource, action) = match &op.command {
                Command::Insert { .. } => (
                    collection_resource_name(self.name(), &op.collection),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Insert)),
                ),
                Command::Update { header, .. } => (
                    document_resource_name(self.name(), &op.collection, &header.id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Update)),
                ),
                Command::Overwrite { id, .. } => (
                    document_resource_name(self.name(), &op.collection, id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Overwrite)),
                ),
                Command::Delete { header } => (
                    document_resource_name(self.name(), &op.collection, &header.id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Delete)),
                ),
                Command::Check { id, .. } => (
                    document_resource_name(self.name(), &op.collection, id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Get)),
                ),
            };
            self.check_permission(&resource, &action)?;
        }

        let mut view_tasks = Vec::new();
        for collection_name in transaction
            .operations
            .iter()
            .map(|op| &op.collection)
            .collect::<HashSet<_>>()
        {
            if let Some(views) = self.data.schema.views_in_collection(collection_name) {
                for view in views {
                    if let Some(task) = self
                        .storage
                        .instance
                        .tasks()
                        .spawn_integrity_check(view, self)
                    {
                        view_tasks.push((view.eager(), task));
                    }
                }
            }
        }

        let mut eager_view_mapping_tasks = Vec::new();
        for (eager, task) in view_tasks {
            if let Some(spawned_task) = task.receive().map_err(Error::from)?.map_err(Error::from)? {
                // Eager views need to be fully updated.
                if eager {
                    eager_view_mapping_tasks.push(spawned_task);
                }
            }
        }

        for task in eager_view_mapping_tasks {
            let mut task = task.lock();
            if let Some(task) = task.take() {
                task.receive().map_err(Error::from)?.map_err(Error::from)?;
            }
        }

        self.apply_transaction_to_roots(&transaction)
            .map_err(bonsaidb_core::Error::from)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, collection),
        fields(
            database = self.name(),
            collection.name = collection.name.as_ref(),
            collection.authority = collection.authority.as_ref(),
        )
    ))]
    fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        self.check_permission(
            document_resource_name(self.name(), collection, &id),
            &BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Get)),
        )?;
        let tree =
            self.data
                .context
                .roots
                .tree(self.collection_tree::<DocumentsTree, _>(
                    collection,
                    document_tree_name(collection),
                )?)
                .map_err(Error::from)?;
        if let Some(ValueIndex { value, index }) =
            tree.get_with_index(id.as_ref()).map_err(Error::from)?
        {
            Ok(Some(OwnedDocument {
                header: Header {
                    id,
                    revision: Revision {
                        id: index.sequence_id.0,
                        sha256: index.embedded.hash,
                    },
                },
                contents: Bytes::from(value),
            }))
        } else {
            Ok(None)
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, collection),
        fields(
            database = self.name(),
            collection.name = collection.name.as_ref(),
            collection.authority = collection.authority.as_ref(),
        )
    ))]
    fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        sort: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.check_permission(
            collection_resource_name(self.name(), collection),
            &BonsaiAction::Database(DatabaseAction::Document(DocumentAction::List)),
        )?;
        let tree =
            self.data
                .context
                .roots
                .tree(self.collection_tree::<DocumentsTree, _>(
                    collection,
                    document_tree_name(collection),
                )?)
                .map_err(Error::from)?;
        let mut found_docs = Vec::new();
        let mut keys_read = 0;
        let ids = DocumentIdRange(ids);
        tree.scan(
            &ids.borrow_as_bytes(),
            match sort {
                Sort::Ascending => true,
                Sort::Descending => false,
            },
            |_, _, _| ScanEvaluation::ReadData,
            |_, _| {
                if let Some(limit) = limit {
                    if keys_read >= limit {
                        return ScanEvaluation::Stop;
                    }

                    keys_read += 1;
                }
                ScanEvaluation::ReadData
            },
            |key, index, contents| {
                found_docs.push(OwnedDocument {
                    header: Header {
                        id: DocumentId::try_from(key.as_slice()).unwrap(),
                        revision: Revision {
                            id: index.sequence_id.0,
                            sha256: index.embedded.hash,
                        },
                    },
                    contents: Bytes::from(contents),
                });
                Ok(())
            },
        )
        .map_err(|err| match err {
            AbortError::Other(err) => err,
            AbortError::Nebari(err) => crate::Error::from(err),
        })?;

        Ok(found_docs)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, collection),
        fields(
            database = self.name(),
            collection.name = collection.name.as_ref(),
            collection.authority = collection.authority.as_ref(),
        )
    ))]
    fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        sort: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, bonsaidb_core::Error> {
        self.check_permission(
            collection_resource_name(self.name(), collection),
            &BonsaiAction::Database(DatabaseAction::Document(DocumentAction::ListHeaders)),
        )?;
        let tree =
            self.data
                .context
                .roots
                .tree(self.collection_tree::<DocumentsTree, _>(
                    collection,
                    document_tree_name(collection),
                )?)
                .map_err(Error::from)?;
        let mut found_headers = Vec::new();
        let mut keys_read = 0;
        let ids = DocumentIdRange(ids);
        tree.scan(
            &ids.borrow_as_bytes(),
            match sort {
                Sort::Ascending => true,
                Sort::Descending => false,
            },
            |_, _, _| ScanEvaluation::ReadData,
            |key, index| {
                if let Some(limit) = limit {
                    if keys_read >= limit {
                        return ScanEvaluation::Stop;
                    }

                    keys_read += 1;
                }
                found_headers.push(Header {
                    id: DocumentId::try_from(key.as_slice()).unwrap(),
                    revision: Revision {
                        id: index.sequence_id.0,
                        sha256: index.embedded.hash,
                    },
                });
                ScanEvaluation::Skip
            },
            |_, _, _| unreachable!(),
        )
        .map_err(|err| match err {
            AbortError::Other(err) => err,
            AbortError::Nebari(err) => crate::Error::from(err),
        })?;

        Ok(found_headers)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, collection),
        fields(
            database = self.name(),
            collection.name = collection.name.as_ref(),
            collection.authority = collection.authority.as_ref(),
        )
    ))]
    fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.check_permission(
            collection_resource_name(self.name(), collection),
            &BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Count)),
        )?;
        let tree =
            self.data
                .context
                .roots
                .tree(self.collection_tree::<DocumentsTree, _>(
                    collection,
                    document_tree_name(collection),
                )?)
                .map_err(Error::from)?;
        let ids = DocumentIdRange(ids);
        let stats = tree.reduce(&ids.borrow_as_bytes()).map_err(Error::from)?;

        Ok(stats.map_or(0, |stats| stats.alive_keys))
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, collection),
        fields(
            database = self.name(),
            collection.name = collection.name.as_ref(),
            collection.authority = collection.authority.as_ref(),
        )
    ))]
    fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        for id in ids {
            self.check_permission(
                document_resource_name(self.name(), collection, id),
                &BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Get)),
            )?;
        }
        let mut ids = ids.to_vec();
        let collection = collection.clone();
        let tree = self
            .data
            .context
            .roots
            .tree(self.collection_tree::<DocumentsTree, _>(
                &collection,
                document_tree_name(&collection),
            )?)
            .map_err(Error::from)?;
        ids.sort();
        let keys_and_values = tree
            .get_multiple_with_indexes(ids.iter().map(|id| id.as_ref()))
            .map_err(Error::from)?;

        Ok(keys_and_values
            .into_iter()
            .map(|Entry { key, index, value }| OwnedDocument {
                header: Header {
                    id: DocumentId::try_from(key.as_slice()).unwrap(),
                    revision: Revision {
                        id: index.sequence_id.0,
                        sha256: index.embedded.hash,
                    },
                },
                contents: Bytes::from(value),
            })
            .collect::<Vec<_>>())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, collection),
        fields(
            database = self.name(),
            collection.name = collection.name.as_ref(),
            collection.authority = collection.authority.as_ref(),
        )
    ))]
    fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.check_permission(
            collection_resource_name(self.name(), &collection),
            &BonsaiAction::Database(DatabaseAction::Compact),
        )?;
        self.storage()
            .instance
            .tasks()
            .compact_collection(self.clone(), collection)?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, view),
        fields(
            database = self.name(),
            view.collection.name = view.collection.name.as_ref(),
            view.collection.authority = view.collection.authority.as_ref(),
            view.name = view.name.as_ref(),
        )
    ))]
    fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, bonsaidb_core::Error> {
        let view = self.schematic().view_by_name(view)?;
        self.check_permission(
            view_resource_name(self.name(), &view.view_name()),
            &BonsaiAction::Database(DatabaseAction::View(ViewAction::Query)),
        )?;
        let mut results = Vec::new();
        self.for_each_in_view(view, key, order, limit, access_policy, |entry| {
            for mapping in entry.mappings {
                results.push(bonsaidb_core::schema::view::map::Serialized {
                    source: mapping.source,
                    key: entry.key.clone(),
                    value: mapping.value,
                });
            }
            Ok(())
        })?;

        Ok(results)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, view),
        fields(
            database = self.name(),
            view.collection.name = view.collection.name.as_ref(),
            view.collection.authority = view.collection.authority.as_ref(),
            view.name = view.name.as_ref(),
        )
    ))]
    fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error> {
        let results = self.query_by_name(view, key, order, limit, access_policy)?;
        let view = self.schematic().view_by_name(view).unwrap(); // query() will fail if it's not present

        let documents = self
            .get_multiple_from_collection(
                &results
                    .iter()
                    .map(|m| m.source.id.clone())
                    .collect::<Vec<_>>(),
                &view.collection(),
            )?
            .into_iter()
            .map(|doc| (doc.header.id.clone(), doc))
            .collect::<BTreeMap<_, _>>();

        Ok(
            bonsaidb_core::schema::view::map::MappedSerializedDocuments {
                mappings: results,
                documents,
            },
        )
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, view_name),
        fields(
            database = self.name(),
            view.collection.name = view_name.collection.name.as_ref(),
            view.collection.authority = view_name.collection.authority.as_ref(),
            view.name = view_name.name.as_ref(),
        )
    ))]
    fn reduce_by_name(
        &self,
        view_name: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        let mut mappings = self.reduce_grouped_by_name(view_name, key, access_policy)?;

        let result = if mappings.len() == 1 {
            mappings.pop().unwrap().value.into_vec()
        } else {
            let view = self.data.schema.view_by_name(view_name)?;
            view.reduce(
                &mappings
                    .iter()
                    .map(|map| map.value.as_ref())
                    .collect::<Vec<_>>(),
                true,
            )
            .map_err(Error::from)?
        };

        Ok(result)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, view_name),
        fields(
            database = self.name(),
            view.collection.name = view_name.collection.name.as_ref(),
            view.collection.authority = view_name.collection.authority.as_ref(),
            view.name = view_name.name.as_ref(),
        )
    ))]
    fn reduce_grouped_by_name(
        &self,
        view_name: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        let view = self.data.schema.view_by_name(view_name)?;
        self.check_permission(
            view_resource_name(self.name(), &view.view_name()),
            &BonsaiAction::Database(DatabaseAction::View(ViewAction::Reduce)),
        )?;
        let mut mappings = Vec::new();
        // self.access_view(view, access_policy, |view_entries| view_entries.scan())?;
        self.for_each_in_view(view, key, Sort::Ascending, None, access_policy, |entry| {
            mappings.push(MappedSerializedValue {
                key: entry.key,
                value: entry.reduced_value,
            });
            Ok(())
        })?;

        Ok(mappings)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(
        level = "trace", 
        skip(self, view),
        fields(
            database = self.name(),
            view.collection.name = view.collection.name.as_ref(),
            view.collection.authority = view.collection.authority.as_ref(),
            view.name = view.name.as_ref(),
        )
    ))]
    fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        let view = self.data.schema.view_by_name(view)?;
        let collection = view.collection();
        let mut transaction = Transaction::default();
        self.for_each_in_view(view, key, Sort::Ascending, None, access_policy, |entry| {
            for mapping in entry.mappings {
                transaction.push(Operation::delete(collection.clone(), mapping.source));
            }

            Ok(())
        })?;

        let results = LowLevelConnection::apply_transaction(self, transaction)?;

        Ok(results.len() as u64)
    }
}

impl HasSchema for Database {
    fn schematic(&self) -> &Schematic {
        &self.data.schema
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
    collection_sequences: Mutex<HashMap<CollectionName, SequenceId>>,
}

impl Borrow<Roots<AnyFile>> for Context {
    fn borrow(&self) -> &Roots<AnyFile> {
        &self.data.roots
    }
}

impl Context {
    pub(crate) fn new(
        roots: Roots<AnyFile>,
        key_value_persistence: KeyValuePersistence,
        storage_lock: Option<StorageLock>,
    ) -> Self {
        let background_worker_target = Watchable::new(BackgroundWorkerProcessTarget::Never);
        let mut background_worker_target_watcher = background_worker_target.watch();
        let key_value_state = Arc::new(Mutex::new(keyvalue::KeyValueState::new(
            key_value_persistence,
            roots.clone(),
            background_worker_target,
        )));
        let background_worker_state = Arc::downgrade(&key_value_state);
        let context = Self {
            data: Arc::new(ContextData {
                roots,
                key_value_state,
                collection_sequences: Mutex::default(),
            }),
        };
        std::thread::Builder::new()
            .name(String::from("keyvalue-worker"))
            .spawn(move || {
                keyvalue::background_worker(
                    &background_worker_state,
                    &mut background_worker_target_watcher,
                    storage_lock,
                );
            })
            .unwrap();
        context
    }

    pub(crate) fn current_collection_sequence(
        &self,
        collection_name: &CollectionName,
    ) -> Option<SequenceId> {
        let mut sequences = self.data.collection_sequences.lock();
        if let Some(sequence) = sequences.get(collection_name) {
            Some(*sequence)
        } else {
            let tree = self
                .data
                .roots
                .tree(DocumentsTree::tree(document_tree_name(collection_name)))
                .ok()?;
            let sequence = dbg!(tree.current_sequence_id());
            sequences.insert(collection_name.clone(), sequence);
            Some(sequence)
        }
    }

    pub(crate) fn update_collection_sequence(
        &self,
        collection_name: CollectionName,
        new_sequence: SequenceId,
    ) {
        let mut sequences = self.data.collection_sequences.lock();
        sequences.insert(collection_name, new_sequence);
    }

    pub(crate) fn perform_kv_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        let mut state = self.data.key_value_state.lock();
        state.perform_kv_operation(op, &self.data.key_value_state)
    }

    pub(crate) fn update_key_expiration<'key>(
        &self,
        tree_key: impl Into<Cow<'key, str>>,
        expiration: Option<Timestamp>,
    ) {
        let mut state = self.data.key_value_state.lock();
        state.update_key_expiration(tree_key, expiration);
    }

    #[cfg(test)]
    pub(crate) fn kv_persistence_watcher(&self) -> watchable::Watcher<Timestamp> {
        let state = self.data.key_value_state.lock();
        state.persistence_watcher()
    }
}

impl Drop for ContextData {
    fn drop(&mut self) {
        if let Some(shutdown) = {
            let mut state = self.key_value_state.lock();
            state.shutdown(&self.key_value_state)
        } {
            let _ = shutdown.recv();
        }
    }
}

pub fn document_tree_name(collection: &CollectionName) -> String {
    format!("collection.{:#}", collection)
}

pub struct DocumentIdRange(Range<DocumentId>);

impl<'a> BorrowByteRange<'a> for DocumentIdRange {
    fn borrow_as_bytes(&'a self) -> BorrowedRange<'a> {
        BorrowedRange {
            start: match &self.0.start {
                connection::Bound::Unbounded => ops::Bound::Unbounded,
                connection::Bound::Included(docid) => ops::Bound::Included(docid.as_ref()),
                connection::Bound::Excluded(docid) => ops::Bound::Excluded(docid.as_ref()),
            },
            end: match &self.0.end {
                connection::Bound::Unbounded => ops::Bound::Unbounded,
                connection::Bound::Included(docid) => ops::Bound::Included(docid.as_ref()),
                connection::Bound::Excluded(docid) => ops::Bound::Excluded(docid.as_ref()),
            },
        }
    }
}

/// Operations that can be performed on both [`Database`] and
/// [`AsyncDatabase`](crate::AsyncDatabase).
pub trait DatabaseNonBlocking {
    /// Returns the name of the database.
    #[must_use]
    fn name(&self) -> &str;
}

impl DatabaseNonBlocking for Database {
    fn name(&self) -> &str {
        self.data.name.as_ref()
    }
}

pub type DocumentsTree = VersionedTreeRoot<DocumentHash>;

#[derive(Debug, Clone, Default)]
pub struct DocumentsIndexer;

impl EmbeddedIndex<ArcBytes<'static>> for DocumentHash {
    type Reduced = ();

    type Indexer = DocumentsIndexer;
}

#[derive(Default, Debug, Clone)]
pub struct DocumentHash {
    pub algorithm: DocumentHashAlgorithm,
    pub hash: [u8; 32],
}

impl Indexer<ArcBytes<'static>, DocumentHash> for DocumentsIndexer {
    fn index(&self, _key: &ArcBytes<'_>, value: Option<&ArcBytes<'static>>) -> DocumentHash {
        value
            .map(|value| DocumentHash {
                algorithm: DocumentHashAlgorithm::Blake3,
                hash: *blake3::hash(value).as_bytes(),
            })
            .unwrap_or_default()
    }
}

impl Reducer<DocumentHash, ()> for DocumentsIndexer {
    fn reduce<'a, Indexes, IndexesIter>(&self, _indexes: Indexes)
    where
        DocumentHash: 'a,
        Indexes: IntoIterator<Item = &'a DocumentHash, IntoIter = IndexesIter> + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a DocumentHash> + ExactSizeIterator + Clone,
    {
    }

    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter>(&self, _values: ReducedIndexes)
    where
        Self: 'a,
        (): 'a,
        ReducedIndexes:
            IntoIterator<Item = &'a (), IntoIter = ReducedIndexesIter> + ExactSizeIterator,
        ReducedIndexesIter: Iterator<Item = &'a ()> + ExactSizeIterator + Clone,
    {
    }
}

impl Serializable for DocumentHash {
    fn serialize_to<W: byteorder::WriteBytesExt>(
        &self,
        writer: &mut W,
    ) -> Result<usize, nebari::Error> {
        writer.write_u8(self.algorithm as u8)?;
        writer.write_all(&self.hash)?;
        Ok(self.hash.len() + 1)
    }

    fn deserialize_from<R: byteorder::ReadBytesExt>(reader: &mut R) -> Result<Self, nebari::Error> {
        let mut algorithm = [0; 1];
        reader.read_exact(&mut algorithm)?;
        let algorithm = DocumentHashAlgorithm::try_from(algorithm[0])
            .map_err(|err| nebari::Error::from(std::io::Error::new(ErrorKind::Other, err)))?;

        let mut hash = [0; 32];
        reader.read_exact(&mut hash)?;

        Ok(Self { algorithm, hash })
    }
}
