use std::{
    borrow::Cow,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use async_trait::async_trait;
use bonsaidb_core::{
    connection::Connection,
    document::KeyId,
    permissions::Permissions,
    schema::{
        view::{map, Serialized},
        CollectionName, Key, Schema, ViewName,
    },
};
use bonsaidb_jobs::{Job, Keyed};
use serde::{Deserialize, Serialize};
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, TransactionalTree},
    IVec, Transactional, Tree,
};

use crate::{
    database::{document_tree_name, Database},
    vault::Vault,
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name, EntryMapping, Task, ViewEntry,
    },
    Error,
};

#[derive(Debug)]
pub struct Mapper<DB> {
    pub storage: Database<DB>,
    pub map: Map,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct Map {
    pub database: Arc<Cow<'static, str>>,
    pub collection: CollectionName,
    pub view_name: ViewName,
}

#[async_trait]
impl<DB> Job for Mapper<DB>
where
    DB: Schema,
{
    type Output = u64;

    #[allow(clippy::too_many_lines)]
    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let documents = self
            .storage
            .data
            .storage
            .roots()
            .open_tree(document_tree_name(
                &self.storage.data.name,
                &self.map.collection,
            ))?;

        let view_entries = self
            .storage
            .data
            .storage
            .roots()
            .open_tree(view_entries_tree_name(
                &self.storage.data.name,
                &self.map.view_name,
            ))?;

        let document_map =
            self.storage
                .data
                .storage
                .roots()
                .open_tree(view_document_map_tree_name(
                    &self.storage.data.name,
                    &self.map.view_name,
                ))?;

        let invalidated_entries =
            self.storage
                .data
                .storage
                .roots()
                .open_tree(view_invalidated_docs_tree_name(
                    &self.storage.data.name,
                    &self.map.view_name,
                ))?;

        let omitted_entries =
            self.storage
                .data
                .storage
                .roots()
                .open_tree(view_omitted_docs_tree_name(
                    &self.storage.data.name,
                    &self.map.view_name,
                ))?;
        let transaction_id = self
            .storage
            .last_transaction_id()
            .await?
            .expect("no way to have documents without a transaction");

        let storage = self.storage.clone();
        let map_request = self.map.clone();

        tokio::task::spawn_blocking(move || {
            map_view(
                &invalidated_entries,
                &document_map,
                &documents,
                &omitted_entries,
                &view_entries,
                &storage,
                &map_request,
            )
        })
        .await??;

        self.storage
            .data
            .storage
            .tasks()
            .mark_view_updated(
                self.map.database.clone(),
                self.map.collection.clone(),
                self.map.view_name.clone(),
                transaction_id,
            )
            .await;

        Ok(transaction_id)
    }
}

fn map_view<DB: Schema>(
    invalidated_entries: &Tree,
    document_map: &Tree,
    documents: &Tree,
    omitted_entries: &Tree,
    view_entries: &Tree,
    storage: &Database<DB>,
    map_request: &Map,
) -> anyhow::Result<()> {
    // Only do any work if there are invalidated documents to process
    let invalidated_ids = invalidated_entries
        .iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    if !invalidated_ids.is_empty() {
        (
            invalidated_entries,
            document_map,
            documents,
            omitted_entries,
            view_entries,
        )
            .transaction(
                |(invalidated_entries, document_map, documents, omitted_entries, view_entries)| {
                    let view = storage
                        .data
                        .schema
                        .view_by_name(&map_request.view_name)
                        .unwrap();
                    for document_id in &invalidated_ids {
                        DocumentRequest {
                            document_id,
                            map_request,
                            database: storage,
                            document_map,
                            documents,
                            omitted_entries,
                            view_entries,
                            view,
                        }
                        .map()?;
                        invalidated_entries.remove(document_id)?;
                    }
                    Ok(())
                },
            )
            .map_err(|err| match err {
                TransactionError::Abort(err) => err,
                TransactionError::Storage(err) => bonsaidb_core::Error::Database(err.to_string()),
            })?;
    }

    Ok(())
}

pub struct DocumentRequest<'a, DB> {
    pub document_id: &'a IVec,
    pub map_request: &'a Map,
    pub database: &'a Database<DB>,

    pub document_map: &'a TransactionalTree,
    pub documents: &'a TransactionalTree,
    pub omitted_entries: &'a TransactionalTree,
    pub view_entries: &'a TransactionalTree,
    pub view: &'a dyn Serialized,
}

impl<'a, DB: Schema> DocumentRequest<'a, DB> {
    pub fn map(&self) -> Result<(), ConflictableTransactionError<bonsaidb_core::Error>> {
        let (doc_still_exists, map_result) =
            if let Some(document) = self.documents.get(self.document_id)? {
                let document = self
                    .database
                    .deserialize_document(&document)
                    .map_to_transaction_error()?;

                // Call the schema map function
                (
                    true,
                    self.view
                        .map(&document)
                        .map_err(bonsaidb_core::Error::from)
                        .map_to_transaction_error()?,
                )
            } else {
                (false, None)
            };

        if let Some(map::Serialized { source, key, value }) = map_result {
            self.save_mapping(source, &key, value)?;
        } else {
            self.omit_document(doc_still_exists)?;
        }

        Ok(())
    }

    fn omit_document(
        &self,
        doc_still_exists: bool,
    ) -> Result<(), ConflictableTransactionError<bonsaidb_core::Error>> {
        // When no entry is emitted, the document map is emptied and a note is made in omitted_entries
        if let Some(existing_map) = self.document_map.remove(self.document_id)? {
            self.remove_existing_view_entries_for_keys(&[], &existing_map)?;
        }

        if doc_still_exists {
            self.omitted_entries
                .insert(self.document_id, IVec::default())?;
        }
        Ok(())
    }

    fn encryption_key(&self) -> Option<&KeyId> {
        self.database.view_encryption_key(self.view)
    }

    fn serialize_and_encrypt<S: Serialize>(
        &self,
        entry: &S,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        let mut bytes = bincode::serialize(&entry).map_err(Error::from)?;
        if let Some(key) = self.encryption_key() {
            bytes = self
                .database
                .storage()
                .vault()
                .encrypt_payload(
                    key,
                    &bytes,
                    self.database.data.effective_permissions.as_ref(),
                )
                .map_err(Error::from)?;
        }
        Ok(bytes)
    }

    fn load_entry_for_key(
        &self,
        key: &[u8],
    ) -> Result<Option<ViewEntryCollection>, ConflictableTransactionError<bonsaidb_core::Error>>
    {
        load_entry_for_key(
            key,
            self.view,
            self.encryption_key().is_some(),
            self.database.storage().vault(),
            self.database.data.effective_permissions.as_ref(),
            |key| {
                self.view_entries
                    .get(key)
                    .map_err(ConflictableTransactionError::from)
            },
        )
    }

    fn save_entry_for_key(
        &self,
        key: &[u8],
        entry: &ViewEntryCollection,
    ) -> Result<(), ConflictableTransactionError<bonsaidb_core::Error>> {
        let bytes = self
            .serialize_and_encrypt(entry)
            .map_to_transaction_error()?;
        if self.view.keys_are_encryptable() && self.encryption_key().is_some() {
            let hashed_key = hash_key(key);
            self.view_entries.insert(&hashed_key, bytes)?;
        } else {
            self.view_entries.insert(key, bytes)?;
        }
        Ok(())
    }

    fn save_mapping(
        &self,
        source: u64,
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), ConflictableTransactionError<bonsaidb_core::Error>> {
        // Before altering any data, verify that the key is unique if this is a unique view.
        if self.view.unique() {
            if let Some(existing_entry) = self.load_entry_for_key(key)? {
                if existing_entry.mappings[0].source != source {
                    return Err(bonsaidb_core::Error::UniqueKeyViolation {
                        view: self.map_request.view_name.clone(),
                        conflicting_document_id: source,
                        existing_document_id: existing_entry.mappings[0].source,
                    })
                    .map_to_transaction_error();
                }
            }
        }

        self.omitted_entries.remove(self.document_id)?;

        // When map results are returned, the document
        // map will contain an array of keys that the
        // document returned. Currently we only support
        // single emits, so it's going to be a
        // single-entry vec for now.
        let keys: Vec<Cow<'_, [u8]>> = vec![Cow::Borrowed(key)];
        if let Some(existing_map) = self.document_map.insert(
            self.document_id,
            self.serialize_and_encrypt(&keys)
                .map_to_transaction_error()?,
        )? {
            self.remove_existing_view_entries_for_keys(&keys, &existing_map)?;
        }

        let entry_mapping = EntryMapping { source, value };

        // Add a new ViewEntry or update an existing
        // ViewEntry for the key given
        let view_entry = if let Some(mut entry) = self.load_entry_for_key(key)? {
            // attempt to update an existing
            // entry for this document, if
            // present
            let mut found = false;
            for mapping in &mut entry.mappings {
                if mapping.source == source {
                    found = true;
                    mapping.value = entry_mapping.value.clone();
                    break;
                }
            }

            // If an existing mapping wasn't
            // found, add it
            if !found {
                entry.mappings.push(entry_mapping);
            }

            // There was a choice to be made here of whether to call
            // reduce()  with all of the existing values, or call it with
            // rereduce=true passing only the new value and the old stored
            // value. In this implementation, it's technically less
            // efficient, but we can guarantee that every value has only
            // been reduced once, and potentially re-reduced a single-time.
            // If we constantly try to update the value to optimize the size
            // of `mappings`, the fear is that the value computed may lose
            // precision in some contexts over time. Thus, the decision was
            // made to always call reduce() with all the mappings within a
            // single ViewEntry.
            let mappings = entry
                .mappings
                .iter()
                .map(|m| (key, m.value.as_slice()))
                .collect::<Vec<_>>();
            entry.reduced_value = self
                .view
                .reduce(&mappings, false)
                .map_err(bonsaidb_core::Error::from)
                .map_to_transaction_error()?;

            entry
        } else {
            let reduced_value = self
                .view
                .reduce(&[(key, &entry_mapping.value)], false)
                .map_err(bonsaidb_core::Error::from)
                .map_to_transaction_error()?;
            ViewEntryCollection::from(ViewEntry {
                key: key.to_vec(),
                view_version: self.view.version(),
                mappings: vec![entry_mapping],
                reduced_value,
            })
        };
        self.save_entry_for_key(key, &view_entry)?;
        Ok(())
    }

    fn remove_existing_view_entries_for_keys(
        &self,
        keys: &[Cow<'_, [u8]>],
        existing_map: &[u8],
    ) -> Result<(), ConflictableTransactionError<bonsaidb_core::Error>> {
        let existing_keys = self
            .database
            .storage()
            .vault()
            .decrypt_serialized::<Vec<Cow<'_, [u8]>>>(
                self.database.data.effective_permissions.as_ref(),
                existing_map,
            )
            .map_err(bonsaidb_core::Error::from)
            .map_to_transaction_error()?;
        if existing_keys == keys {
            // No change
            return Ok(());
        }

        assert_eq!(
            existing_keys.len(),
            1,
            "need to add support for multi-emitted keys"
        );
        // Remove the old key
        if let Some(mut entry_collection) = self.load_entry_for_key(&existing_keys[0])? {
            let document_id = u64::from_big_endian_bytes(self.document_id).unwrap();
            entry_collection
                .mappings
                .retain(|m| m.source != document_id);

            if entry_collection.mappings.is_empty() {
                entry_collection.remove_active_entry();
                if entry_collection.is_empty() {
                    // Remove the key
                    self.view_entries.remove(
                        entry_collection
                            .loaded_from
                            .as_ref()
                            .map_or(existing_keys[0].as_ref(), |key| &key[..]),
                    )?;
                    return Ok(());
                }
            } else {
                let mappings = entry_collection
                    .mappings
                    .iter()
                    .map(|m| (existing_keys[0].as_ref(), m.value.as_slice()))
                    .collect::<Vec<_>>();
                entry_collection.reduced_value = self
                    .view
                    .reduce(&mappings, false)
                    .map_err(bonsaidb_core::Error::from)
                    .map_to_transaction_error()?;
            }

            self.view_entries.insert(
                entry_collection
                    .loaded_from
                    .as_ref()
                    .map_or(existing_keys[0].as_ref(), |key| &key[..]),
                self.serialize_and_encrypt(&entry_collection)
                    .map_to_transaction_error()?,
            )?;
        }

        Ok(())
    }
}

impl<DB> Keyed<Task> for Mapper<DB>
where
    DB: Schema,
{
    fn key(&self) -> Task {
        Task::ViewMap(self.map.clone())
    }
}

trait ToTransactionResult<T, E> {
    fn map_to_transaction_error<RE: From<E>>(self) -> Result<T, ConflictableTransactionError<RE>>;
}

impl<T, E> ToTransactionResult<T, E> for Result<T, E> {
    fn map_to_transaction_error<RE: From<E>>(self) -> Result<T, ConflictableTransactionError<RE>> {
        self.map_err(|err| ConflictableTransactionError::Abort(RE::from(err)))
    }
}

fn hash_key(key: &[u8]) -> [u8; 32] {
    let res = blake3::hash(key);
    *res.as_bytes()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewEntryCollection {
    entries: Vec<ViewEntry>,
    #[serde(skip)]
    active_index: usize,
    #[serde(skip)]
    loaded_from: Option<[u8; 32]>,
}

impl From<ViewEntry> for ViewEntryCollection {
    fn from(entry: ViewEntry) -> Self {
        Self {
            entries: vec![entry],
            active_index: 0,
            loaded_from: None,
        }
    }
}

impl From<ViewEntryCollection> for ViewEntry {
    fn from(collection: ViewEntryCollection) -> Self {
        collection
            .entries
            .into_iter()
            .nth(collection.active_index)
            .unwrap()
    }
}

impl Deref for ViewEntryCollection {
    type Target = ViewEntry;

    fn deref(&self) -> &Self::Target {
        &self.entries[self.active_index]
    }
}

impl DerefMut for ViewEntryCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries[self.active_index]
    }
}

impl ViewEntryCollection {
    pub fn remove_active_entry(&mut self) {
        self.entries.remove(self.active_index);
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub(crate) fn load_entry_for_key<
    F: FnOnce(&[u8]) -> Result<Option<IVec>, ConflictableTransactionError<bonsaidb_core::Error>>,
>(
    key: &[u8],
    view: &dyn Serialized,
    encrypt_by_default: bool,
    vault: &Vault,
    permissions: Option<&Permissions>,
    get_entry_fn: F,
) -> Result<Option<ViewEntryCollection>, ConflictableTransactionError<bonsaidb_core::Error>> {
    if view.keys_are_encryptable() && encrypt_by_default {
        // When we encrypt the keys, we need to be able to find them
        // reliably. We're using a hashing function to create buckets where
        // more than one ViewEntry can be stored. These payloads can be
        // encrypted using random nonces, and can contain the actual key.
        // Thus, if a hash collision occurs, the loop will find the correct
        // entry before returning.
        let key_hash = hash_key(key);
        if let Some(bytes) = get_entry_fn(&key_hash)? {
            let mut collection =
                deserialize_entry(vault, permissions, &bytes).map_to_transaction_error()?;
            collection.loaded_from = Some(key_hash);
            for (index, entry) in collection.entries.iter().enumerate() {
                if entry.key == key {
                    collection.active_index = index;
                    return Ok(Some(collection));
                }
            }
            // No key matched
            Ok(None)
        } else {
            Ok(None)
        }
    } else {
        // Without encryption, the keys are guaranteed to be unique.
        Ok(get_entry_fn(key)?
            .map(|bytes| deserialize_entry(vault, permissions, &bytes))
            .transpose()
            .map_to_transaction_error()?)
    }
}

fn deserialize_entry(
    vault: &Vault,
    permissions: Option<&Permissions>,
    bytes: &[u8],
) -> Result<ViewEntryCollection, bonsaidb_core::Error> {
    vault
        .decrypt_serialized(permissions, bytes)
        .map_err(bonsaidb_core::Error::from)
}
