use std::borrow::Cow;

use async_trait::async_trait;
use pliantdb_core::{
    document::Document,
    schema::{collection, map, view, Key, Schema},
};
use pliantdb_jobs::{Job, Keyed};
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, TransactionalTree},
    IVec, Transactional, Tree,
};

use super::{
    view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
    view_omitted_docs_tree_name, EntryMapping, Task, ViewEntry,
};
use crate::storage::{document_tree_name, Storage};

#[derive(Debug)]
pub struct Mapper<DB> {
    pub storage: Storage<DB>,
    pub map: Map,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct Map {
    pub collection: collection::Id,
    pub view_name: Cow<'static, str>,
}

#[async_trait]
impl<DB> Job for Mapper<DB>
where
    DB: Schema,
{
    type Output = u64;

    #[allow(clippy::clippy::too_many_lines)]
    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let documents = self
            .storage
            .sled
            .open_tree(document_tree_name(&self.map.collection))?;

        let view_entries = self.storage.sled.open_tree(view_entries_tree_name(
            &self.map.collection,
            &self.map.view_name,
        ))?;

        let document_map = self.storage.sled.open_tree(view_document_map_tree_name(
            &self.map.collection,
            &self.map.view_name,
        ))?;

        let invalidated_entries = self
            .storage
            .sled
            .open_tree(view_invalidated_docs_tree_name(
                &self.map.collection,
                &self.map.view_name,
            ))?;

        let omitted_entries = self.storage.sled.open_tree(view_omitted_docs_tree_name(
            &self.map.collection,
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
            .tasks
            .mark_view_updated(
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
    storage: &Storage<DB>,
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
                    for document_id in &invalidated_ids {
                        DocumentRequest {
                            document_id,
                            map_request,
                            invalidated_entries,
                            document_map,
                            documents,
                            omitted_entries,
                            view_entries,
                            storage,
                        }
                        .map()?;
                    }
                    Ok(())
                },
            )
            .map_err(|err| match err {
                TransactionError::Abort(err) => err,
                TransactionError::Storage(err) => anyhow::Error::from(err),
            })?;
    }

    Ok(())
}

struct DocumentRequest<'a, DB> {
    document_id: &'a IVec,
    map_request: &'a Map,

    invalidated_entries: &'a TransactionalTree,
    document_map: &'a TransactionalTree,
    documents: &'a TransactionalTree,
    omitted_entries: &'a TransactionalTree,
    view_entries: &'a TransactionalTree,
    storage: &'a Storage<DB>,
}

impl<'a, DB: Schema> DocumentRequest<'a, DB> {
    fn map(&self) -> Result<(), ConflictableTransactionError<anyhow::Error>> {
        self.invalidated_entries.remove(self.document_id)?;

        let view = self
            .storage
            .schema
            .view_by_name(&self.map_request.view_name)
            .unwrap();

        let (doc_still_exists, map_result) =
            if let Some(document) = self.documents.get(self.document_id)? {
                let document =
                    bincode::deserialize::<Document<'_>>(&document).map_to_transaction_error()?;

                // Call the schema map function
                (true, view.map(&document).map_to_transaction_error()?)
            } else {
                (false, None)
            };

        if let Some(map::Serialized { source, key, value }) = map_result {
            self.omitted_entries.remove(self.document_id)?;

            // When map results are returned, the document
            // map will contain an array of keys that the
            // document returned. Currently we only support
            // single emits, so it's going to be a
            // single-entry vec for now.
            let keys: Vec<Cow<'_, [u8]>> = vec![Cow::Borrowed(&key)];
            if let Some(existing_map) = self.document_map.insert(
                self.document_id,
                bincode::serialize(&keys).map_to_transaction_error()?,
            )? {
                remove_existing_view_entries_for_keys(
                    self.document_id,
                    &keys,
                    &existing_map,
                    self.view_entries,
                    view,
                )?;
            }

            let entry_mapping = EntryMapping { source, value };

            // Add a new ViewEntry or update an existing
            // ViewEntry for the key given
            let view_entry = if let Some(existing_entry) = self.view_entries.get(&key)? {
                let mut entry = bincode::deserialize::<ViewEntry>(&existing_entry)
                    .map_to_transaction_error()?;

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
                    .map(|m| (key.as_slice(), m.value.as_slice()))
                    .collect::<Vec<_>>();
                entry.reduced_value = view.reduce(&mappings, false).map_to_transaction_error()?;

                entry
            } else {
                let reduced_value = view
                    .reduce(&[(&key, &entry_mapping.value)], false)
                    .map_to_transaction_error()?;
                ViewEntry {
                    view_version: view.version(),
                    mappings: vec![entry_mapping],
                    reduced_value,
                }
            };
            self.view_entries.insert(
                key,
                bincode::serialize(&view_entry).map_to_transaction_error()?,
            )?;
        } else {
            // When no entry is emitted, the document map is emptied and a note is made in omitted_entries
            if let Some(existing_map) = self.document_map.remove(self.document_id)? {
                remove_existing_view_entries_for_keys(
                    self.document_id,
                    &[],
                    &existing_map,
                    self.view_entries,
                    view,
                )?;
            }

            if doc_still_exists {
                self.omitted_entries
                    .insert(self.document_id, IVec::default())?;
            }
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

fn remove_existing_view_entries_for_keys(
    document_id: &[u8],
    keys: &[Cow<'_, [u8]>],
    existing_map: &[u8],
    view_entries: &TransactionalTree,
    view: &dyn view::Serialized,
) -> Result<(), ConflictableTransactionError<anyhow::Error>> {
    let existing_keys =
        bincode::deserialize::<Vec<Cow<'_, [u8]>>>(existing_map).map_to_transaction_error()?;
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
    if let Some(existing_entry) = view_entries.get(&existing_keys[0])? {
        let mut entry =
            bincode::deserialize::<ViewEntry>(&existing_entry).map_to_transaction_error()?;
        let document_id = u64::from_big_endian_bytes(document_id).unwrap();
        entry.mappings.retain(|m| m.source != document_id);
        let mappings = entry
            .mappings
            .iter()
            .map(|m| (existing_keys[0].as_ref(), m.value.as_slice()))
            .collect::<Vec<_>>();
        entry.reduced_value = view.reduce(&mappings, false).map_to_transaction_error()?;
        view_entries.insert(
            existing_keys[0].as_ref(),
            bincode::serialize(&entry).map_to_transaction_error()?,
        )?;
    }
    Ok(())
}
