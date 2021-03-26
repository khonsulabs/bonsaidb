use std::borrow::Cow;

use async_trait::async_trait;
use pliantdb_core::{
    document::Document,
    schema::{collection, map, Database},
};
use pliantdb_jobs::Job;

use crate::{storage::document_tree_name, Storage};

use super::{
    view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
    view_omitted_docs_tree_name, EntryMapping, ViewEntry,
};
use sled::{transaction::ConflictableTransactionError, IVec, Transactional};

#[derive(Debug)]
pub struct Mapper<DB> {
    storage: Storage<DB>,
    map: Map,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct Map {
    collection: collection::Id,
    view_name: Cow<'static, str>,
}

#[async_trait]
impl<DB> Job for Mapper<DB>
where
    DB: Database,
{
    type Output = u64;

    #[allow(clippy::clippy::too_many_lines)] // I'm well aware clippy, thank you for turning my entire screen yellow
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

        tokio::task::spawn_blocking::<_, anyhow::Result<u64>>(move || {
            // TODO refactor into its own method
            // Only do any work if there are invalidated documents to process
            let invalidated_ids = invalidated_entries
                .iter()
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|(key, _)| key)
                .collect::<Vec<_>>();
            if !invalidated_ids.is_empty() {
                (
                    &invalidated_entries,
                    &document_map,
                    &documents,
                    &omitted_entries,
                )
                    .transaction(
                        |(invalidated_entries, document_map, documents, omitted_entries)| {
                            for document_id in &invalidated_ids {
                                // TODO refactor into its own method
                                invalidated_entries.remove(document_id)?;

                                let document = documents.get(document_id)?.unwrap();
                                let document = bincode::deserialize::<Document<'_>>(&document)
                                    .map_err(|err| {
                                        ConflictableTransactionError::Abort(anyhow::Error::from(
                                            err,
                                        ))
                                    })?;

                                // Call the schema map function
                                let view =
                                    storage.schema.view_by_name(&map_request.view_name).unwrap();
                                let map_result = view.map(&document).map_err(|err| {
                                    ConflictableTransactionError::Abort(anyhow::Error::from(err))
                                })?;

                                if let Some(map::Serialized { source, key, value }) = map_result {
                                    omitted_entries.remove(document_id)?;

                                    // When map results are returned, the document
                                    // map will contain an array of keys that the
                                    // document returned. Currently we only support
                                    // single emits, so it's going to be a
                                    // single-entry vec for now.
                                    let keys = vec![Cow::Borrowed(&key)];
                                    document_map.insert(
                                        document_id,
                                        bincode::serialize(&keys).map_err(|err| {
                                            ConflictableTransactionError::Abort(
                                                anyhow::Error::from(err),
                                            )
                                        })?,
                                    )?;

                                    let entry_mapping = EntryMapping { source, value };

                                    // Add a new ViewEntry or update an existing
                                    // ViewEntry for the key given
                                    let view_entry =
                                        if let Some(existing_entry) = view_entries.get(&key)? {
                                            let mut entry =
                                                bincode::deserialize::<ViewEntry>(&existing_entry)
                                                    .map_err(|err| {
                                                        ConflictableTransactionError::Abort(
                                                            anyhow::Error::from(err),
                                                        )
                                                    })?;

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

                                            entry
                                        } else {
                                            ViewEntry {
                                                view_version: view.version(),
                                                mappings: vec![entry_mapping],
                                            }
                                        };
                                    view_entries.insert(
                                        key,
                                        bincode::serialize(&view_entry).map_err(|err| {
                                            ConflictableTransactionError::Abort(
                                                anyhow::Error::from(err),
                                            )
                                        })?,
                                    )?;
                                } else {
                                    // When no entry is emitted, the document map is emptied and a note is made in omitted_entries
                                    document_map.remove(document_id)?;
                                    omitted_entries.insert(document_id, IVec::default())?;
                                }
                            }
                            Ok(())
                        },
                    )
                    .map_err(|err| match err {
                        sled::transaction::TransactionError::Abort(err) => err,
                        sled::transaction::TransactionError::Storage(err) => {
                            anyhow::Error::from(err)
                        }
                    })?;
            }

            Ok(transaction_id)
        })
        .await?
    }
}