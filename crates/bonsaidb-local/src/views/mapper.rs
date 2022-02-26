use std::{
    borrow::Cow,
    collections::{hash_map::RandomState, BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::{serde::Bytes, ArcBytes, OwnedBytes},
    connection::Connection,
    schema::{
        view::{self, map, Serialized},
        CollectionName, ViewName,
    },
};
use easy_parallel::Parallel;
use nebari::{
    io::any::AnyFile,
    tree::{AnyTreeRoot, CompareSwap, KeyOperation, Operation, Unversioned, Versioned},
    ExecutingTransaction, Tree,
};

use crate::{
    database::{deserialize_document, document_tree_name, Database},
    tasks::{Job, Keyed, Task},
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name, EntryMapping, ViewEntry,
    },
    Error,
};

#[derive(Debug)]
pub struct Mapper {
    pub database: Database,
    pub map: Map,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct Map {
    pub database: Arc<Cow<'static, str>>,
    pub collection: CollectionName,
    pub view_name: ViewName,
}

#[async_trait]
impl Job for Mapper {
    type Output = u64;
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    #[allow(clippy::too_many_lines)]
    async fn execute(&mut self) -> Result<Self::Output, Error> {
        let documents =
            self.database
                .roots()
                .tree(self.database.collection_tree::<Versioned, _>(
                    &self.map.collection,
                    document_tree_name(&self.map.collection),
                )?)?;

        let view_entries =
            self.database
                .roots()
                .tree(self.database.collection_tree::<Unversioned, _>(
                    &self.map.collection,
                    view_entries_tree_name(&self.map.view_name),
                )?)?;

        let document_map =
            self.database
                .roots()
                .tree(self.database.collection_tree::<Unversioned, _>(
                    &self.map.collection,
                    view_document_map_tree_name(&self.map.view_name),
                )?)?;

        let invalidated_entries =
            self.database
                .roots()
                .tree(self.database.collection_tree::<Unversioned, _>(
                    &self.map.collection,
                    view_invalidated_docs_tree_name(&self.map.view_name),
                )?)?;

        let omitted_entries =
            self.database
                .roots()
                .tree(self.database.collection_tree::<Unversioned, _>(
                    &self.map.collection,
                    view_omitted_docs_tree_name(&self.map.view_name),
                )?)?;
        let transaction_id = self
            .database
            .last_transaction_id()
            .await?
            .expect("no way to have documents without a transaction");

        let storage = self.database.clone();
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

        self.database
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

fn map_view(
    invalidated_entries: &Tree<Unversioned, AnyFile>,
    document_map: &Tree<Unversioned, AnyFile>,
    documents: &Tree<Versioned, AnyFile>,
    omitted_entries: &Tree<Unversioned, AnyFile>,
    view_entries: &Tree<Unversioned, AnyFile>,
    database: &Database,
    map_request: &Map,
) -> Result<(), Error> {
    const CHUNK_SIZE: usize = 100_000;
    // Only do any work if there are invalidated documents to process
    let mut invalidated_ids = invalidated_entries
        .get_range(&(..))?
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    while !invalidated_ids.is_empty() {
        let mut transaction = database
            .roots()
            .transaction::<_, dyn AnyTreeRoot<AnyFile>>(&[
                Box::new(invalidated_entries.clone()) as Box<dyn AnyTreeRoot<AnyFile>>,
                Box::new(document_map.clone()),
                Box::new(documents.clone()),
                Box::new(omitted_entries.clone()),
                Box::new(view_entries.clone()),
            ])?;
        let view = database
            .data
            .schema
            .view_by_name(&map_request.view_name)
            .unwrap();

        // todo reuse these vecs
        let document_ids = invalidated_ids
            .drain(invalidated_ids.len().saturating_sub(CHUNK_SIZE)..)
            .collect::<Vec<_>>();

        DocumentRequest {
            document_ids: document_ids.clone(),
            map_request,
            database,
            transaction: &mut transaction,
            document_map_index: 1,
            documents_index: 2,
            omitted_entries_index: 3,
            view_entries_index: 4,
            view,
        }
        .map()?;

        let invalidated_entries = transaction.tree::<Unversioned>(0).unwrap();
        invalidated_entries.modify(document_ids, nebari::tree::Operation::Remove)?;
        transaction.commit()?;
    }

    Ok(())
}

pub struct DocumentRequest<'a> {
    pub document_ids: Vec<ArcBytes<'static>>,
    pub map_request: &'a Map,
    pub database: &'a Database,

    pub transaction: &'a mut ExecutingTransaction<AnyFile>,
    pub document_map_index: usize,
    pub documents_index: usize,
    pub omitted_entries_index: usize,
    pub view_entries_index: usize,
    pub view: &'a dyn Serialized,
}

impl<'a> DocumentRequest<'a> {
    #[allow(clippy::too_many_lines)]
    pub fn map(&mut self) -> Result<(), Error> {
        for chunk in self.document_ids.chunks(1024) {
            let (document_id_sender, document_id_receiver) = flume::bounded(chunk.len());
            let mut document_maps = BTreeMap::new();
            let mut document_keys = BTreeMap::new();
            let mut document_statuses = BTreeMap::new();
            let mut new_mappings = BTreeMap::new();
            let mut all_keys = BTreeSet::new();
            for result in Parallel::new()
                .add(|| {
                    let documents = self
                        .transaction
                        .tree::<Versioned>(self.documents_index)
                        .unwrap();
                    let mut documents =
                        documents.get_multiple(chunk.iter().map(ArcBytes::as_slice))?;
                    documents.sort_by(|a, b| a.0.cmp(&b.0));

                    for document_id in chunk.iter().rev() {
                        let document = documents
                            .last()
                            .map_or(false, |(key, _)| (key == document_id))
                            .then(|| documents.pop().unwrap().1);

                        document_id_sender
                            .send((document_id.clone(), document))
                            .unwrap();
                    }

                    drop(document_id_sender);
                    Ok(Vec::new())
                })
                .each(1..=16, |_| -> Result<_, Error> {
                    let mut results = Vec::new();
                    while let Ok((document_id, document)) = document_id_receiver.recv() {
                        let (document_is_present, map_result) = if let Some(document) = document {
                            let document = deserialize_document(&document)?;

                            // Call the schema map function
                            (
                                true,
                                self.view
                                    .map(&document)
                                    .map_err(bonsaidb_core::Error::from)?,
                            )
                        } else {
                            // Get multiple didn't return this document ID.
                            (false, Vec::new())
                        };
                        let keys: HashSet<OwnedBytes> = map_result
                            .iter()
                            .map(|map| OwnedBytes::from(map.key.as_slice()))
                            .collect();
                        let new_keys = ArcBytes::from(bincode::serialize(&keys)?);

                        results.push((
                            document_id,
                            document_is_present,
                            new_keys,
                            keys,
                            map_result,
                        ));
                    }
                    Ok(results)
                })
                .run()
            {
                for (document_id, document_is_present, new_keys, keys, map_result) in result? {
                    for key in &keys {
                        all_keys.insert(key.0.clone());
                    }
                    document_maps.insert(document_id.clone(), new_keys);
                    document_keys.insert(document_id.clone(), keys);
                    for mapping in map_result {
                        let key_mappings = new_mappings
                            .entry(ArcBytes::from(mapping.key.to_vec()))
                            .or_insert_with(Vec::default);
                        key_mappings.push(mapping);
                    }
                    document_statuses.insert(document_id, document_is_present);
                }
            }

            // We need to store a record of all the mappings this document produced.
            let document_map = self
                .transaction
                .tree::<Unversioned>(self.document_map_index)
                .unwrap();
            let mut maps_to_clear = Vec::new();
            document_map.modify(
                chunk.to_vec(),
                nebari::tree::Operation::CompareSwap(CompareSwap::new(&mut |key, value| {
                    if let Some(existing_map) = value {
                        maps_to_clear.push((key.to_owned(), existing_map));
                    }
                    let new_map = document_maps.get(key).unwrap();
                    KeyOperation::Set(new_map.clone())
                })),
            )?;
            let mut view_entries_to_clean = BTreeMap::new();
            for (document_id, existing_map) in maps_to_clear {
                let existing_keys = bincode::deserialize::<HashSet<OwnedBytes>>(&existing_map)?;
                let new_keys = document_keys.remove(&document_id).unwrap();
                for key in existing_keys.difference(&new_keys) {
                    all_keys.insert(key.clone().0);
                    let key_documents = view_entries_to_clean
                        .entry(key.clone().0)
                        .or_insert_with(HashSet::<_, RandomState>::default);
                    key_documents.insert(document_id.clone());
                }
            }

            let mut has_reduce = true;
            let mut error = None;
            self.transaction
                .tree::<Unversioned>(self.view_entries_index)
                .unwrap()
                .modify(
                    all_keys.into_iter().collect(),
                    Operation::CompareSwap(CompareSwap::new(&mut |key,
                                                                  view_entries: Option<
                        ArcBytes<'static>,
                    >| {
                        let mut view_entry = view_entries
                            .and_then(|view_entries| {
                                bincode::deserialize::<ViewEntry>(&view_entries).ok()
                            })
                            .unwrap_or_else(|| ViewEntry {
                                key: Bytes::from(key.to_vec()),
                                view_version: self.view.version(),
                                mappings: vec![],
                                reduced_value: Bytes::default(),
                            });
                        let key = key.to_owned();
                        if let Some(document_ids) = view_entries_to_clean.remove(&key) {
                            view_entry
                                .mappings
                                .retain(|m| !document_ids.contains(m.source.id.as_ref()));

                            if view_entry.mappings.is_empty() {
                                return KeyOperation::Remove;
                            } else if has_reduce {
                                let mappings = view_entry
                                    .mappings
                                    .iter()
                                    .map(|m| (&key[..], m.value.as_slice()))
                                    .collect::<Vec<_>>();

                                match self.view.reduce(&mappings, false) {
                                    Ok(reduced) => {
                                        view_entry.reduced_value = Bytes::from(reduced);
                                    }
                                    Err(view::Error::Core(
                                        bonsaidb_core::Error::ReduceUnimplemented,
                                    )) => {
                                        has_reduce = false;
                                    }
                                    Err(other) => {
                                        error = Some(Error::from(other));
                                        return KeyOperation::Skip;
                                    }
                                }
                            }
                        }

                        if let Some(new_mappings) = new_mappings.remove(&key[..]) {
                            for map::Serialized { source, value, .. } in new_mappings {
                                // Before altering any data, verify that the key is unique if this is a unique view.
                                if self.view.unique()
                                    && !view_entry.mappings.is_empty()
                                    && view_entry.mappings[0].source.id != source.id
                                {
                                    error = Some(Error::Core(
                                        bonsaidb_core::Error::UniqueKeyViolation {
                                            view: self.map_request.view_name.clone(),
                                            conflicting_document: Box::new(source),
                                            existing_document: Box::new(
                                                view_entry.mappings[0].source.clone(),
                                            ),
                                        },
                                    ));
                                    return KeyOperation::Skip;
                                }
                                let entry_mapping = EntryMapping { source, value };

                                // attempt to update an existing
                                // entry for this document, if
                                // present
                                let mut found = false;
                                for mapping in &mut view_entry.mappings {
                                    if mapping.source.id == entry_mapping.source.id {
                                        found = true;
                                        mapping.value = entry_mapping.value.clone();
                                        break;
                                    }
                                }

                                // If an existing mapping wasn't
                                // found, add it
                                if !found {
                                    view_entry.mappings.push(entry_mapping);
                                }
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
                            if has_reduce {
                                let mappings = view_entry
                                    .mappings
                                    .iter()
                                    .map(|m| (key.as_slice(), m.value.as_slice()))
                                    .collect::<Vec<_>>();

                                match self.view.reduce(&mappings, false) {
                                    Ok(reduced) => {
                                        view_entry.reduced_value = Bytes::from(reduced);
                                    }
                                    Err(view::Error::Core(
                                        bonsaidb_core::Error::ReduceUnimplemented,
                                    )) => {
                                        has_reduce = false;
                                    }
                                    Err(other) => {
                                        error = Some(Error::from(other));
                                        return KeyOperation::Skip;
                                    }
                                }
                            }
                        }

                        let value = bincode::serialize(&view_entry).unwrap();
                        KeyOperation::Set(ArcBytes::from(value))
                    })),
                )?;

            // TODO allow CompareSwap to return an AbortError
            if let Some(err) = error {
                return Err(err);
            }

            let (document_ids, mut is_present): (Vec<_>, VecDeque<_>) =
                document_statuses.into_iter().unzip();
            self.transaction
                .tree::<Unversioned>(self.omitted_entries_index)
                .unwrap()
                .modify(
                    document_ids,
                    Operation::CompareSwap(CompareSwap::new(&mut |_,
                                                                  value: Option<
                        ArcBytes<'static>,
                    >| {
                        let is_present = is_present.pop_front().unwrap();
                        if is_present {
                            KeyOperation::Remove
                        } else if value.is_some() {
                            KeyOperation::Skip
                        } else {
                            KeyOperation::Set(ArcBytes::default())
                        }
                    })),
                )?;
        }

        Ok(())
    }
}

impl Keyed<Task> for Mapper {
    fn key(&self) -> Task {
        Task::ViewMap(self.map.clone())
    }
}
