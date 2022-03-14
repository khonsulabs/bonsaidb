use std::{
    borrow::Cow,
    collections::{hash_map::RandomState, BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

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
    LockedTransactionTree, Tree, UnlockedTransactionTree,
};

use crate::{
    database::{deserialize_document, document_tree_name, Database},
    tasks::{Job, Keyed, Task},
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        EntryMapping, ViewEntry,
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

impl Job for Mapper {
    type Output = u64;
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    #[allow(clippy::too_many_lines)]
    fn execute(&mut self) -> Result<Self::Output, Error> {
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

        let transaction_id = self
            .database
            .last_transaction_id()?
            .expect("no way to have documents without a transaction");

        let storage = self.database.clone();
        let map_request = self.map.clone();

        map_view(
            &invalidated_entries,
            &document_map,
            &documents,
            &view_entries,
            &storage,
            &map_request,
        )?;

        self.database.storage.instance.tasks().mark_view_updated(
            self.map.database.clone(),
            self.map.collection.clone(),
            self.map.view_name.clone(),
            transaction_id,
        );

        Ok(transaction_id)
    }
}

fn map_view(
    invalidated_entries: &Tree<Unversioned, AnyFile>,
    document_map: &Tree<Unversioned, AnyFile>,
    documents: &Tree<Versioned, AnyFile>,
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
        let transaction = database
            .roots()
            .transaction::<_, dyn AnyTreeRoot<AnyFile>>(&[
                Box::new(invalidated_entries.clone()) as Box<dyn AnyTreeRoot<AnyFile>>,
                Box::new(document_map.clone()),
                Box::new(documents.clone()),
                Box::new(view_entries.clone()),
            ])?;
        {
            let view = database
                .data
                .schema
                .view_by_name(&map_request.view_name)
                .unwrap();

            let document_ids = invalidated_ids
                .drain(invalidated_ids.len().saturating_sub(CHUNK_SIZE)..)
                .collect::<Vec<_>>();
            let document_map = transaction.unlocked_tree(1).unwrap();
            let documents = transaction.unlocked_tree(2).unwrap();
            let view_entries = transaction.unlocked_tree(3).unwrap();
            DocumentRequest {
                document_ids: document_ids.clone(),
                map_request,
                database,
                document_map,
                documents,
                view_entries,
                view,
            }
            .map()?;

            let mut invalidated_entries = transaction.tree::<Unversioned>(0).unwrap();
            invalidated_entries.modify(document_ids, nebari::tree::Operation::Remove)?;
        }
        transaction.commit()?;
    }

    Ok(())
}

pub struct DocumentRequest<'a> {
    pub document_ids: Vec<ArcBytes<'static>>,
    pub map_request: &'a Map,
    pub database: &'a Database,

    pub document_map: &'a UnlockedTransactionTree<AnyFile>,
    pub documents: &'a UnlockedTransactionTree<AnyFile>,
    pub view_entries: &'a UnlockedTransactionTree<AnyFile>,
    pub view: &'a dyn Serialized,
}

type DocumentIdPayload = (ArcBytes<'static>, Option<ArcBytes<'static>>);
type BatchPayload = (Vec<ArcBytes<'static>>, flume::Receiver<DocumentIdPayload>);

impl<'a> DocumentRequest<'a> {
    fn generate_batches(
        batch_sender: flume::Sender<BatchPayload>,
        document_ids: &[ArcBytes<'static>],
        documents: &UnlockedTransactionTree<AnyFile>,
    ) -> Result<(), Error> {
        // Generate batches
        let mut documents = documents.lock::<Versioned>();
        for chunk in document_ids.chunks(1024) {
            let (document_id_sender, document_id_receiver) = flume::bounded(chunk.len());
            batch_sender
                .send((chunk.to_vec(), document_id_receiver))
                .unwrap();
            let mut documents = documents.get_multiple(chunk.iter().map(ArcBytes::as_slice))?;
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
        }
        drop(batch_sender);
        Ok(())
    }

    fn map_batches(
        batch_receiver: &flume::Receiver<BatchPayload>,
        mapped_sender: flume::Sender<Batch>,
        view: &dyn Serialized,
        parallelization: usize,
    ) -> Result<(), Error> {
        // Process batches
        while let Ok((document_ids, document_id_receiver)) = batch_receiver.recv() {
            let mut batch = Batch {
                document_ids,
                ..Batch::default()
            };
            for result in Parallel::new()
                .each(1..=parallelization, |_| -> Result<_, Error> {
                    let mut results = Vec::new();
                    while let Ok((document_id, document)) = document_id_receiver.recv() {
                        let map_result = if let Some(document) = document {
                            let document = deserialize_document(&document)?;

                            // Call the schema map function
                            view.map(&document).map_err(bonsaidb_core::Error::from)?
                        } else {
                            // Get multiple didn't return this document ID.
                            Vec::new()
                        };
                        let keys: HashSet<OwnedBytes> = map_result
                            .iter()
                            .map(|map| OwnedBytes::from(map.key.as_slice()))
                            .collect();
                        let new_keys = ArcBytes::from(bincode::serialize(&keys)?);

                        results.push((document_id, new_keys, keys, map_result));
                    }

                    Ok(results)
                })
                .run()
            {
                for (document_id, new_keys, keys, map_result) in result? {
                    for key in &keys {
                        batch.all_keys.insert(key.0.clone());
                    }
                    batch.document_maps.insert(document_id.clone(), new_keys);
                    batch.document_keys.insert(document_id.clone(), keys);
                    for mapping in map_result {
                        let key_mappings = batch
                            .new_mappings
                            .entry(ArcBytes::from(mapping.key.to_vec()))
                            .or_insert_with(Vec::default);
                        key_mappings.push(mapping);
                    }
                }
            }
            mapped_sender.send(batch).unwrap();
        }
        drop(mapped_sender);
        Ok(())
    }

    fn update_document_map(
        document_ids: Vec<ArcBytes<'static>>,
        document_map: &mut LockedTransactionTree<'_, Unversioned, AnyFile>,
        document_maps: &BTreeMap<ArcBytes<'static>, ArcBytes<'static>>,
        mut document_keys: BTreeMap<ArcBytes<'static>, HashSet<OwnedBytes>>,
        all_keys: &mut BTreeSet<ArcBytes<'static>>,
    ) -> Result<BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>, Error> {
        // We need to store a record of all the mappings this document produced.
        let mut maps_to_clear = Vec::new();
        document_map.modify(
            document_ids,
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
        Ok(view_entries_to_clean)
    }

    fn update_view_entries(
        view: &dyn Serialized,
        map_request: &Map,
        view_entries: &mut LockedTransactionTree<'_, Unversioned, AnyFile>,
        all_keys: BTreeSet<ArcBytes<'static>>,
        view_entries_to_clean: BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>,
        new_mappings: BTreeMap<ArcBytes<'static>, Vec<map::Serialized>>,
    ) -> Result<(), Error> {
        let mut updater = ViewEntryUpdater {
            view,
            map_request,
            view_entries_to_clean,
            new_mappings,
            result: Ok(()),
            has_reduce: true,
        };
        view_entries
            .modify(
                all_keys.into_iter().collect(),
                Operation::CompareSwap(CompareSwap::new(&mut |key, view_entries| {
                    updater.compare_swap_view_entry(key, view_entries)
                })),
            )
            .map_err(Error::from)
            .and(updater.result)
    }

    fn save_mappings(
        mapped_receiver: &flume::Receiver<Batch>,
        view: &dyn Serialized,
        map_request: &Map,
        document_map: &mut LockedTransactionTree<'_, Unversioned, AnyFile>,
        view_entries: &mut LockedTransactionTree<'_, Unversioned, AnyFile>,
    ) -> Result<(), Error> {
        while let Ok(Batch {
            document_ids,
            document_maps,
            document_keys,
            new_mappings,
            mut all_keys,
        }) = mapped_receiver.recv()
        {
            let view_entries_to_clean = Self::update_document_map(
                document_ids,
                document_map,
                &document_maps,
                document_keys,
                &mut all_keys,
            )?;

            Self::update_view_entries(
                view,
                map_request,
                view_entries,
                all_keys,
                view_entries_to_clean,
                new_mappings,
            )?;
        }
        Ok(())
    }

    pub fn map(&mut self) -> Result<(), Error> {
        let (batch_sender, batch_receiver) = flume::bounded(1);
        let (mapped_sender, mapped_receiver) = flume::bounded(1);

        for result in Parallel::new()
            .add(|| Self::generate_batches(batch_sender, &self.document_ids, self.documents))
            .add(|| {
                Self::map_batches(
                    &batch_receiver,
                    mapped_sender,
                    self.view,
                    self.database.storage().parallelization(),
                )
            })
            .add(|| {
                let mut document_map = self.document_map.lock();
                let mut view_entries = self.view_entries.lock();
                Self::save_mappings(
                    &mapped_receiver,
                    self.view,
                    self.map_request,
                    &mut document_map,
                    &mut view_entries,
                )
            })
            .run()
        {
            result?;
        }

        Ok(())
    }
}

#[derive(Default)]
struct Batch {
    document_ids: Vec<ArcBytes<'static>>,
    document_maps: BTreeMap<ArcBytes<'static>, ArcBytes<'static>>,
    document_keys: BTreeMap<ArcBytes<'static>, HashSet<OwnedBytes>>,
    new_mappings: BTreeMap<ArcBytes<'static>, Vec<map::Serialized>>,
    all_keys: BTreeSet<ArcBytes<'static>>,
}

impl Keyed<Task> for Mapper {
    fn key(&self) -> Task {
        Task::ViewMap(self.map.clone())
    }
}

struct ViewEntryUpdater<'a> {
    view: &'a dyn Serialized,
    map_request: &'a Map,
    view_entries_to_clean: BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>,
    new_mappings: BTreeMap<ArcBytes<'static>, Vec<map::Serialized>>,
    result: Result<(), Error>,
    has_reduce: bool,
}

impl<'a> ViewEntryUpdater<'a> {
    fn compare_swap_view_entry(
        &mut self,
        key: &ArcBytes<'_>,
        view_entries: Option<ArcBytes<'static>>,
    ) -> KeyOperation<ArcBytes<'static>> {
        let mut view_entry = view_entries
            .and_then(|view_entries| bincode::deserialize::<ViewEntry>(&view_entries).ok())
            .unwrap_or_else(|| ViewEntry {
                key: Bytes::from(key.to_vec()),
                view_version: self.view.version(),
                mappings: vec![],
                reduced_value: Bytes::default(),
            });
        let key = key.to_owned();
        if let Some(document_ids) = self.view_entries_to_clean.remove(&key) {
            view_entry
                .mappings
                .retain(|m| !document_ids.contains(m.source.id.as_ref()));

            if view_entry.mappings.is_empty() {
                return KeyOperation::Remove;
            } else if self.has_reduce {
                let mappings = view_entry
                    .mappings
                    .iter()
                    .map(|m| (&key[..], m.value.as_slice()))
                    .collect::<Vec<_>>();

                match self.view.reduce(&mappings, false) {
                    Ok(reduced) => {
                        view_entry.reduced_value = Bytes::from(reduced);
                    }
                    Err(view::Error::Core(bonsaidb_core::Error::ReduceUnimplemented)) => {
                        self.has_reduce = false;
                    }
                    Err(other) => {
                        self.result = Err(Error::from(other));
                        return KeyOperation::Skip;
                    }
                }
            }
        }

        if let Some(new_mappings) = self.new_mappings.remove(&key[..]) {
            for map::Serialized { source, value, .. } in new_mappings {
                // Before altering any data, verify that the key is unique if this is a unique view.
                if self.view.unique()
                    && !view_entry.mappings.is_empty()
                    && view_entry.mappings[0].source.id != source.id
                {
                    self.result = Err(Error::Core(bonsaidb_core::Error::UniqueKeyViolation {
                        view: self.map_request.view_name.clone(),
                        conflicting_document: Box::new(source),
                        existing_document: Box::new(view_entry.mappings[0].source.clone()),
                    }));
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
            if self.has_reduce {
                let mappings = view_entry
                    .mappings
                    .iter()
                    .map(|m| (key.as_slice(), m.value.as_slice()))
                    .collect::<Vec<_>>();

                match self.view.reduce(&mappings, false) {
                    Ok(reduced) => {
                        view_entry.reduced_value = Bytes::from(reduced);
                    }
                    Err(view::Error::Core(bonsaidb_core::Error::ReduceUnimplemented)) => {
                        self.has_reduce = false;
                    }
                    Err(other) => {
                        self.result = Err(Error::from(other));
                        return KeyOperation::Skip;
                    }
                }
            }
        }

        let value = bincode::serialize(&view_entry).unwrap();
        KeyOperation::Set(ArcBytes::from(value))
    }
}
