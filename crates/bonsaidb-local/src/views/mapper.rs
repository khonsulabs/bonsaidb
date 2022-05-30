use std::{
    borrow::Cow,
    collections::{hash_map::RandomState, BTreeMap, BTreeSet, HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
};

use bonsaidb_core::{
    arc_bytes::{serde::CowBytes, ArcBytes},
    document::{BorrowedDocument, DocumentId, Header, Revision},
    schema::{
        view::{map, Serialized},
        CollectionName, ViewName,
    },
};
use nebari::{
    io::any::AnyFile,
    tree::{
        btree::KeyOperation, AnyTreeRoot, ByIdIndexer, CompareSwap, KeyValue, Modification,
        Operation, PersistenceMode, ScanEvaluation, SequenceId,
    },
    Tree,
};
use rayon::prelude::*;

use super::EntryMappings;
use crate::{
    database::{document_tree_name, Database, DocumentsTree},
    tasks::{Job, Keyed, Task},
    views::{view_entries_tree_name, EntryMapping, ViewEntries, ViewEntriesTree, ViewIndexer},
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
    type Output = Option<SequenceId>;
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all))]
    #[allow(clippy::too_many_lines)]
    fn execute(&mut self) -> Result<Self::Output, Error> {
        let documents =
            self.database
                .roots()
                .tree(self.database.collection_tree::<DocumentsTree, _>(
                    &self.map.collection,
                    document_tree_name(&self.map.collection),
                )?)?;

        let view_entries = self.database.roots().tree(
            self.database
                .collection_tree_with_reducer::<ViewEntries, _>(
                    &self.map.collection,
                    view_entries_tree_name(&self.map.view_name),
                    ByIdIndexer(ViewIndexer::new(
                        self.database
                            .schematic()
                            .view_by_name(&self.map.view_name)?
                            .clone(),
                    )),
                )?,
        )?;

        let storage = self.database.clone();
        let map_request = self.map.clone();
        let sequence_id = map_view(&documents, &view_entries, &storage, &map_request)?;

        Ok(sequence_id)
    }
}

fn map_view(
    documents: &Tree<DocumentsTree, AnyFile>,
    view_entries: &Tree<ViewEntries, AnyFile>,
    database: &Database,
    map_request: &Map,
) -> Result<Option<SequenceId>, Error> {
    const CHUNK_SIZE: usize = 100_000;

    let starting_sequence = if let Some(index) = view_entries.reduce(&(..))? {
        index
            .embedded
            .latest_sequence
            .next_sequence()
            .expect("u64 should never wrap?") // TODO this is lazy
    } else {
        SequenceId::default()
    };

    // We send unique sequence ids for revisions of documents that fit within
    // our current scan range. The mapper will request document data using the
    // sequence id, ensuring that the mapping operation never maps a newer
    // sequence for a document, which would introduce an issue that we didn't
    // necessarily process all the sequences leading up to it.
    let mut document_ids = HashMap::<ArcBytes<'static>, SequenceId>::new();
    documents.scan_sequences::<Infallible, _, _, _>(
        starting_sequence..,
        true,
        |sequence| {
            let sequence_id = document_ids.entry(sequence.key).or_default();
            *sequence_id = (*sequence_id).max(sequence.sequence);
            ScanEvaluation::Skip
        },
        |_key, _value| unreachable!(),
    )?;
    let mut sequence_ids = document_ids.into_iter().map(|(_, v)| v).collect::<Vec<_>>();
    sequence_ids.sort_unstable();
    let last_sequence = sequence_ids.last().copied();

    let view = database
        .data
        .schema
        .view_by_name(&map_request.view_name)
        .unwrap();
    let mut documents = documents.open_for_read()?;

    while !sequence_ids.is_empty() {
        let sequence_ids = sequence_ids.split_off(sequence_ids.len().saturating_sub(CHUNK_SIZE));
        if view.eager() {
            let tx = database
                .roots()
                .transaction::<_, dyn AnyTreeRoot<AnyFile>>(&[
                    Box::new(view_entries.clone()) as Box<dyn AnyTreeRoot<AnyFile>>
                ])?;
            let mut view_entries = tx.tree(0).unwrap();
            DocumentRequest {
                sequence_ids,
                map_request,
                database,
                documents: &mut documents,
                view_entries: &mut view_entries.tree,
                view,
                persistence_mode: PersistenceMode::Transactional(tx.entry().id),
            }
            .map()?;
        } else {
            let mut view_entries = view_entries.open_for_write()?;
            DocumentRequest {
                sequence_ids,
                map_request,
                database,
                documents: &mut documents,
                view_entries: &mut view_entries,
                view,
                persistence_mode: PersistenceMode::Flush,
            }
            .map()?;
        }
    }

    Ok(last_sequence)
}

pub struct DocumentRequest<'a> {
    pub sequence_ids: Vec<SequenceId>,
    pub map_request: &'a Map,
    pub database: &'a Database,

    pub documents: &'a mut nebari::tree::TreeFile<DocumentsTree, AnyFile>,
    pub view_entries: &'a mut nebari::tree::TreeFile<ViewEntries, AnyFile>,
    pub view: &'a Arc<dyn Serialized>,
    pub persistence_mode: PersistenceMode,
}

impl<'a> DocumentRequest<'a> {
    fn generate_batches(
        document_ids: &[SequenceId],
        documents: &mut nebari::tree::TreeFile<DocumentsTree, AnyFile>,
        persistence_mode: PersistenceMode,
        mapped_sender: flume::Sender<Batch>,
        view: &Arc<dyn Serialized>,
    ) -> Result<(), Error> {
        // Generate batches
        for chunk in document_ids.chunks(1_000) {
            let mut documents = documents.get_multiple_with_indexes_by_sequence(
                chunk.iter().copied(),
                persistence_mode.transaction_id().is_some(),
            )?;

            let batch = chunk
                .iter()
                .rev()
                .filter_map(|sequence| documents.remove(sequence).map(|entry| (sequence, entry)))
                .collect::<Vec<_>>();

            let map_results = batch
                .into_par_iter()
                .map(|(sequence, entry)| {
                    let map_result = if let Some(contents) = entry.value {
                        let document = BorrowedDocument {
                            header: Header {
                                id: DocumentId::try_from(entry.index.key.as_slice()).unwrap(),
                                revision: Revision {
                                    id: sequence.0,
                                    sha256: entry.index.embedded.unwrap().hash,
                                },
                            },
                            contents: CowBytes::from(contents.as_slice()),
                        };
                        view.map(&document).map_err(bonsaidb_core::Error::from)
                    } else {
                        Ok(Vec::new())
                    };

                    map_result.map(|mappings| {
                        let keys: HashSet<ArcBytes<'static>> = mappings
                            .iter()
                            .map(|map| ArcBytes::from(map.key.to_vec()))
                            .collect();
                        (entry.index.key.clone(), keys, mappings)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let mut batch = Batch::default();
            for (document_id, keys, map_result) in map_results.into_iter() {
                for key in &keys {
                    batch.all_keys.insert(key.clone());
                }
                batch.document_keys.insert(document_id.clone(), keys);
                batch.document_ids.push(document_id);
                for mapping in map_result {
                    batch.last_sequence = batch
                        .last_sequence
                        .max(SequenceId(mapping.source.revision.id));
                    let key_mappings = batch
                        .new_mappings
                        .entry(ArcBytes::from(mapping.key.to_vec()))
                        .or_insert_with(Vec::default);
                    key_mappings.push(mapping);
                }
            }
            mapped_sender.send(batch).unwrap();
        }
        Ok(())
    }

    fn find_entries_to_clean(
        document_ids: &[ArcBytes<'static>],
        view_entries: &mut nebari::tree::TreeFile<ViewEntries, AnyFile>,
        mut document_keys: BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>,
        all_keys: &mut BTreeSet<ArcBytes<'static>>,
    ) -> Result<BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>, Error> {
        let mut view_entries_to_clean = BTreeMap::new();
        for KeyValue {
            key: document_id,
            value: existing_keys,
        } in
            view_entries.keys_for_documents(document_ids.iter().map(ArcBytes::as_slice), true)?
        {
            let new_keys = document_keys.remove(&document_id).unwrap();
            for key in existing_keys.difference(&new_keys) {
                all_keys.insert(key.clone());
                let key_documents = view_entries_to_clean
                    .entry(key.clone())
                    .or_insert_with(HashSet::<_, RandomState>::default);
                key_documents.insert(document_id.clone());
            }
        }
        Ok(view_entries_to_clean)
    }

    fn update_view_entries(
        view: &Arc<dyn Serialized>,
        map_request: &Map,
        view_entries: &mut nebari::tree::TreeFile<ViewEntries, AnyFile>,
        all_keys: BTreeSet<ArcBytes<'static>>,
        view_entries_to_clean: BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>,
        new_mappings: BTreeMap<ArcBytes<'static>, Vec<map::Serialized>>,
        persistence_mode: PersistenceMode,
    ) -> Result<(), Error> {
        let mut updater = ViewEntryUpdater {
            view,
            map_request,
            view_entries_to_clean,
            new_mappings,
            result: Ok(()),
        };
        view_entries
            .modify(Modification {
                keys: all_keys.into_iter().collect(),
                persistence_mode,
                operation: Operation::CompareSwap(CompareSwap::new(&mut |key, _index, value| {
                    updater.compare_swap_view_entry(key, value)
                })),
            })
            .map_err(Error::from)
            .and(updater.result)
    }

    fn save_mappings(
        mapped_receiver: &flume::Receiver<Batch>,
        view: &Arc<dyn Serialized>,
        map_request: &Map,
        view_entries: &mut nebari::tree::TreeFile<ViewEntries, AnyFile>,
        persistence_mode: PersistenceMode,
    ) -> Result<Option<SequenceId>, Error> {
        let mut sequence = None;
        while let Ok(Batch {
            mut document_ids,
            document_keys,
            new_mappings,
            mut all_keys,
            last_sequence,
        }) = mapped_receiver.recv()
        {
            sequence = Some(last_sequence);
            document_ids.sort_unstable();
            let view_entries_to_clean = Self::find_entries_to_clean(
                &document_ids,
                view_entries,
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
                persistence_mode,
            )?;
        }
        Ok(sequence)
    }

    pub fn map(&mut self) -> Result<Option<SequenceId>, Error> {
        let (mapped_sender, mapped_receiver) = flume::bounded(1);

        let mut latest_sequence_id = Ok(None);
        let mut generate_batches_result = Ok(());
        rayon::scope(|scope| {
            scope.spawn(|_| {
                generate_batches_result = Self::generate_batches(
                    &self.sequence_ids,
                    self.documents,
                    self.persistence_mode,
                    mapped_sender,
                    self.view,
                );
            });
            scope.spawn(|_| {
                latest_sequence_id = Self::save_mappings(
                    &mapped_receiver,
                    self.view,
                    self.map_request,
                    self.view_entries,
                    self.persistence_mode,
                );
            });
        });
        generate_batches_result?;
        let latest_sequence_id = latest_sequence_id?;

        if let Some(sequence_id) = latest_sequence_id {
            self.database.storage.instance.tasks().mark_view_updated(
                self.database.data.name.clone(),
                self.map_request.view_name.clone(),
                sequence_id,
            );
        }

        Ok(latest_sequence_id)
    }
}

#[derive(Default)]
struct Batch {
    last_sequence: SequenceId,
    document_ids: Vec<ArcBytes<'static>>,
    document_keys: BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>,
    new_mappings: BTreeMap<ArcBytes<'static>, Vec<map::Serialized>>,
    all_keys: BTreeSet<ArcBytes<'static>>,
}

impl Keyed<Task> for Mapper {
    fn key(&self) -> Task {
        Task::ViewMap(self.map.clone())
    }
}

struct ViewEntryUpdater<'a> {
    view: &'a Arc<dyn Serialized>,
    map_request: &'a Map,
    view_entries_to_clean: BTreeMap<ArcBytes<'static>, HashSet<ArcBytes<'static>>>,
    new_mappings: BTreeMap<ArcBytes<'static>, Vec<map::Serialized>>,
    result: Result<(), Error>,
}

impl<'a> ViewEntryUpdater<'a> {
    fn compare_swap_view_entry(
        &mut self,
        key: &ArcBytes<'_>,
        view_entries: Option<EntryMappings>,
    ) -> KeyOperation<EntryMappings> {
        let mut view_entries = view_entries.unwrap_or_default();
        let key = key.to_owned();
        if let Some(document_ids) = self.view_entries_to_clean.remove(&key) {
            view_entries
                .mappings
                .retain(|m| !document_ids.contains(m.source.id.as_ref()));
            for document_id in document_ids {
                view_entries.documents.insert(document_id, false);
            }

            if view_entries.mappings.is_empty() && !self.new_mappings.contains_key(&key[..]) {
                return KeyOperation::Set(view_entries);
            }
        }

        if let Some(new_mappings) = self.new_mappings.remove(&key[..]) {
            for map::Serialized { source, value, .. } in new_mappings {
                // Before altering any data, verify that the key is unique if this is a unique view.
                if self.view.unique()
                    && !view_entries.mappings.is_empty()
                    && view_entries.mappings[0].source.id != source.id
                {
                    self.result = Err(Error::Core(bonsaidb_core::Error::UniqueKeyViolation {
                        view: self.map_request.view_name.clone(),
                        conflicting_document: Box::new(source),
                        existing_document: Box::new(view_entries.mappings[0].source.clone()),
                    }));
                    return KeyOperation::Skip;
                }
                let entry_mapping = EntryMapping { source, value };

                // attempt to update an existing
                // entry for this document, if
                // present
                let mut found = false;
                for mapping in &mut view_entries.mappings {
                    if mapping.source.id == entry_mapping.source.id {
                        found = true;
                        mapping.source.revision = entry_mapping.source.revision;
                        mapping.value = entry_mapping.value.clone();
                        break;
                    }
                }

                // If an existing mapping wasn't
                // found, add it
                if !found {
                    view_entries
                        .documents
                        .insert(ArcBytes::from(entry_mapping.source.id.to_vec()), true);
                    view_entries.mappings.push(entry_mapping);
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
            // if self.has_reduce {
            //     let mappings =
            //         mappings
            //         .iter()
            //         .map(|m| (key.as_slice(), m.value.as_slice()))
            //         .collect::<Vec<_>>();

            //     match self.view.reduce(&mappings, false) {
            //         Ok(reduced) => {
            //             view_entry.reduced_value = Bytes::from(reduced);
            //         }
            //         Err(view::Error::Core(bonsaidb_core::Error::ReduceUnimplemented)) => {
            //             self.has_reduce = false;
            //         }
            //         Err(other) => {
            //             self.result = Err(Error::from(other));
            //             return KeyOperation::Skip;
            //         }
            //     }
            // }
        }

        KeyOperation::Set(view_entries)
    }
}
