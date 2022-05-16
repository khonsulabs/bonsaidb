use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
    io::{ErrorKind, Read, Write},
    mem::size_of,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    document::Header,
    schema::{view, CollectionName},
};
use byteorder::{BigEndian, ReadBytesExt};
use nebari::{
    transaction::TransactionId,
    tree::{
        btree::{
            BTreeEntry, BTreeNode, Indexer, KeyOperation, ModificationContext, Reducer, ScanArgs,
        },
        dynamic_order, BinarySerialization, ByIdIndexer, ByIdStats, ChangeResult, CompareSwap,
        EmbeddedIndex, Modification, ModificationResult, Operation, PagedWriter, PersistenceMode,
        Root, SequenceId, Serializable, UnversionedByIdIndex, PAGE_SIZE,
    },
    ArcBytes, CacheEntry,
};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct ViewEntry {
    pub key: Bytes,
    pub reduced_value: Bytes,
    pub mappings: Vec<EntryMapping>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntryMapping {
    pub source: Header,
    pub value: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct EntryIndex {
    pub value: Option<Vec<u8>>,
    pub latest_sequence: SequenceId,
}

impl EmbeddedIndex<EntryMappings> for EntryIndex {
    type Reduced = Self;
    type Indexer = ViewIndexer;
}

impl Indexer<EntryMappings, EntryIndex> for ViewIndexer {
    fn index(&self, _key: &ArcBytes<'_>, value: Option<&EntryMappings>) -> EntryIndex {
        let has_reduce = self.has_reduce.load(Ordering::Relaxed);
        if has_reduce {
            if let Some(value) = value {
                let mut latest_sequence_id = 0;
                let mappings = value
                    .mappings
                    .iter()
                    .map(|m| {
                        latest_sequence_id = latest_sequence_id.max(m.source.revision.id);
                        m.value.as_slice()
                    })
                    .collect::<Vec<_>>();

                return self.reduce_values(SequenceId(latest_sequence_id), &mappings);
            }
        }

        EntryIndex::default()
    }
}

impl Serializable for EntryIndex {
    fn serialize_to<W: byteorder::WriteBytesExt>(
        &self,
        writer: &mut W,
    ) -> Result<usize, nebari::Error> {
        if let Some(value) = self.value.as_ref() {
            // To distinguish between an empty value and an error, we're going to add one to the length
            let header_usize = value.len() + 1;
            match u32::try_from(header_usize) {
                Ok(header) => {
                    writer.write_all(&header.to_be_bytes())?;
                    writer.write_all(&self.latest_sequence.0.to_be_bytes())?;
                    writer.write_all(value)?;

                    Ok(value.len() + size_of::<u64>() + size_of::<u32>())
                }
                Err(err) => Err(nebari::Error::from(std::io::Error::new(
                    ErrorKind::Other,
                    err,
                ))),
            }
        } else {
            writer.write_all(&0_u32.to_be_bytes())?;
            writer.write_all(&self.latest_sequence.0.to_be_bytes())?;
            Ok(size_of::<u32>() + size_of::<u64>())
        }
    }

    fn deserialize_from<R: byteorder::ReadBytesExt>(reader: &mut R) -> Result<Self, nebari::Error> {
        let mut header_bytes = [0; 4];
        reader.read_exact(&mut header_bytes)?;

        let mut sequence_bytes = [0; 8];
        reader.read_exact(&mut sequence_bytes)?;
        let latest_sequence = SequenceId(u64::from_be_bytes(sequence_bytes));

        let value = match u32::from_be_bytes(header_bytes).checked_sub(1) {
            None => None,
            Some(value_length) => {
                let value_length = match usize::try_from(value_length) {
                    Ok(length) => length,
                    Err(err) => {
                        return Err(nebari::Error::from(std::io::Error::new(
                            ErrorKind::Other,
                            err,
                        )))
                    }
                };
                let mut bytes = vec![0; value_length];
                if value_length > 0 {
                    reader.read_exact(&mut bytes)?;
                }
                Some(bytes)
            }
        };

        Ok(Self {
            value,
            latest_sequence,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ViewIndexer {
    pub view: Arc<dyn view::Serialized>,
    has_reduce: Arc<AtomicBool>,
}

impl ViewIndexer {
    pub fn new(view: Arc<dyn view::Serialized>) -> Self {
        Self {
            view,
            has_reduce: Arc::new(AtomicBool::new(true)),
        }
    }

    fn reduce_values(&self, latest_sequence: SequenceId, values: &[&[u8]]) -> EntryIndex {
        let mut entry = EntryIndex {
            value: None,
            latest_sequence,
        };

        let has_reduce = self.has_reduce.load(Ordering::Relaxed);
        if has_reduce {
            match self.view.reduce(values, false) {
                Ok(reduced) => {
                    entry.value = Some(reduced);
                }
                Err(view::Error::Core(bonsaidb_core::Error::ReduceUnimplemented)) => {
                    self.has_reduce.store(false, Ordering::Relaxed);
                }
                Err(other) => {
                    // TODO handle error in view indexing... For now maybe store
                    // the error's Display.
                }
            };
        }

        entry
    }
}

impl Reducer<EntryIndex> for ViewIndexer {
    fn reduce<'a, Indexes, IndexesIter>(&self, indexes: Indexes) -> EntryIndex
    where
        EntryIndex: 'a,
        Indexes: IntoIterator<Item = &'a EntryIndex, IntoIter = IndexesIter> + ExactSizeIterator,
        IndexesIter: Iterator<Item = &'a EntryIndex> + ExactSizeIterator + Clone,
    {
        let mut latest_sequence_id = SequenceId::default();
        let values = indexes
            .into_iter()
            .filter_map(|m| {
                latest_sequence_id = latest_sequence_id.max(m.latest_sequence);
                m.value.as_deref()
            })
            .collect::<Vec<_>>();
        self.reduce_values(latest_sequence_id, &values)
    }

    fn rereduce<'a, ReducedIndexes, ReducedIndexesIter>(&self, values: ReducedIndexes) -> EntryIndex
    where
        Self: 'a,
        ReducedIndexes:
            IntoIterator<Item = &'a EntryIndex, IntoIter = ReducedIndexesIter> + ExactSizeIterator,
        ReducedIndexesIter: Iterator<Item = &'a EntryIndex> + ExactSizeIterator + Clone,
    {
        self.reduce(values)
    }
}

#[derive(Clone, Debug)]
pub struct ViewEntries {
    pub transaction_id: Option<TransactionId>,
    pub by_id_root:
        BTreeEntry<UnversionedByIdIndex<EntryIndex, EntryMappings>, ByIdStats<EntryIndex>>,
    pub by_source_root:
        BTreeEntry<UnversionedByIdIndex<(), HashSet<ArcBytes<'static>>>, ByIdStats<()>>,

    reducer: ByIdIndexer<ViewIndexer>,
}

impl ViewEntries {
    fn modify_id_root<'a, 'w>(
        &'a mut self,
        mut modification: Modification<
            '_,
            EntryMappings,
            UnversionedByIdIndex<EntryIndex, EntryMappings>,
        >,
        writer: &'a mut PagedWriter<'w>,
        max_order: Option<usize>,
    ) -> Result<
        (
            Vec<ModificationResult<UnversionedByIdIndex<EntryIndex, EntryMappings>>>,
            BTreeMap<ArcBytes<'static>, HashMap<ArcBytes<'static>, bool>>,
        ),
        nebari::Error,
    > {
        modification.prepare()?;

        let total_keys =
            self.by_id_root.stats(self.reducer()).total_keys() + modification.keys.len() as u64;
        let by_id_order = dynamic_order(total_keys, max_order);
        let minimum_children = by_id_order / 2 - 1;
        let minimum_children =
            minimum_children.min(usize::try_from(total_keys).unwrap_or(usize::MAX));

        let reducer = self.reducer.clone();

        let mut results = Vec::with_capacity(modification.keys.len());
        let mut document_map_changes = BTreeMap::new();

        while !modification.keys.is_empty() {
            match self.by_id_root.modify(
                &mut modification,
                &mut ModificationContext::new(
                    by_id_order,
                    minimum_children,
                    |key: &ArcBytes<'_>,
                     value: Option<&EntryMappings>,
                     _existing_index,
                     writer: &mut PagedWriter<'_>| {
                        if let Some(value) = value {
                            for (document_id, presence) in &value.documents {
                                let key_changes = document_map_changes
                                    .entry(document_id.clone())
                                    .or_insert_with(HashMap::new);
                                key_changes.insert(key.to_owned(), *presence);
                            }
                            if !value.mappings.is_empty() {
                                let serialized = ArcBytes::from(
                                    bincode::serialize(&value.mappings).map_err(|err| {
                                        nebari::Error::from(std::io::Error::new(
                                            ErrorKind::Other,
                                            err,
                                        ))
                                    })?,
                                );
                                let position = writer.write_chunk(&serialized)?;
                                // write_chunk errors if it can't fit within a u32
                                #[allow(clippy::cast_possible_truncation)]
                                let value_length = serialized.len() as u32;
                                let new_index = UnversionedByIdIndex::new(
                                    value_length,
                                    position,
                                    reducer.0.index(key, Some(value)),
                                );
                                results.push(ModificationResult {
                                    key: key.to_owned(),
                                    index: Some(new_index.clone()),
                                });

                                return Ok(KeyOperation::Set(new_index));
                            }
                        }

                        results.push(ModificationResult {
                            key: key.to_owned(),
                            index: None,
                        });
                        Ok(KeyOperation::Remove)
                    },
                    |index, writer| match writer.read_chunk(index.position)? {
                        CacheEntry::ArcBytes(buffer) => Ok(Some(EntryMappings {
                            mappings: bincode::deserialize(&buffer).map_err(|err| {
                                nebari::Error::from(std::io::Error::new(ErrorKind::Other, err))
                            })?,
                            documents: BTreeMap::default(),
                        })),
                        CacheEntry::Decoded(_) => unreachable!(),
                    },
                    self.reducer().clone(),
                ),
                None,
                writer,
            )? {
                ChangeResult::Absorb | ChangeResult::Changed | ChangeResult::Unchanged => {}
                ChangeResult::Remove => {
                    self.by_id_root.node = BTreeNode::Leaf(vec![]);
                    self.by_id_root.dirty = true;
                }
                ChangeResult::Split => {
                    self.by_id_root.split_root(&self.reducer().clone());
                }
            }
        }

        Ok((results, document_map_changes))
    }

    fn modify_document_map<'a, 'w>(
        &'a mut self,
        mut document_map_changes: BTreeMap<ArcBytes<'static>, HashMap<ArcBytes<'static>, bool>>,
        writer: &'a mut PagedWriter<'w>,
        max_order: Option<usize>,
        persistence_mode: PersistenceMode,
    ) -> Result<(), nebari::Error> {
        let keys = document_map_changes.keys().cloned().collect();
        let mut compare_swap =
            |document_id: &ArcBytes<'_>,
             _index: Option<&UnversionedByIdIndex<(), HashSet<ArcBytes<'static>>>>,
             value: Option<HashSet<ArcBytes<'static>>>| {
                let changes = document_map_changes.remove(document_id.as_slice()).unwrap();
                // TODO error handling
                let mut view_keys = value.unwrap_or_default();
                for (key, should_be_present) in changes {
                    if should_be_present {
                        view_keys.insert(key);
                    } else {
                        view_keys.remove(&key);
                    }
                }
                if view_keys.is_empty() {
                    KeyOperation::Remove
                } else {
                    KeyOperation::Set(view_keys)
                }
            };
        let mut modification = Modification {
            persistence_mode,
            keys,
            operation: Operation::CompareSwap(CompareSwap::new(&mut compare_swap)),
        };

        modification.prepare()?;

        let reducer = ByIdIndexer(());

        let total_keys =
            self.by_source_root.stats(&reducer).total_keys() + modification.keys.len() as u64;
        let by_source_order = dynamic_order(total_keys, max_order);
        let minimum_children = by_source_order / 2 - 1;
        let minimum_children =
            minimum_children.min(usize::try_from(total_keys).unwrap_or(usize::MAX));

        while !modification.keys.is_empty() {
            match self.by_source_root.modify(
                &mut modification,
                &mut ModificationContext::new(
                    by_source_order,
                    minimum_children,
                    |_key: &ArcBytes<'_>,
                     value: Option<&HashSet<ArcBytes<'static>>>,
                     _existing_index,
                     writer: &mut PagedWriter<'_>| {
                        if let Some(value) = value {
                            if !value.is_empty() {
                                let serialized = Self::serialize_document_map_entries(value);
                                let position = writer.write_chunk(&serialized)?;
                                // write_chunk errors if it can't fit within a u32
                                #[allow(clippy::cast_possible_truncation)]
                                let value_length = serialized.len() as u32;
                                let new_index =
                                    UnversionedByIdIndex::new(value_length, position, ());

                                return Ok(KeyOperation::Set(new_index));
                            }
                        }
                        Ok(KeyOperation::Remove)
                    },
                    |index, writer| match writer.read_chunk(index.position)? {
                        CacheEntry::ArcBytes(buffer) => {
                            Ok(Some(Self::deserialize_document_map_entries(buffer)?))
                        }
                        CacheEntry::Decoded(_) => unreachable!(),
                    },
                    reducer.clone(),
                ),
                None,
                writer,
            )? {
                ChangeResult::Absorb | ChangeResult::Changed | ChangeResult::Unchanged => {}
                ChangeResult::Remove => {
                    self.by_source_root.node = BTreeNode::Leaf(vec![]);
                    self.by_source_root.dirty = true;
                }
                ChangeResult::Split => {
                    self.by_source_root.split_root(&ByIdIndexer(()));
                }
            }
        }

        Ok(())
    }

    fn deserialize_document_map_entries(
        mut serialized: ArcBytes<'static>,
    ) -> Result<HashSet<ArcBytes<'static>>, nebari::Error> {
        let mut number_of_keys = [0; 4];
        serialized.read_exact(&mut number_of_keys)?;
        let number_of_keys = u32::from_be_bytes(number_of_keys);
        let mut keys = HashSet::new();
        for _ in 0..number_of_keys {
            let mut key_len = [0; 2];
            serialized.read_exact(&mut key_len)?;
            let key_len = usize::from(u16::from_be_bytes(key_len));
            let (key, remaining) = serialized.split_at(key_len);
            serialized = remaining;
            keys.insert(key);
        }
        Ok(keys)
    }

    fn serialize_document_map_entries(entries: &HashSet<ArcBytes<'_>>) -> Vec<u8> {
        let mut serialized = Vec::with_capacity(
            entries.iter().map(|e| e.len()).sum::<usize>() + entries.len() * 2 + 4,
        );

        // TODO not an assert
        serialized.extend(
            u32::try_from(entries.len())
                .expect("too many entries")
                .to_be_bytes(),
        );

        for key in entries {
            serialized.extend(
                u16::try_from(key.len())
                    .expect("key too large")
                    .to_be_bytes(),
            );
            serialized.extend(key.iter());
        }

        serialized
    }
}

impl Root for ViewEntries {
    const HEADER: nebari::tree::PageHeader = nebari::tree::PageHeader::UnversionedHeader;
    type Index = UnversionedByIdIndex<EntryIndex, EntryMappings>;
    type ReducedIndex = ByIdStats<EntryIndex>;
    type Reducer = ByIdIndexer<ViewIndexer>;
    type Value = EntryMappings;

    fn default_with(reducer: Self::Reducer) -> Self {
        Self {
            transaction_id: None,
            by_id_root: BTreeEntry::default(),
            by_source_root: BTreeEntry::default(),
            reducer,
        }
    }

    fn reducer(&self) -> &Self::Reducer {
        &self.reducer
    }

    fn count(&self) -> u64 {
        self.by_id_root.stats(self.reducer()).alive_keys
    }

    fn dirty(&self) -> bool {
        self.by_id_root.dirty
    }

    fn initialized(&self) -> bool {
        self.transaction_id.is_some()
    }

    fn initialize_default(&mut self) {
        self.transaction_id = Some(TransactionId(0));
    }

    fn serialize(
        &mut self,
        paged_writer: &mut nebari::tree::PagedWriter<'_>,
        output: &mut Vec<u8>,
    ) -> Result<(), nebari::Error> {
        output.reserve(PAGE_SIZE);
        output.write_all(&self.transaction_id.unwrap_or_default().0.to_be_bytes())?;
        // Reserve space for by_source and by_id sizes (2xu32).
        output.write_all(&[0; 8])?;

        let by_id_size = self.by_id_root.serialize_to(output, paged_writer)?;
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(nebari::ErrorKind::Internal(
                nebari::InternalError::HeaderTooLarge,
            ))?;
        let by_source_size = self.by_source_root.serialize_to(output, paged_writer)?;
        let by_source_size =
            u32::try_from(by_source_size)
                .ok()
                .ok_or(nebari::ErrorKind::Internal(
                    nebari::InternalError::HeaderTooLarge,
                ))?;

        output[8..12].copy_from_slice(&by_id_size.to_be_bytes());
        output[12..16].copy_from_slice(&by_source_size.to_be_bytes());

        Ok(())
    }

    fn deserialize(mut bytes: ArcBytes<'_>, reducer: Self::Reducer) -> Result<Self, nebari::Error> {
        let transaction_id = TransactionId(bytes.read_u64::<BigEndian>()?);
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        let by_source_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_source_size + by_id_size != bytes.len() {
            return Err(nebari::Error::from(format!(
                "Header reported index sizes {} and {}, but data has {} remaining",
                by_source_size,
                by_id_size,
                bytes.len()
            )));
        }
        let mut by_id_bytes = bytes.read_bytes(by_id_size)?.to_owned();
        let mut by_source_bytes = bytes.read_bytes(by_source_size)?.to_owned();
        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes, None)?;
        let by_source_root = BTreeEntry::deserialize_from(&mut by_source_bytes, None)?;

        Ok(Self {
            transaction_id: Some(transaction_id),
            by_id_root,
            by_source_root,
            reducer,
        })
    }

    fn transaction_id(&self) -> nebari::transaction::TransactionId {
        self.transaction_id.unwrap()
    }

    fn modify<'a, 'w>(
        &'a mut self,
        modification: nebari::tree::Modification<'_, Self::Value, Self::Index>,
        writer: &'a mut nebari::tree::PagedWriter<'w>,
        max_order: Option<usize>,
    ) -> Result<Vec<nebari::tree::ModificationResult<Self::Index>>, nebari::Error> {
        let transaction_id = modification.persistence_mode.transaction_id();

        let persistence_mode = modification.persistence_mode;
        let (results, document_map_changes) =
            self.modify_id_root(modification, writer, max_order)?;

        self.modify_document_map(document_map_changes, writer, max_order, persistence_mode)?;

        // Only update the transaction id if a new one was specified.
        if let Some(transaction_id) = transaction_id {
            self.transaction_id = Some(transaction_id);
        }

        Ok(results)
    }

    fn get_multiple<'keys, KeyEvaluator, KeyReader, Keys>(
        &self,
        keys: &mut Keys,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut dyn nebari::io::File,
        vault: Option<&dyn nebari::AnyVault>,
        cache: Option<&nebari::ChunkCache>,
    ) -> Result<(), nebari::Error>
    where
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Self::Index) -> nebari::tree::ScanEvaluation,
        KeyReader: FnMut(ArcBytes<'static>, Self::Value, Self::Index) -> Result<(), nebari::Error>,
        Keys: Iterator<Item = &'keys [u8]>,
    {
        self.by_id_root.get_multiple(
            keys,
            key_evaluator,
            |key, value, index| {
                key_reader(
                    key,
                    EntryMappings {
                        mappings: bincode::deserialize(&value).map_err(|err| {
                            nebari::Error::from(std::io::Error::new(ErrorKind::Other, err))
                        })?,
                        documents: BTreeMap::default(),
                    },
                    index,
                )
            },
            file,
            vault,
            cache,
        )
    }

    fn scan<
        'keys,
        CallerError: Display + std::fmt::Debug,
        NodeEvaluator,
        KeyRangeBounds,
        KeyEvaluator,
        ScanDataCallback,
    >(
        &self,
        range: &'keys KeyRangeBounds,
        mut args: ScanArgs<
            Self::Value,
            Self::Index,
            Self::ReducedIndex,
            CallerError,
            NodeEvaluator,
            KeyEvaluator,
            ScanDataCallback,
        >,
        file: &mut dyn nebari::io::File,
        vault: Option<&dyn nebari::AnyVault>,
        cache: Option<&nebari::ChunkCache>,
    ) -> Result<bool, nebari::AbortError<CallerError>>
    where
        NodeEvaluator:
            FnMut(&ArcBytes<'static>, &Self::ReducedIndex, usize) -> nebari::tree::ScanEvaluation,
        KeyEvaluator: FnMut(&ArcBytes<'static>, &Self::Index) -> nebari::tree::ScanEvaluation,
        KeyRangeBounds: std::ops::RangeBounds<&'keys [u8]> + std::fmt::Debug + ?Sized,
        ScanDataCallback: FnMut(
            ArcBytes<'static>,
            &Self::Index,
            Self::Value,
        ) -> Result<(), nebari::AbortError<CallerError>>,
    {
        self.by_id_root.scan(
            range,
            &mut ScanArgs::new(
                args.forwards,
                args.node_evaluator,
                args.key_evaluator,
                |key, index, value: ArcBytes<'static>| {
                    let value = EntryMappings {
                        mappings: bincode::deserialize(&value).map_err(|err| {
                            nebari::Error::from(std::io::Error::new(ErrorKind::Other, err))
                        })?,
                        documents: BTreeMap::default(),
                    };
                    (args.data_callback)(key, index, value)
                },
            ),
            file,
            vault,
            cache,
            0,
        )
    }

    fn copy_data_to(
        &mut self,
        _include_nodes: bool,
        _file: &mut dyn nebari::io::File,
        _copied_chunks: &mut std::collections::HashMap<u64, u64>,
        _writer: &mut nebari::tree::PagedWriter<'_>,
        _vault: Option<&dyn nebari::AnyVault>,
    ) -> Result<(), nebari::Error> {
        todo!()
    }
}

pub mod integrity_scanner;
pub mod mapper;

pub fn view_entries_tree_name(view_name: &impl Display) -> String {
    format!("view.{:#}", view_name)
}

/// Used to store Document ID -> Key mappings, so that when a document is updated, we can remove the old entry.
pub fn view_document_map_tree_name(view_name: &impl Display) -> String {
    format!("view.{:#}.document-map", view_name)
}

pub fn view_versions_tree_name(collection: &CollectionName) -> String {
    format!("view-versions.{:#}", collection)
}

#[derive(Debug, Default, Clone)]
pub struct EntryMappings {
    pub mappings: Vec<EntryMapping>,
    pub documents: BTreeMap<ArcBytes<'static>, bool>,
}
