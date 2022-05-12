use std::{
    fmt::Display,
    io::ErrorKind,
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
use nebari::tree::{EmbeddedIndex, Indexer, Reducer, SequenceId, Serializable};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct ViewEntry {
    pub key: Bytes,
    pub reduced_value: Bytes,
    pub mappings: Vec<EntryMapping>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EntryMapping {
    pub source: Header,
    pub value: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct EntryIndex {
    pub value: Option<Vec<u8>>,
    pub latest_sequence: SequenceId,
}

impl EmbeddedIndex for EntryIndex {
    type Reduced = Self;
    type Indexer = ViewIndexer;
}

impl Indexer<EntryIndex> for ViewIndexer {
    fn index(
        &self,
        _key: &nebari::ArcBytes<'_>,
        value: Option<&nebari::ArcBytes<'static>>,
    ) -> EntryIndex {
        let has_reduce = self.has_reduce.load(Ordering::Relaxed);
        if has_reduce {
            if let Some(mappings) =
                value.and_then(|bytes| bincode::deserialize::<Vec<EntryMapping>>(bytes).ok())
            {
                let mut latest_sequence_id = 0;
                let mappings = mappings
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

pub type ViewEntries = nebari::tree::UnversionedTreeRoot<EntryIndex>;

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
