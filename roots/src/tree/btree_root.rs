use std::{collections::VecDeque, convert::TryFrom, marker::PhantomData};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use futures::FutureExt;

use super::{
    btree_entry::BTreeEntry,
    by_id::{ByIdIndex, ByIdStats},
    by_sequence::{BySequenceIndex, BySequenceStats},
    modify::Modification,
    read_chunk,
    serialization::BinarySerialization,
    PagedWriter,
};
use crate::{
    error::InternalError,
    tree::{
        btree_entry::ModificationContext,
        modify::{CompareSwap, Operation},
    },
    AsyncFile, Buffer, ChunkCache, Error, Vault,
};

#[derive(Clone, Default, Debug)]
pub struct BTreeRoot<const MAX_ORDER: usize> {
    pub transaction_id: u64,
    pub sequence: u64,
    by_sequence_root: BTreeEntry<BySequenceIndex, BySequenceStats>,
    by_id_root: BTreeEntry<ByIdIndex, ByIdStats>,
}

pub enum ChangeResult<I: BinarySerialization, R: BinarySerialization> {
    Unchanged,
    Replace(BTreeEntry<I, R>),
    Split(BTreeEntry<I, R>, BTreeEntry<I, R>),
}

#[allow(clippy::future_not_send)]
impl<const MAX_ORDER: usize> BTreeRoot<MAX_ORDER> {
    #[allow(clippy::too_many_lines)]
    pub async fn modify<'a, 'w, F: AsyncFile>(
        &'a mut self,
        mut modification: Modification<'static, Buffer<'static>>,
        writer: &'a mut PagedWriter<'w, F>,
    ) -> Result<(), Error> {
        let new_sequence = self
            .sequence
            .checked_add(1)
            .expect("sequence rollover prevented");

        // Reverse sort so that pop is efficient.
        modification.keys.sort_by(|a, b| b.cmp(a));

        // Insert into both trees
        let by_sequence_order =
            dynamic_order::<MAX_ORDER>(self.by_sequence_root.stats().number_of_records);
        // Generate a list of sequence changes, somehow.
        let mut changes = EntryChanges::default();
        match self
            .by_sequence_root
            .modify(
                &mut modification,
                &ModificationContext {
                    current_order: by_sequence_order,
                    indexer: |key: &Buffer<'static>,
                              value: &Buffer<'static>,
                              _existing_index: Option<&BySequenceIndex>,
                              changes: &mut EntryChanges,
                              writer: &mut PagedWriter<'_, F>| {
                        async move {
                            let document_position = writer.write_chunk(value).await?;
                            // write_chunk errors if it can't fit within a u32
                            #[allow(clippy::cast_possible_truncation)]
                            let document_size = value.len() as u32;
                            changes.current_sequence = changes
                                .current_sequence
                                .checked_add(1)
                                .expect("sequence rollover prevented");
                            changes.changes.push(EntryChange {
                                key: key.clone(),
                                sequence: changes.current_sequence,
                                document_position,
                                document_size,
                            });
                            Ok(Some(BySequenceIndex {
                                document_id: key.clone(),
                                position: document_position,
                                document_size,
                            }))
                        }
                        .boxed_local()
                    },
                    loader: |index: &BySequenceIndex, writer: &mut PagedWriter<'_, F>| {
                        async move { writer.read_chunk(index.position).await.map(Some) }
                            .boxed_local()
                    },
                    _phantom: PhantomData,
                },
                None,
                &mut changes,
                writer,
            )
            .await?
        {
            ChangeResult::Unchanged => unreachable!(),
            ChangeResult::Replace(new_root) => {
                self.by_sequence_root = new_root;
            }
            ChangeResult::Split(lower, upper) => {
                self.by_sequence_root
                    .split_root(lower, upper, writer)
                    .await?;
            }
        }
        self.sequence = changes.current_sequence;
        let by_id_order = dynamic_order::<MAX_ORDER>(self.by_id_root.stats().total_documents());
        let mut operations = VecDeque::with_capacity(changes.changes.len());
        let keys = changes
            .changes
            .into_iter()
            .map(|change| {
                operations.push_back(ByIdIndex {
                    sequence_id: change.sequence,
                    document_size: change.document_size,
                    position: change.document_position,
                });
                change.key
            })
            .collect();
        let mut sequence_modification = Modification {
            transaction_id: modification.transaction_id,
            keys,
            operation: Operation::CompareSwap(CompareSwap::new(move |_key, _existing_entry| {
                operations.pop_front()
            })),
        };
        match self
            .by_id_root
            .modify(
                &mut sequence_modification,
                &ModificationContext {
                    current_order: by_id_order,
                    indexer: |_key: &Buffer<'static>,
                              value: &ByIdIndex,
                              _existing_index,
                              _changes,
                              _writer: &mut PagedWriter<'_, F>| {
                        async move { Ok(Some(value.clone())) }.boxed_local()
                    },
                    loader: |_index, _writer| async move { Ok(None) }.boxed_local(),
                    _phantom: PhantomData,
                },
                None,
                &mut EntryChanges::default(),
                writer,
            )
            .await?
        {
            ChangeResult::Unchanged => unreachable!(),
            ChangeResult::Replace(new_root) => {
                self.by_id_root = new_root;
            }
            ChangeResult::Split(lower, upper) => {
                self.by_id_root.split_root(lower, upper, writer).await?;
            }
        }

        self.sequence = new_sequence;

        Ok(())
    }

    pub async fn get<F: AsyncFile>(
        &self,
        key: &[u8],
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<Option<Buffer<'static>>, Error> {
        match self.by_id_root.get(key, file, vault, cache).await? {
            Some(entry) => {
                let contents = read_chunk(entry.position, file, vault, cache).await?;
                Ok(Some(contents))
            }
            None => Ok(None),
        }
    }

    pub fn deserialize(mut bytes: Buffer<'_>) -> Result<Self, Error> {
        let transaction_id = bytes.read_u64::<BigEndian>()?;
        let sequence = bytes.read_u64::<BigEndian>()?;
        let by_sequence_size = bytes.read_u32::<BigEndian>()? as usize;
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_sequence_size + by_id_size != bytes.len() {
            return Err(Error::data_integrity(format!(
                "Header reported index sizes {} and {}, but data has {} remaining",
                by_sequence_size,
                by_id_size,
                bytes.len()
            )));
        };

        let mut by_sequence_bytes = bytes.read_bytes(by_sequence_size)?.to_owned();
        let mut by_id_bytes = bytes.read_bytes(by_id_size)?.to_owned();

        let by_sequence_root = BTreeEntry::deserialize_from(&mut by_sequence_bytes)?;
        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes)?;

        Ok(Self {
            transaction_id,
            sequence,
            by_sequence_root,
            by_id_root,
        })
    }

    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        output.write_u64::<BigEndian>(self.transaction_id)?;
        output.write_u64::<BigEndian>(self.sequence)?;
        // Reserve space for by_sequence and by_id sizes (2xu16).
        output.write_u64::<BigEndian>(0)?;

        let by_sequence_size = self.by_sequence_root.serialize_to(&mut output)?;
        let by_id_size = self.by_id_root.serialize_to(&mut output)?;

        let by_sequence_size = u32::try_from(by_sequence_size)
            .ok()
            .ok_or(Error::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[16..20], by_sequence_size);
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(Error::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[20..24], by_id_size);

        Ok(output)
    }
}

/// Returns a value for the "order" (maximum children per node) value for the
/// database. This function is meant to keep nodes smaller while the database
/// can fit in a tree with 2 depth: root -> interior -> leaf. This is an
/// approximation that always returns an order larger than what is needed, but
/// will never return a value larger than `MAX_ORDER`.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn dynamic_order<const MAX_ORDER: usize>(number_of_records: u64) -> usize {
    if number_of_records > MAX_ORDER.pow(3) as u64 {
        MAX_ORDER
    } else {
        // Use the ceil of the cube root of the number of records as a base. We don't want 100% fill rate, however, so we'll add one.
        MAX_ORDER.min(2.max((number_of_records as f64).cbrt().ceil() as usize) + 1)
    }
}

#[test]
fn dynamic_order_tests() {
    assert_eq!(dynamic_order::<10>(0), 3);
    assert_eq!(dynamic_order::<10>(26), 4);
    assert_eq!(dynamic_order::<10>(27), 5);
    assert_eq!(dynamic_order::<10>(500), 9);
    assert_eq!(dynamic_order::<10>(1000), 10);
}

#[derive(Default)]
pub struct EntryChanges {
    pub current_sequence: u64,
    pub changes: Vec<EntryChange>,
}
pub struct EntryChange {
    pub sequence: u64,
    pub key: Buffer<'static>,
    pub document_position: u64,
    pub document_size: u32,
}
