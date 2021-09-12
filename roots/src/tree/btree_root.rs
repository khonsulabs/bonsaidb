use std::convert::TryFrom;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

use super::{
    btree_entry::BTreeEntry,
    by_id::{ByIdIndex, ByIdStats},
    by_sequence::{BySequenceIndex, BySequenceStats},
    read_chunk,
    serialization::BinarySerialization,
    PagedWriter,
};
use crate::{error::InternalError, AsyncFile, Buffer, ChunkCache, Error, Vault};

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
    pub async fn insert<F: AsyncFile>(
        &mut self,
        key: Buffer,
        document_size: u32,
        document_position: u64,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<(), Error> {
        let new_sequence = self
            .sequence
            .checked_add(1)
            .expect("sequence rollover prevented");

        // Insert into both trees
        let by_sequence_order =
            dynamic_order::<MAX_ORDER>(self.by_sequence_root.stats().number_of_records);
        match self
            .by_sequence_root
            .insert(
                Buffer::from(new_sequence.to_be_bytes().to_vec()),
                BySequenceIndex {
                    document_id: key.clone(),
                    document_size,
                    position: document_position,
                },
                by_sequence_order,
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
        let by_id_order = dynamic_order::<MAX_ORDER>(self.by_id_root.stats().total_documents());
        match self
            .by_id_root
            .insert(
                key,
                ByIdIndex {
                    sequence_id: new_sequence,
                    document_size,
                    position: document_position,
                },
                by_id_order,
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
    ) -> Result<Option<Buffer>, Error> {
        match self.by_id_root.get(key, file, vault, cache).await? {
            Some(entry) => {
                let contents = read_chunk(entry.position, file, vault, cache).await?;
                Ok(Some(contents))
            }
            None => Ok(None),
        }
    }

    pub fn deserialize(mut bytes: Buffer) -> Result<Self, Error> {
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

        let mut by_sequence_bytes = bytes.read_bytes(by_sequence_size)?;
        let mut by_id_bytes = bytes.read_bytes(by_id_size)?;

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
    assert_eq!(dynamic_order::<10>(800), 9);
    assert_eq!(dynamic_order::<10>(1000), 10);
}
