use std::{convert::TryFrom, fmt::Debug};

use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{
    btree_entry::{BTreeEntry, Reducer},
    BinarySerialization, PagedWriter,
};
use crate::{AsyncFile, Buffer, Error};

#[derive(Clone, Debug)]
pub struct Interior<I, R> {
    // The key with the highest sort value within.
    pub key: Buffer<'static>,
    /// The location of the node.
    pub position: Pointer<I, R>,
    /// The reduced statistics.
    pub stats: R,
}

impl<I, R> From<BTreeEntry<I, R>> for Interior<I, R>
where
    I: Clone + Debug + BinarySerialization + 'static,
    R: Reducer<I> + Clone + Debug + BinarySerialization + 'static,
{
    fn from(entry: BTreeEntry<I, R>) -> Self {
        let key = entry.max_key().clone();
        let stats = entry.stats();

        Self {
            key,
            stats,
            position: Pointer::Loaded(Box::new(entry)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Pointer<I, R> {
    OnDisk(u64),
    Loaded(Box<BTreeEntry<I, R>>),
}

#[allow(clippy::future_not_send)]
impl<
        I: BinarySerialization + Debug + Clone + 'static,
        R: Reducer<I> + BinarySerialization + Debug + Clone + 'static,
    > Pointer<I, R>
{
    pub async fn load<F: AsyncFile>(
        &mut self,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<(), Error> {
        match self {
            Pointer::OnDisk(position) => {
                let entry = BTreeEntry::deserialize_from(&mut writer.read_chunk(*position).await?)?;
                *self = Self::Loaded(Box::new(entry));
            }
            Pointer::Loaded(_) => {}
        }
        Ok(())
    }

    pub fn get_mut(&mut self) -> Option<&mut BTreeEntry<I, R>> {
        match self {
            Pointer::OnDisk(_) => None,
            Pointer::Loaded(entry) => Some(entry.as_mut()),
        }
    }
}

#[async_trait(?Send)]
impl<
        I: Clone + BinarySerialization + Debug + 'static,
        R: Reducer<I> + Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for Interior<I, R>
{
    async fn serialize_to<W: WriteBytesExt, F: AsyncFile>(
        &mut self,
        writer: &mut W,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        let position = match &mut self.position {
            Pointer::OnDisk(position) => *position,
            Pointer::Loaded(node) => {
                let bytes = node.serialize(paged_writer).await?;
                let position = paged_writer.write_chunk(&bytes).await?;
                self.position = Pointer::OnDisk(position);
                position
            }
        };
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| Error::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.write_all(&self.key)?;
        bytes_written += 2 + key_len as usize;

        writer.write_u64::<BigEndian>(position)?;
        bytes_written += 8;

        bytes_written += self.stats.serialize_to(writer, paged_writer).await?;

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut Buffer<'_>) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let key = reader.read_bytes(key_len)?.to_owned();

        let position = reader.read_u64::<BigEndian>()?;
        let stats = R::deserialize_from(reader)?;

        Ok(Self {
            key,
            position: Pointer::OnDisk(position),
            stats,
        })
    }
}
