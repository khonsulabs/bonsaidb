use std::{borrow::Cow, convert::TryFrom};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{BinarySerialization, NoLifetime, Ownable, Reducer};
use crate::Error;

#[derive(Clone, Debug)]
pub struct BySequenceIndex<'a> {
    pub document_id: Cow<'a, [u8]>,
    pub document_size: u32,
    pub position: u64,
}

impl<T> Ownable for T
where
    T: NoLifetime,
{
    type Output = Self;
    fn to_owned_lifetime(&self) -> Self::Output {
        self.clone()
    }
}

impl<'a> Ownable for BySequenceIndex<'a> {
    type Output = BySequenceIndex<'static>;

    fn to_owned_lifetime(&self) -> Self::Output {
        BySequenceIndex {
            document_id: Cow::Owned(self.document_id.to_vec()),
            document_size: self.document_size,
            position: self.position,
        }
    }
}

impl<'a> BinarySerialization<'a> for BySequenceIndex<'a> {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        let mut bytes_written = 0;
        writer.write_u32::<BigEndian>(self.document_size)?;
        bytes_written += 4;
        writer.write_u64::<BigEndian>(self.position)?;
        bytes_written += 8;

        let document_id_length =
            u16::try_from(self.document_id.len()).map_err(|_| Error::IdTooLarge)?;
        writer.write_u16::<BigEndian>(document_id_length)?;
        bytes_written += 2;
        writer.write_all(&self.document_id)?;
        bytes_written += document_id_length as usize;
        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut &'a [u8]) -> Result<Self, Error> {
        let document_size = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        let document_id_length = reader.read_u16::<BigEndian>()? as usize;
        if document_id_length > reader.len() {
            return Err(Error::data_integrity(format!(
                "document id length {} found but only {} bytes remaining",
                document_id_length,
                reader.len()
            )));
        }
        let (document_id, remainder) = reader.split_at(document_id_length);
        *reader = remainder;

        Ok(Self {
            document_id: Cow::Borrowed(document_id),
            document_size,
            position,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BySequenceStats {
    pub number_of_records: u64,
}

impl NoLifetime for BySequenceStats {}

impl<'a> BinarySerialization<'a> for BySequenceStats {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.number_of_records)?;
        Ok(8)
    }

    fn deserialize_from(reader: &mut &'a [u8]) -> Result<Self, Error> {
        let number_of_records = reader.read_u64::<BigEndian>()?;
        Ok(Self { number_of_records })
    }
}

impl<'a> Reducer<BySequenceIndex<'a>> for BySequenceStats {
    fn reduce(values: &[&BySequenceIndex<'a>]) -> Self {
        Self {
            number_of_records: values.len() as u64,
        }
    }

    fn rereduce(values: &[&Self]) -> Self {
        Self {
            number_of_records: values.iter().map(|v| v.number_of_records).sum(),
        }
    }
}
