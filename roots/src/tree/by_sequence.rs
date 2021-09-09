use std::convert::TryFrom;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{BinaryDeserialization, BinarySerialization, Reducer, ScratchBuffer};
use crate::Error;

#[derive(Clone, Debug)]
pub struct BySequenceIndex {
    pub document_id: ScratchBuffer,
    pub document_size: u32,
    pub position: u64,
}

impl BinarySerialization for BySequenceIndex {
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
}

impl<'a> BinaryDeserialization for BySequenceIndex {
    fn deserialize_from(reader: &mut ScratchBuffer) -> Result<Self, Error> {
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
        let document_id = reader.read_bytes(document_id_length)?;

        Ok(Self {
            document_id,
            document_size,
            position,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BySequenceStats {
    pub number_of_records: u64,
}

impl BinarySerialization for BySequenceStats {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.number_of_records)?;
        Ok(8)
    }
}

impl<'a> BinaryDeserialization for BySequenceStats {
    fn deserialize_from(reader: &mut ScratchBuffer) -> Result<Self, Error> {
        let number_of_records = reader.read_u64::<BigEndian>()?;
        Ok(Self { number_of_records })
    }
}

impl<'a> Reducer<BySequenceIndex> for BySequenceStats {
    fn reduce(values: &[&BySequenceIndex]) -> Self {
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
