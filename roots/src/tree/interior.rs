use std::{borrow::Cow, convert::TryFrom};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{BinarySerialization, Ownable};
use crate::Error;

#[derive(Clone, Debug)]
pub struct Interior<'a, R> {
    // The key with the highest sort value within.
    pub key: Cow<'a, [u8]>,
    /// The location of the node on disk.
    pub position: u64,
    /// The reduced statistics.
    pub stats: R,
}

impl<'a, R: Ownable> Ownable for Interior<'a, R> {
    type Output = Interior<'static, <R as Ownable>::Output>;

    fn to_owned_lifetime(&self) -> Self::Output {
        Interior {
            key: Cow::Owned(self.key.to_vec()),
            position: self.position,
            stats: self.stats.to_owned_lifetime(),
        }
    }
}

impl<'a, R: BinarySerialization<'a>> BinarySerialization<'a> for Interior<'a, R> {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| Error::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.write_all(&self.key)?;
        bytes_written += 2 + key_len as usize;

        writer.write_u64::<BigEndian>(self.position)?;
        bytes_written += 8;

        bytes_written += self.stats.serialize_to(writer)?;

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut &'a [u8]) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let (key, remainder) = reader.split_at(key_len);
        *reader = remainder;

        let position = reader.read_u64::<BigEndian>()?;
        let stats = R::deserialize_from(reader)?;

        Ok(Self {
            key: Cow::Borrowed(key),
            position,
            stats,
        })
    }
}
