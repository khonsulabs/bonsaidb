use byteorder::WriteBytesExt;

use crate::{Buffer, Error};

pub trait BinarySerialization: Sized {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error>;
    fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        self.serialize_to(&mut buffer)?;
        Ok(buffer)
    }
    fn deserialize_from(reader: &mut Buffer<'static>) -> Result<Self, Error>;
}

impl BinarySerialization for () {
    fn serialize_to<W: WriteBytesExt>(&self, _writer: &mut W) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(_reader: &mut Buffer<'static>) -> Result<Self, Error> {
        Ok(())
    }
}
