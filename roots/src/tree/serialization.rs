use async_trait::async_trait;
use byteorder::WriteBytesExt;

use super::PagedWriter;
use crate::{AsyncFile, Buffer, Error};

#[async_trait(?Send)]
pub trait BinarySerialization: Sized {
    async fn serialize_to<W: WriteBytesExt, F: AsyncFile>(
        &mut self,
        writer: &mut W,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error>;
    async fn serialize<F: AsyncFile>(
        &mut self,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        buffer.reserve(super::PAGE_SIZE);
        self.serialize_to(&mut buffer, paged_writer).await?;
        Ok(buffer)
    }
    fn deserialize_from(reader: &mut Buffer<'_>) -> Result<Self, Error>;
}

#[async_trait(?Send)]
impl BinarySerialization for () {
    async fn serialize_to<W: WriteBytesExt, F: AsyncFile>(
        &mut self,
        _writer: &mut W,
        _paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(_reader: &mut Buffer<'_>) -> Result<Self, Error> {
        Ok(())
    }
}
