use std::path::Path;

use async_trait::async_trait;
use futures::Future;
use tokio_uring::{
    buf::IoBuf,
    fs::{File, OpenOptions},
};

use super::{AsyncFile, AsyncFileManager, FileOp, OpenableFile};
use crate::Error;

/// An open file that uses `tokio-uring`. Requires feature `uring`.
pub struct UringFile(File);

#[async_trait(?Send)]
impl AsyncFile for UringFile {
    type Manager = UringFileManager;
    async fn read(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error> {
        Ok(Self(File::open(path).await?))
    }

    async fn append(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error> {
        Ok(Self(
            OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .write(true)
                .open(path)
                .await?,
        ))
    }

    async fn read_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        let (result, buffer) = self.0.read_at(buffer.slice(offset..len), position).await;
        (result.map_err(Error::from), buffer.into_inner())
    }

    async fn write_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        let (result, buffer) = self.0.write_at(buffer.slice(offset..len), position).await;
        (result.map_err(Error::from), buffer.into_inner())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        self.0.sync_data().await.map_err(Error::from)
    }

    async fn close(self) -> Result<(), Error> {
        self.0.sync_all().await?;
        self.0.close().await.map_err(Error::from)
    }
}

#[derive(Default, Clone)]
pub struct UringFileManager;

#[async_trait(?Send)]
impl AsyncFileManager<UringFile> for UringFileManager {
    type FileHandle = UringFile;

    async fn append(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<Self::FileHandle, Error> {
        UringFile::append(path).await
    }

    async fn read(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<Self::FileHandle, Error> {
        UringFile::read(path).await
    }

    fn run<Fut: Future<Output = ()>>(future: Fut) {
        tokio_uring::start(future);
    }
}

#[async_trait(?Send)]
impl OpenableFile<Self> for UringFile {
    async fn write<W: FileOp<Self>>(&mut self, mut writer: W) -> Result<W::Output, Error> {
        writer.write(self).await
    }

    async fn close(self) -> Result<(), Error> {
        AsyncFile::close(self).await
    }
}
