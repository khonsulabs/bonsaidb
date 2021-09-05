use std::path::Path;

use async_trait::async_trait;
use cfg_if::cfg_if;
use futures::Future;

use crate::{error::Error, try_with_buffer_result};

pub mod tokio;
#[cfg(feature = "uring")]
pub mod uring;

#[async_trait(?Send)]
pub trait AsyncFile: Sized + 'static {
    type Manager: AsyncFileManager<Self>;

    async fn read(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error>;
    async fn append(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error>;
    async fn read_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>);
    async fn write_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>);
    async fn flush(&mut self) -> Result<(), Error>;
    async fn close(self) -> Result<(), Error>;

    async fn read_exact(
        &mut self,
        mut position: u64,
        mut buffer: Vec<u8>,
        length: usize,
    ) -> (Result<(), Error>, Vec<u8>) {
        let mut offset = 0;
        while offset < length {
            let bytes_read = try_with_buffer_result!(
                buffer,
                self.read_at(position, buffer, offset, length - offset)
                    .await
            );
            offset += bytes_read;
            position += bytes_read as u64;
        }

        (Ok(()), buffer)
    }

    async fn write_all(
        &mut self,
        mut position: u64,
        mut buffer: Vec<u8>,
        start: usize,
        length: usize,
    ) -> (Result<(), Error>, Vec<u8>) {
        let mut offset = 0;
        while offset < length {
            let bytes_written = try_with_buffer_result!(
                buffer,
                self.write_at(position, buffer, start + offset, length - offset)
                    .await
            );
            offset += bytes_written;
            position += bytes_written as u64;
        }

        (Ok(()), buffer)
    }
}

pub enum File {
    #[cfg(feature = "uring")]
    Uring(uring::UringFile),
    Tokio(tokio::TokioFile),
}

#[async_trait(?Send)]
impl AsyncFile for File {
    type Manager = FileManager;

    async fn read(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error> {
        cfg_if! {
            if #[cfg(feature = "uring")] {
                uring::UringFile::read(path).await.map(Self::Uring)
            } else {
                tokio::TokioFile::read(path).await.map(Self::Tokio)
            }
        }
    }

    async fn append(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error> {
        cfg_if! {
            if #[cfg(feature = "uring")] {
                uring::UringFile::append(path).await.map(Self::Uring)
            } else {
                tokio::TokioFile::append(path).await.map(Self::Tokio)
            }
        }
    }

    async fn read_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        match self {
            #[cfg(feature = "uring")]
            Self::Uring(uring) => uring.read_at(position, buffer, offset, len).await,
            Self::Tokio(tokio) => tokio.read_at(position, buffer, offset, len).await,
        }
    }

    async fn write_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        match self {
            #[cfg(feature = "uring")]
            Self::Uring(uring) => uring.write_at(position, buffer, offset, len).await,
            Self::Tokio(tokio) => tokio.write_at(position, buffer, offset, len).await,
        }
    }

    async fn flush(&mut self) -> Result<(), Error> {
        match self {
            #[cfg(feature = "uring")]
            Self::Uring(uring) => uring.flush().await,
            Self::Tokio(tokio) => tokio.flush().await,
        }
    }

    async fn close(self) -> Result<(), Error> {
        match self {
            #[cfg(feature = "uring")]
            Self::Uring(uring) => AsyncFile::close(uring).await,
            Self::Tokio(tokio) => AsyncFile::close(tokio).await,
        }
    }
}

#[macro_export]
macro_rules! try_with_buffer {
    ($buffer:ident, $expr:expr) => {
        match $expr {
            Ok(result) => result,
            Err(err) => return (Err(crate::Error::from(err)), $buffer),
        }
    };
}

#[macro_export]
macro_rules! try_with_buffer_result {
    ($buffer:ident, $expr:expr) => {{
        let result = $expr;
        $buffer = result.1;
        match result.0 {
            Ok(result) => result,
            Err(err) => return (Err(crate::Error::from(err)), $buffer),
        }
    }};
}

#[async_trait(?Send)]
pub trait AsyncFileManager<F: AsyncFile>: Send + Clone + Default {
    type FileHandle: OpenableFile<F>;
    // async fn read(&self, path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self::FileHandle, Error>;
    async fn append(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<Self::FileHandle, Error>;

    fn run<Fut: Future<Output = ()>>(future: Fut);
}

#[async_trait(?Send)]
pub trait OpenableFile<F: AsyncFile> {
    async fn write<W: FileWriter<F>>(&mut self, writer: W) -> Result<(), Error>;

    async fn close(self) -> Result<(), Error>;
}

#[async_trait(?Send)]
pub trait FileWriter<F: AsyncFile> {
    async fn write(&mut self, file: &mut F) -> Result<(), Error>;
}

#[derive(Default, Clone)]
pub struct FileManager;

#[async_trait(?Send)]
impl AsyncFileManager<File> for FileManager {
    type FileHandle = File;

    async fn append(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<Self::FileHandle, Error> {
        File::append(path).await
    }

    fn run<Fut: Future<Output = ()>>(future: Fut) {
        cfg_if! {
            if #[cfg(feature = "uring")] {
                tokio_uring::start(future);
            } else {
                tokio::runtime::Runtime::new().block_on(future);
            }
        }
    }
}

#[async_trait(?Send)]
impl OpenableFile<File> for File {
    async fn write<W: FileWriter<File>>(&mut self, mut writer: W) -> Result<(), Error> {
        writer.write(self).await
    }

    async fn close(self) -> Result<(), Error> {
        AsyncFile::close(self).await
    }
}
