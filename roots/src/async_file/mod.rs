use std::path::Path;

use async_trait::async_trait;
use cfg_if::cfg_if;
use futures::Future;

use crate::error::Error;

macro_rules! try_with_buffer {
    ($buffer:ident, $expr:expr) => {
        match $expr {
            Ok(result) => result,
            Err(err) => return (Err(crate::Error::from(err)), $buffer),
        }
    };
}

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

pub mod tokio;
#[cfg(feature = "uring")]
pub mod uring;

/// A file that can be interacted with using async operations.
///
/// This trait is an abstraction that mimics `tokio-uring`'s File type, allowing
/// for a non-uring implementation to be provided as well. This is why the
/// read/write APIs take ownership of the buffer -- to satisfy the requirements
/// of tokio-uring.
#[async_trait(?Send)]
pub trait AsyncFile: Sized + 'static {
    /// The file manager that synchronizes file access across threads.
    type Manager: AsyncFileManager<Self>;

    /// Opens a file at `path` with read-only permission.
    async fn read(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error>;
    /// Opens or creates a file at `path`, positioning the cursor at the end of the file.
    async fn append(path: impl AsRef<Path> + Send + 'async_trait) -> Result<Self, Error>;

    /// Tries to read data at `position`, using `buffer` for the read. An
    /// attempt to read `len` bytes is made, and are written to the buffer at
    /// `offset`. Returns a tuple with a result and the buffer. If successful,
    /// the result will contain then umber of bytes read.
    async fn read_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>);

    /// Tries to write data at `position`, using `buffer` for the data. An
    /// attempt to write `len` bytes is made, and reading bytes from buffer at
    /// `offset`. Returns a tuple with a result and the buffer. If successful,
    /// the result will contain then umber of bytes written.
    async fn write_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>);

    /// Flushes all data to the file.
    async fn flush(&mut self) -> Result<(), Error>;

    /// Safely closes the file after flushing any pending operations to disk.
    async fn close(self) -> Result<(), Error>;

    /// Reads exactly `length` bytes from `position` within this file into
    /// `buffer`. Returns a tuple with a result and the buffer. An error is
    /// returned if not enough data can be read.
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

    /// Writes `length` bytes starting at `start` from `buffer` into this file
    /// at `position`. Returns a tuple with a result and the buffer. An error is
    /// returned if not all data is written. Note: use [`Self::flush()] to ensure the
    /// data is fully committed to the disk.
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

/// An open file.
pub enum File {
    /// A `io_uring` capable file. Requires feature `uring`.
    #[cfg(feature = "uring")]
    Uring(uring::UringFile),
    /// A `tokio::fs` file.
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

#[async_trait(?Send)]
pub trait AsyncFileManager<F: AsyncFile>: Send + Sync + Clone + Default {
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
impl OpenableFile<Self> for File {
    async fn write<W: FileWriter<Self>>(&mut self, mut writer: W) -> Result<(), Error> {
        writer.write(self).await
    }

    async fn close(self) -> Result<(), Error> {
        AsyncFile::close(self).await
    }
}
