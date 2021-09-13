use std::{
    collections::HashMap,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use futures::Future;
use once_cell::sync::Lazy;
use tokio::sync::{Mutex, RwLock};

use super::{AsyncFile, AsyncFileManager, FileOp, OpenableFile};
use crate::Error;

/// A fake "file" represented by an in-memory buffer. This should only be used
/// in testing, as this database format is not optimized for memory efficiency.
pub struct MemoryFile {
    path: Arc<PathBuf>,
    buffer: Arc<RwLock<Vec<u8>>>,
}

type OpenBuffers = Arc<Mutex<HashMap<PathBuf, Weak<RwLock<Vec<u8>>>>>>;
static OPEN_BUFFERS: Lazy<OpenBuffers> = Lazy::new(Arc::default);

async fn lookup_buffer(
    path: impl AsRef<std::path::Path> + Send,
    create_if_not_found: bool,
) -> Option<Arc<RwLock<Vec<u8>>>> {
    let mut open_buffers = OPEN_BUFFERS.lock().await;
    if let Some(existing_buffer) = open_buffers.get(path.as_ref()).and_then(Weak::upgrade) {
        Some(existing_buffer)
    } else if create_if_not_found {
        let new_buffer = Arc::default();
        open_buffers.insert(path.as_ref().to_path_buf(), Arc::downgrade(&new_buffer));
        Some(new_buffer)
    } else {
        None
    }
}

#[allow(clippy::cast_possible_truncation)]
#[async_trait(?Send)]
impl AsyncFile for MemoryFile {
    type Manager = MemoryFileManager;
    fn path(&self) -> Arc<PathBuf> {
        self.path.clone()
    }

    async fn read(path: impl AsRef<std::path::Path> + Send + 'async_trait) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            buffer: lookup_buffer(path, true).await.unwrap(),
        })
    }

    async fn append(
        path: impl AsRef<std::path::Path> + Send + 'async_trait,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            buffer: lookup_buffer(path, true).await.unwrap(),
        })
    }

    async fn read_at(
        &mut self,
        position: u64,
        mut buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        if len > buffer.len() {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    Error::message("buffer length < len"),
                ))),
                buffer,
            );
        }

        let file_buffer = self.buffer.read().await;

        let read_end = position as usize + len;
        if read_end > file_buffer.len() {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    Error::message("read requested more bytes than available"),
                ))),
                buffer,
            );
        }

        buffer[offset..offset + len].copy_from_slice(&file_buffer[position as usize..read_end]);

        (Ok(len), buffer)
    }

    async fn write_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        let mut file_buffer = self.buffer.write().await;
        // Our database only uses append-only files. By asserting this is true, we can simply use Write
        assert_eq!(position as usize, file_buffer.len());

        (
            file_buffer
                .write_all(&buffer[offset..offset + len])
                .map(|_| len)
                .map_err(Error::from),
            buffer,
        )
    }

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct MemoryFileManager {
    open_files: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<MemoryFile>>>>>,
}

#[async_trait(?Send)]
impl AsyncFileManager<MemoryFile> for MemoryFileManager {
    type FileHandle = OpenMemoryFile;
    async fn append(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<Self::FileHandle, Error> {
        let mut open_files = self.open_files.lock().await;
        if let Some(open_file) = open_files.get(path.as_ref()) {
            Ok(OpenMemoryFile(open_file.clone()))
        } else {
            let file = Arc::new(Mutex::new(MemoryFile::append(path.as_ref()).await?));
            open_files.insert(path.as_ref().to_path_buf(), file.clone());
            Ok(OpenMemoryFile(file))
        }
    }

    fn run<R, Fut: Future<Output = R>>(future: Fut) -> R {
        tokio::runtime::Runtime::new().unwrap().block_on(future)
    }

    async fn read(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<Self::FileHandle, Error> {
        self.append(path).await
    }

    async fn file_length(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<u64, Error> {
        let buffer = lookup_buffer(path, false).await.ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                Error::message("not found"),
            ))
        })?;
        let buffer = buffer.read().await;
        Ok(buffer.len() as u64)
    }
}
// TODO async file manager: For uring, does nothing. For tokio, manages access to open files.

pub struct OpenMemoryFile(Arc<Mutex<MemoryFile>>);

#[async_trait(?Send)]
impl OpenableFile<MemoryFile> for OpenMemoryFile {
    async fn write<W: FileOp<MemoryFile>>(&mut self, mut writer: W) -> Result<W::Output, Error> {
        let mut file = self.0.lock().await;
        writer.write(&mut file).await
    }

    async fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}
