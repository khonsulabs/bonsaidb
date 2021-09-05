use std::{
    collections::HashMap,
    convert::TryFrom,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use super::{AsyncFile, AsyncFileManager, FileWriter, OpenableFile};
use crate::{try_with_buffer, Error};

pub struct TokioFile {
    position: u64,
    file: File,
}

#[async_trait(?Send)]
impl AsyncFile for TokioFile {
    type Manager = TokioFileManager;
    async fn read(path: impl AsRef<std::path::Path> + Send + 'async_trait) -> Result<Self, Error> {
        Ok(Self {
            position: 0,
            file: File::open(path).await?,
        })
    }

    async fn append(
        path: impl AsRef<std::path::Path> + Send + 'async_trait,
    ) -> Result<Self, Error> {
        let length = fs::metadata(&path.as_ref())
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        Ok(Self {
            position: length,
            file: OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(path)
                .await?,
        })
    }

    async fn read_at(
        &mut self,
        position: u64,
        mut buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        // Seek to the appropriate location
        let delta = i64::try_from(position).unwrap() - i64::try_from(self.position).unwrap();
        if delta != 0 {
            self.position =
                try_with_buffer!(buffer, self.file.seek(SeekFrom::Current(delta)).await);
        }

        let bytes_read = try_with_buffer!(
            buffer,
            self.file.read(&mut buffer[offset..offset + len]).await
        );
        self.position += bytes_read as u64;
        (Ok(bytes_read), buffer)
    }

    async fn write_at(
        &mut self,
        position: u64,
        buffer: Vec<u8>,
        offset: usize,
        len: usize,
    ) -> (Result<usize, Error>, Vec<u8>) {
        // Seek to the appropriate location
        let delta = i64::try_from(self.position).unwrap() - i64::try_from(position).unwrap();
        if delta != 0 {
            self.position =
                try_with_buffer!(buffer, self.file.seek(SeekFrom::Current(delta)).await);
        }

        let bytes_read =
            try_with_buffer!(buffer, self.file.write(&buffer[offset..offset + len]).await);
        self.position += bytes_read as u64;
        (Ok(bytes_read), buffer)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        self.file.flush().await.map_err(Error::from)
    }

    async fn close(mut self) -> Result<(), Error> {
        // Closing is done by just dropping it
        self.flush().await.map_err(Error::from)
    }
}

#[derive(Default, Clone)]
pub struct TokioFileManager {
    open_files: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<TokioFile>>>>>,
}

#[async_trait(?Send)]
impl AsyncFileManager<TokioFile> for TokioFileManager {
    type FileHandle = OpenTokioFile;
    async fn append(
        &self,
        path: impl AsRef<Path> + Send + 'async_trait,
    ) -> Result<Self::FileHandle, Error> {
        let mut open_files = self.open_files.lock().await;
        match open_files.get(path.as_ref()) {
            Some(open_file) => Ok(OpenTokioFile(open_file.clone())),
            None => {
                let file = Arc::new(Mutex::new(TokioFile::append(path.as_ref()).await?));
                open_files.insert(path.as_ref().to_path_buf(), file.clone());
                Ok(OpenTokioFile(file))
            }
        }
    }
}
// TODO async file manager: For uring, does nothing. For tokio, manages access to open files.

pub struct OpenTokioFile(Arc<Mutex<TokioFile>>);

#[async_trait(?Send)]
impl OpenableFile<TokioFile> for OpenTokioFile {
    async fn write<W: FileWriter<TokioFile>>(&mut self, writer: W) -> Result<(), Error> {
        let mut file = self.0.lock().await;
        writer.write(&mut file).await
    }

    async fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}
