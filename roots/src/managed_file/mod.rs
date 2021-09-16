use std::{
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::error::Error;

pub mod fs;
pub mod memory;

/// A file that can be interacted with using async operations.
///
/// This trait is an abstraction that mimics `tokio-uring`'s File type, allowing
/// for a non-uring implementation to be provided as well. This is why the
/// read/write APIs take ownership of the buffer -- to satisfy the requirements
/// of tokio-uring.
pub trait ManagedFile: Seek + Read + Write + Sized {
    /// The file manager that synchronizes file access across threads.
    type Manager: FileManager<Self>;

    /// Returns a shared reference to the path for the file.
    fn path(&self) -> Arc<PathBuf>;

    /// Opens a file at `path` with read-only permission.
    fn open_for_read(path: impl AsRef<Path> + Send) -> Result<Self, Error>;
    /// Opens or creates a file at `path`, positioning the cursor at the end of the file.
    fn open_for_append(path: impl AsRef<Path> + Send) -> Result<Self, Error>;

    /// Safely closes the file after flushing any pending operations to disk.
    fn close(self) -> Result<(), Error>;
}

/// A manager that is responsible for controlling write access to a file.
pub trait FileManager<F: ManagedFile>: Send + Sync + Clone + Default {
    /// A file handle type, which can have operations executed against it.
    type FileHandle: OpenableFile<F>;

    /// Returns a file handle that can be used for reading operations.
    fn read(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error>;

    /// Returns a file handle that can be used to read and write.
    fn append(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error>;

    /// Returns the length of the file.
    fn file_length(&self, path: impl AsRef<Path> + Send) -> Result<u64, Error> {
        path.as_ref()
            .metadata()
            .map_err(Error::from)
            .map(|metadata| metadata.len())
    }
}

/// A file that can have operations performed on it.
pub trait OpenableFile<F: ManagedFile> {
    /// Executes an operation.
    fn execute<W: FileOp<F>>(&mut self, operator: W) -> Result<W::Output, Error>;

    /// Closes the file. This may not actually close the underlying file,
    /// depending on what other tasks have access to the underlying file as
    /// well.
    fn close(self) -> Result<(), Error>;
}

/// An operation to perform on a file.
pub trait FileOp<F: ManagedFile> {
    /// The output type of the operation.
    type Output;

    /// Executes the operation and returns the result.
    fn execute(&mut self, file: &mut F) -> Result<Self::Output, Error>;
}