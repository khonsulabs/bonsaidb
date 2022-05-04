//! Large file storage support for BonsaiDb.
//!
//! This crate provides support for storing large files in
//! [BonsaiDb](https://bonsaidb.io/). While BonsaiDb's document size limit is 4
//! gigabytes, the requirement that each document is loaded in memory fully can
//! cause higher memory usage when storing larger files.
//!
//! # `FileConfig`
//!
//! The [`FileConfig`] trait allows customizing the [`CollectionName`]s and
//! block size. If you want to use smaller or larger blocks, you can. If you
//! want to store more than one set of files in the same database, you can use
//! two [`FileConfig`] implementors with different [`CollectionName`]s.
//!
//! For most users, the provided implementation [`BonsaiFiles`] will work for
//! them.
//!
//! # Basic Example
//!
//! ```rust
#![doc = include_str!("../examples/basic-files.rs")]
//! ```
//! 
//! # Async Support
//!
//! This crate adds implementations of `tokio::io::AsyncRead` and
//! `tokio::io::AsyncWrite` when the `async` feature flag is enabled.
//! ```rust
#![cfg_attr(feature = "async", doc = include_str!("../examples/basic-files-async.rs"))]
//! ```

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

use std::{fmt::Debug, marker::PhantomData};

#[cfg(feature = "async")]
use bonsaidb_core::async_trait::async_trait;
#[cfg(feature = "async")]
use bonsaidb_core::connection::AsyncConnection;
use bonsaidb_core::{
    connection::Connection,
    key::time::TimestampAsNanoseconds,
    schema::{CollectionName, InsertError, Qualified, Schema, SchemaName, Schematic},
};
use derive_where::derive_where;
use serde::{de::DeserializeOwned, Serialize};

mod schema;

/// Types for accessing files directly from a connection to a database. These
/// types perform no permission checking beyond what BonsaiDb normally checks as
/// part of accessing/updating the underlying collections.
pub mod direct;

/// A configuration for a set of [stored files](direct::File).
#[cfg_attr(feature = "async", async_trait)]
pub trait FileConfig: Sized + Send + Sync + Unpin + 'static {
    /// The type of the `metadata` stored in [`File`](direct::File). If you do
    /// not need to store metadata, you can set this type to `()`.
    type Metadata: Serialize + DeserializeOwned + Send + Sync + Debug + Clone;

    /// The maximum size for each write to an underlying file. The file will be
    /// stored by breaking the data written into chunks no larger than
    /// `BLOCK_SIZE`.
    const BLOCK_SIZE: usize;
    /// Returns the unique collection name to use to store [`File`s][direct::File].
    fn files_name() -> CollectionName;
    /// Returns the unique collection name to use to store file blocks.
    fn blocks_name() -> CollectionName;

    /// Registers the collections for this configuration into `schema`.
    fn register_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_collection::<schema::file::File<Self>>()?;
        schema.define_collection::<schema::block::Block<Self>>()?;

        Ok(())
    }

    /// Builds a new file. If `name_or_path` starts with a `/`, the argument is
    /// treated as a full path to the file being built. Otherwise, the argument
    /// is treated as the file's name.
    fn build<NameOrPath: AsRef<str>>(
        name_or_path: NameOrPath,
    ) -> direct::FileBuilder<'static, Self> {
        direct::FileBuilder::new(name_or_path)
    }

    /// Returns the file with the unique `id` given, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    fn get<Database: Connection + Clone>(
        id: u32,
        database: &Database,
    ) -> Result<Option<direct::File<direct::Blocking<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::get(id, database)
    }

    /// Returns the file located at `path`, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    fn load<Database: Connection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Option<direct::File<direct::Blocking<Database>, Self>>, Error> {
        direct::File::<_, Self>::load(path, database)
    }

    /// Returns the file locate at `path`, or creates an empty file if not
    /// currently present.
    ///
    /// If `expect_present` is true, this function will first check for an
    /// existing file before attempting to create the file. This parameter is
    /// purely an optimization, and the function will work regardless of the
    /// value. Pass true if you expect the file to be present a majority of the
    /// time this function is invoked. For example, using this function to
    /// retrieve a file created once and append to the same path in the future,
    /// passing true will make this function slightly more optimized for the
    /// most common flow.
    ///
    /// Regardless whether `expect_present` is true or false, this function will
    /// proceed by attempting to create a file at `path`, relying on BonsaiDb's
    /// ACID-compliance to notify of a conflict if another request succeeds
    /// before this one. If a conflict occurs, this function will then attempt
    /// to load the document. If the document has been deleted, the
    /// [`Error::Deleted`] will be returned.
    fn load_or_create<Database: Connection + Clone>(
        path: &str,
        expect_present: bool,
        database: &Database,
    ) -> Result<direct::File<direct::Blocking<Database>, Self>, Error> {
        // First, try loading the file if we expect the file will be present
        // (ie, a singleton file that is always preseent after the first
        // launch).
        if expect_present {
            if let Some(file) = direct::File::<_, Self>::load(path, database)? {
                return Ok(file);
            }
        }

        // File not found, or we are going to assume the file isn't present.
        match Self::build(path).create(database) {
            Ok(file) => Ok(file),
            Err(Error::AlreadyExists) => {
                // Rather than continue to loop, we will just propogate the
                // previous error in the situation where the file was deleted
                // between our failed attempt to create and the attempt to
                // retrieve the conflicted document.
                direct::File::<_, Self>::load(path, database)?.ok_or(Error::Deleted)
            }
            Err(other) => Err(other),
        }
    }

    /// Deletes the file at `path`. Returns true if a file was deleted. Does not
    /// error if the file is not found.
    fn delete<Database: Connection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<bool, Error> {
        if let Some(file) = direct::File::<_, Self>::load(path, database)? {
            file.delete()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns all files that have a containing path of exactly `path`. It will
    /// only return files that have been created, and will not return "virtual"
    /// directories that are part of a file's path but have never been created.
    ///
    /// This function only loads metadata about the files, it does not load the
    /// contents of the files.
    fn list<Database: Connection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<direct::File<direct::Blocking<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list(path, database)
    }

    /// Returns all files that have a path starting with `path`.
    ///
    /// This function only loads metadata about the files, it does not load the
    /// contents of the files.
    fn list_recursive<Database: Connection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<direct::File<direct::Blocking<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list_recursive(path, database)
    }

    /// Returns statistics for all files contained within this collection. This
    /// is equivalent to calling [`Self::stats_for_path`] with `"/"` for the
    /// path.
    fn stats<Database: Connection + Clone>(
        database: &Database,
    ) -> Result<Statistics, bonsaidb_core::Error> {
        Self::stats_for_path("/", database)
    }

    /// Returns statistics for all files whose path starts with `path`.
    fn stats_for_path<Database: Connection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Statistics, bonsaidb_core::Error> {
        direct::File::<_, Self>::stats_for_path(path, database)
    }

    /// Returns the file with the unique `id` given, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    #[cfg(feature = "async")]
    async fn get_async<Database: AsyncConnection + Clone>(
        id: u32,
        database: &Database,
    ) -> Result<Option<direct::File<direct::Async<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::get_async(id, database).await
    }

    /// Returns the file located at `path`, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    #[cfg(feature = "async")]
    async fn load_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Option<direct::File<direct::Async<Database>, Self>>, Error> {
        direct::File::<_, Self>::load_async(path, database).await
    }

    /// Returns the file locate at `path`, or creates an empty file if not
    /// currently present.
    ///
    /// If `expect_present` is true, this function will first check for an
    /// existing file before attempting to create the file. This parameter is
    /// purely an optimization, and the function will work regardless of the
    /// value. Pass true if you expect the file to be present a majority of the
    /// time this function is invoked. For example, using this function to
    /// retrieve a file created once and append to the same path in the future,
    /// passing true will make this function slightly more optimized for the
    /// most common flow.
    ///
    /// Regardless whether `expect_present` is true or false, this function will
    /// proceed by attempting to create a file at `path`, relying on BonsaiDb's
    /// ACID-compliance to notify of a conflict if another request succeeds
    /// before this one. If a conflict occurs, this function will then attempt
    /// to load the document. If the document has been deleted, the
    /// [`Error::Deleted`] will be returned.
    #[cfg(feature = "async")]
    async fn load_or_create_async<Database: AsyncConnection + Clone>(
        path: &str,
        expect_present: bool,
        database: &Database,
    ) -> Result<direct::File<direct::Async<Database>, Self>, Error> {
        // First, try loading the file if we expect the file will be present
        // (ie, a singleton file that is always preseent after the first
        // launch).
        if expect_present {
            if let Some(file) = direct::File::<_, Self>::load_async(path, database).await? {
                return Ok(file);
            }
        }

        // File not found, or we are going to assume the file isn't present.
        match Self::build(path).create_async(database).await {
            Ok(file) => Ok(file),
            Err(Error::AlreadyExists) => {
                // Rather than continue to loop, we will just propogate the
                // previous error in the situation where the file was deleted
                // between our failed attempt to create and the attempt to
                // retrieve the conflicted document.
                direct::File::<_, Self>::load_async(path, database)
                    .await?
                    .ok_or(Error::Deleted)
            }
            Err(other) => Err(other),
        }
    }

    /// Deletes the file at `path`. Returns true if a file was deleted. Does not
    /// error if the file is not found.
    #[cfg(feature = "async")]
    async fn delete_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<bool, Error> {
        if let Some(file) = direct::File::<_, Self>::load_async(path, database).await? {
            file.delete().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns all files that have a containing path of exactly `path`. It will
    /// only return files that have been created, and will not return "virtual"
    /// directories that are part of a file's path but have never been created.
    ///
    /// This function only loads metadata about the files, it does not load the
    /// contents of the files.
    #[cfg(feature = "async")]
    async fn list_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<direct::File<direct::Async<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list_async(path, database).await
    }

    /// Returns all files that have a path starting with `path`.
    ///
    /// This function only loads metadata about the files, it does not load the
    /// contents of the files.
    #[cfg(feature = "async")]
    async fn list_recursive_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<direct::File<direct::Async<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list_recursive_async(path, database).await
    }

    /// Returns statistics for all files contained within this collection. This
    /// is equivalent to calling [`Self::stats_for_path_async`] with `"/"` for the
    /// path.
    #[cfg(feature = "async")]
    async fn stats_async<Database: AsyncConnection + Clone>(
        database: &Database,
    ) -> Result<Statistics, bonsaidb_core::Error> {
        Self::stats_for_path_async("/", database).await
    }

    /// Returns statistics for all files whose path starts with `path`.
    #[cfg(feature = "async")]
    async fn stats_for_path_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: &Database,
    ) -> Result<Statistics, bonsaidb_core::Error> {
        direct::File::<_, Self>::stats_for_path_async(path, database).await
    }
}

/// A default configuration for storing files within BonsaiDb.
#[derive(Debug)]
pub struct BonsaiFiles;

impl FileConfig for BonsaiFiles {
    type Metadata = ();
    const BLOCK_SIZE: usize = 65_536;

    fn files_name() -> CollectionName {
        CollectionName::new("bonsaidb", "files")
    }
    fn blocks_name() -> CollectionName {
        CollectionName::new("bonsaidb", "blocks")
    }
}

/// A schema implementation that allows using any [`FileConfig`] as a [`Schema`]
/// without manually implementing [`Schema`].
#[derive_where(Default, Debug)]
pub struct FilesSchema<Config: FileConfig = BonsaiFiles>(PhantomData<Config>);

impl<Config: FileConfig> Schema for FilesSchema<Config> {
    fn schema_name() -> SchemaName {
        SchemaName::from(Config::files_name())
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        Config::register_collections(schema)
    }
}

/// Errors that can be returned when interacting with files.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An underlying database error was returned.
    #[error("database error: {0}")]
    Database(bonsaidb_core::Error),
    /// A name contained an invalid character. Currently, the only disallowed
    /// character is `/`.
    #[error("names must not contain '/'")]
    InvalidName,
    /// An absolute path was expected, but the path provided did not include a
    /// leading `/`.
    #[error("all paths must start with a leading '/'")]
    InvalidPath,
    /// An attempt at creating a file failed because a file already existed.
    #[error("a file already exists at the path provided")]
    AlreadyExists,
    /// The file was deleted during the operation.
    #[error("the file was deleted during the operation")]
    Deleted,
}

impl<T> From<InsertError<T>> for Error {
    fn from(err: InsertError<T>) -> Self {
        Self::from(err.error)
    }
}

impl From<bonsaidb_core::Error> for Error {
    fn from(err: bonsaidb_core::Error) -> Self {
        match err {
            bonsaidb_core::Error::UniqueKeyViolation { .. } => Self::AlreadyExists,
            other => Self::Database(other),
        }
    }
}

impl From<Error> for bonsaidb_core::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Database(err) => err,
            other => Self::other("bonsaidb-files", other),
        }
    }
}

/// Controls which location of a file to remove data from during a truncation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Truncate {
    /// Remove data from the start (head) of the file when truncating.
    RemovingStart,
    /// Remove data from the end (tail) of the file when truncating.
    RemovingEnd,
}

/// Statistics about a set of files contained in a collection.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Statistics {
    /// The total number of bytes contained within the path queried. This only
    /// counts bytes that would be returned when reading the contents of files.
    /// No metadata information is included in this calculation.
    pub total_bytes: u64,
    /// The number of files contained within the path queried.
    pub file_count: usize,
    /// The last timestamp data was appended to a file within the path queried.
    /// This contains `None` if there is no data present.
    pub last_appended_at: Option<TimestampAsNanoseconds>,
}

#[cfg(test)]
mod tests;
