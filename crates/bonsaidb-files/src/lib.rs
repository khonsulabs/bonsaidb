//! Large file storage support for BonsaiDb.
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

use std::marker::PhantomData;

#[cfg(feature = "async")]
use bonsaidb_core::async_trait::async_trait;
#[cfg(feature = "async")]
use bonsaidb_core::connection::AsyncConnection;
use bonsaidb_core::{
    connection::Connection,
    schema::{CollectionName, InsertError, Qualified, Schema, SchemaName, Schematic},
};
use derive_where::derive_where;

mod schema;

/// Types for accessing files directly from a connection to a database. These
/// types perform no permission checking beyond what BonsaiDb normally checks as
/// part of accessing/updating the underlying collections.
pub mod direct;

/// A configuration for a set of [stored files](direct::File).
#[cfg_attr(feature = "async", async_trait)]
pub trait FileConfig: Sized + Send + Sync + Unpin + 'static {
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

    /// Builds a new file named `name`.
    fn build<Name: Into<String>>(name: Name) -> direct::FileBuilder<'static, Self> {
        direct::FileBuilder::named(name)
    }

    /// Returns the file with the unique `id` given, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    fn get<Database: Connection + Clone>(
        id: u32,
        database: Database,
    ) -> Result<Option<direct::File<direct::Blocking<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::get(id, database)
    }

    /// Returns the file located at `path`, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    fn load<Database: Connection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Option<direct::File<direct::Blocking<Database>, Self>>, Error> {
        direct::File::<_, Self>::load(path, database)
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

    /// Returns the file with the unique `id` given, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    #[cfg(feature = "async")]
    async fn get_async<Database: AsyncConnection + Clone>(
        id: u32,
        database: Database,
    ) -> Result<Option<direct::File<direct::Async<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::get_async(id, database).await
    }

    /// Returns the file located at `path`, if found. This function
    /// only loads metadata about the file, it does not load the contents of the
    /// file.
    #[cfg(feature = "async")]
    async fn load_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Option<direct::File<direct::Async<Database>, Self>>, Error> {
        direct::File::<_, Self>::load_async(path, database).await
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
}

/// A default configuration for storing files within BonsaiDb.
#[derive(Debug)]
pub struct BonsaiFiles;

impl FileConfig for BonsaiFiles {
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
    Database(#[from] bonsaidb_core::Error),
    /// A name contained an invalid character. Currently, the only disallowed
    /// character is `/`.
    #[error("names must not contain '/'")]
    InvalidName,
    /// An absolute path was expected, but the path provided did not include a
    /// leading `/`.
    #[error("all paths must start with a leading '/'")]
    InvalidPath,
}

impl<T> From<InsertError<T>> for Error {
    fn from(err: InsertError<T>) -> Self {
        Self::Database(err.error)
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

#[cfg(test)]
mod tests;
