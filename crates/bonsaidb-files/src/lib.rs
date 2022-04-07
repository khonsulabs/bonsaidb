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

pub mod direct;

#[cfg_attr(feature = "async", async_trait)]
pub trait FileConfig: Sized + Send + Sync + Unpin + 'static {
    const BLOCK_SIZE: usize;
    fn files_name() -> CollectionName;
    fn blocks_name() -> CollectionName;

    fn register_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_collection::<schema::file::File<Self>>()?;
        schema.define_collection::<schema::block::Block<Self>>()?;

        Ok(())
    }

    fn build<Name: Into<String>>(name: Name) -> direct::CreateFile<'static, Self> {
        direct::CreateFile::named(name)
    }

    fn get<Database: Connection + Clone>(
        id: u32,
        database: Database,
    ) -> Result<Option<direct::File<direct::Blocking<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::get(id, database)
    }

    fn load<Database: Connection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Option<direct::File<direct::Blocking<Database>, Self>>, Error> {
        direct::File::<_, Self>::load(path, database)
    }

    fn list<Database: Connection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Vec<direct::File<direct::Blocking<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list(path, database)
    }

    fn list_recursive<Database: Connection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Vec<direct::File<direct::Blocking<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list_recursive(path, database)
    }

    #[cfg(feature = "async")]
    async fn get_async<Database: AsyncConnection + Clone>(
        id: u32,
        database: Database,
    ) -> Result<Option<direct::File<direct::Async<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::get_async(id, database).await
    }

    #[cfg(feature = "async")]
    async fn load_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Option<direct::File<direct::Async<Database>, Self>>, Error> {
        direct::File::<_, Self>::load_async(path, database).await
    }

    #[cfg(feature = "async")]
    async fn list_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Vec<direct::File<direct::Async<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list_async(path, database).await
    }

    #[cfg(feature = "async")]
    async fn list_recursive_async<Database: AsyncConnection + Clone>(
        path: &str,
        database: Database,
    ) -> Result<Vec<direct::File<direct::Async<Database>, Self>>, bonsaidb_core::Error> {
        direct::File::<_, Self>::list_recursive_async(path, database).await
    }
}

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

#[derive_where(Default, Debug)]
pub struct FilesSchema<Config: FileConfig = BonsaiFiles>(PhantomData<Config>);

impl<Config: FileConfig> Schema for FilesSchema<Config> {
    fn schema_name() -> SchemaName {
        SchemaName::new("bonsaidb", "files")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        Config::register_collections(schema)
    }
}
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    Database(#[from] bonsaidb_core::Error),
    #[error("names must not contain '/'")]
    InvalidName,
    #[error("all paths must start with a leading '/'")]
    InvalidPath,
}

impl<T> From<InsertError<T>> for Error {
    fn from(err: InsertError<T>) -> Self {
        Self::Database(err.error)
    }
}

pub enum TruncateFrom {
    Start,
    End,
}

#[cfg(test)]
mod tests;
