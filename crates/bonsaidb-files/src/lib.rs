use std::marker::PhantomData;

use bonsaidb_core::schema::{
    CollectionName, InsertError, Qualified, Schema, SchemaName, Schematic,
};
use derive_where::derive_where;

mod schema;

pub mod direct;

pub trait FileConfig: Sized + Send + Sync + Unpin + 'static {
    const BLOCK_SIZE: usize;
    fn files_name() -> CollectionName;
    fn blocks_name() -> CollectionName;

    fn register_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_collection::<schema::file::File<Self>>()?;
        schema.define_collection::<schema::block::Block<Self>>()?;

        Ok(())
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
