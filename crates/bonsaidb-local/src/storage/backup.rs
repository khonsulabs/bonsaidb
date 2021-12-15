use async_trait::async_trait;
use bonsaidb_core::{
    connection::{Connection, Range, Sort, StorageConnection},
    schema::SchemaName,
    transaction::{Operation, Transaction},
    AnyError,
};

use crate::{database::keyvalue::Entry, Database, Error, Storage};

/// A location to store and restore a database from.
#[async_trait]
pub trait BackupLocation: Send + Sync {
    /// The error type for the backup location.
    type Error: AnyError;

    /// Store `object` at `path` with `name`.
    async fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        name: &str,
        container: &str,
        object: &[u8],
    ) -> Result<(), Self::Error>;

    /// Lists all of the schemas stored in this backup location.
    async fn list_schemas(&self) -> Result<Vec<SchemaName>, Self::Error>;

    /// List all of the names of the databases stored for `schema`.
    async fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Self::Error>;

    /// List all stored named objects at `path`. The names should be the same that were provided when `store()` was called.
    async fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Self::Error>;

    /// Load a previously stored object from `path` with `name`.
    async fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Self::Error>;
}

impl Storage {
    /// Stores a copy of all data in this instance to `location`.
    pub async fn backup<L: BackupLocation>(&self, location: L) -> Result<(), Error> {
        let databases = {
            self.data
                .available_databases
                .read()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };

        for name in databases {
            let database = self.database_without_schema(&name).await?;
            database.backup(&location).await?;
        }

        Ok(())
    }

    /// Stores a copy of all data in this instance to `location`.
    pub async fn restore<L: BackupLocation>(&self, location: L) -> Result<(), Error> {
        for schema in location
            .list_schemas()
            .await
            .map_err(|err| Error::Backup(Box::new(err)))?
        {
            for database in location
                .list_databases(&schema)
                .await
                .map_err(|err| Error::Backup(Box::new(err)))?
            {
                self.create_database_with_schema(&database, schema.clone(), false)
                    .await?;

                let database = self.database_without_schema(&database).await?;
                database.restore(&location).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn backup_database(
        &self,
        database: &Database,
        location: &dyn AnyBackupLocation,
    ) -> Result<(), Error> {
        let schema = database.schematic().name.clone();
        for collection in database.schematic().collections() {
            let documents = database
                .list(Range::from(..), Sort::Ascending, None, &collection)
                .await?;
            // TODO consider how to best parallelize -- perhaps a location can opt into parallelization?
            for document in documents {
                location
                    .store(
                        &schema,
                        database.name(),
                        &collection.to_string(),
                        &document.id.to_string(),
                        &document.contents,
                    )
                    .await?;
            }
            for (key, entry) in database.all_key_value_entries().await? {
                location
                    .store(&schema, database.name(), "_kv", &key, &pot::to_vec(&entry)?)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn restore_database(
        &self,
        database: &Database,
        location: &dyn AnyBackupLocation,
    ) -> Result<(), Error> {
        let schema = database.schematic().name.clone();
        let mut transaction = Transaction::new();
        for collection in database.schematic().collections() {
            let collection_name = collection.to_string();
            for (id, id_string) in location
                .list_stored(&schema, database.name(), &collection_name)
                .await?
                .into_iter()
                .filter_map(|id_string| id_string.parse::<u64>().ok().map(|id| (id, id_string)))
            {
                let contents = location
                    .load(&schema, database.name(), &collection_name, &id_string)
                    .await?;
                transaction.push(Operation::insert(collection.clone(), Some(id), contents));
            }
        }
        database.apply_transaction(transaction).await?;

        for key in location
            .list_stored(&schema, database.name(), "_kv")
            .await?
        {
            let entry = location.load(&schema, database.name(), "_kv", &key).await?;
            let entry = pot::from_slice::<Entry>(&entry)?;
            entry.restore(key, database).await?;
        }

        Ok(())
    }
}

#[async_trait]
pub trait AnyBackupLocation: Send + Sync {
    async fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Error>;

    async fn list_schemas(&self) -> Result<Vec<SchemaName>, Error>;

    async fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Error>;

    async fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Error>;

    async fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Error>;
}

#[async_trait]
impl<L, E> AnyBackupLocation for L
where
    L: BackupLocation<Error = E>,
    E: AnyError,
{
    async fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Error> {
        self.store(schema, database_name, container, name, object)
            .await
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaName>, Error> {
        self.list_schemas()
            .await
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    async fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Error> {
        self.list_databases(schema)
            .await
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    async fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Error> {
        self.list_stored(schema, database_name, container)
            .await
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    async fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Error> {
        self.load(schema, database_name, container, name)
            .await
            .map_err(|err| Error::Backup(Box::new(err)))
    }
}
