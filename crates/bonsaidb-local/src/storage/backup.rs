use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use bonsaidb_core::{
    admin,
    connection::{Connection, Range, Sort, StorageConnection},
    schema::{Collection, SchemaName},
    transaction::{Operation, Transaction},
    AnyError,
};
use futures::Future;
use tokio::fs::DirEntry;

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
        container: &str,
        name: &str,
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
                // The admin database is already going to be created by the process of creating a database.
                self.create_database_with_schema(&database, schema.clone(), database == "_admin")
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
        // Restore all the collections. However, there's one collection we don't
        // want to restore: the Databases list. This will be recreated during
        // the process of restoring the backup, so we skip it.
        let database_collection = admin::Database::collection_name()?;
        for collection in database
            .schematic()
            .collections()
            .into_iter()
            .filter(|c| c != &database_collection)
        {
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

#[async_trait]
impl<'a> BackupLocation for &'a Path {
    type Error = std::io::Error;

    async fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Self::Error> {
        let container_folder = container_folder(self, schema, database_name, container);
        tokio::fs::create_dir_all(&container_folder).await?;
        tokio::fs::write(container_folder.join(name), object).await?;

        Ok(())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaName>, Self::Error> {
        iterate_directory(self, |entry, file_name| async move {
            if entry.file_type().await?.is_dir() {
                if let Ok(schema_name) = SchemaName::try_from(file_name.as_str()) {
                    return Ok(Some(schema_name));
                }
            }
            Ok(None)
        })
        .await
    }

    async fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Self::Error> {
        iterate_directory(
            &schema_folder(self, schema),
            |entry, file_name| async move {
                if entry.file_type().await?.is_dir() && file_name != "_kv" {
                    return Ok(Some(file_name));
                }
                Ok(None)
            },
        )
        .await
    }

    async fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Self::Error> {
        iterate_directory(
            &container_folder(self, schema, database_name, container),
            |entry, file_name| async move {
                if entry.file_type().await?.is_file() {
                    return Ok(Some(file_name));
                }
                Ok(None)
            },
        )
        .await
    }

    async fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Self::Error> {
        tokio::fs::read(container_folder(self, schema, database_name, container).join(name)).await
    }
}

async fn iterate_directory<
    T,
    F: FnMut(DirEntry, String) -> Fut,
    Fut: Future<Output = Result<Option<T>, std::io::Error>>,
>(
    path: &Path,
    mut callback: F,
) -> Result<Vec<T>, std::io::Error> {
    let mut collected = Vec::new();
    let mut directories =
        if let Some(directories) = tokio::fs::read_dir(path).await.ignore_not_found()? {
            directories
        } else {
            return Ok(collected);
        };

    while let Some(entry) = directories.next_entry().await.ignore_not_found()?.flatten() {
        if let Ok(file_name) = entry.file_name().into_string() {
            if let Some(result) = callback(entry, file_name).await? {
                collected.push(result);
            }
        }
    }

    Ok(collected)
}

trait IoResultExt<T>: Sized {
    fn ignore_not_found(self) -> Result<Option<T>, std::io::Error>;
}

impl<T> IoResultExt<T> for Result<T, std::io::Error> {
    fn ignore_not_found(self) -> Result<Option<T>, std::io::Error> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(err)
                }
            }
        }
    }
}

fn schema_folder(base: &Path, schema: &SchemaName) -> PathBuf {
    base.join(schema.to_string())
}

fn database_folder(base: &Path, schema: &SchemaName, database_name: &str) -> PathBuf {
    schema_folder(base, schema).join(database_name)
}

fn container_folder(
    base: &Path,
    schema: &SchemaName,
    database_name: &str,
    container: &str,
) -> PathBuf {
    database_folder(base, schema, database_name).join(container)
}

#[cfg(test)]
mod tests {
    use bonsaidb_core::{
        connection::Connection as _,
        keyvalue::KeyValue,
        test_util::{Basic, TestDirectory},
    };

    use super::*;
    use crate::config::{Builder, StorageConfiguration};

    #[tokio::test]
    async fn backup_restore() -> anyhow::Result<()> {
        let backup_destination = TestDirectory::new("backup-restore.bonsaidb.backup");

        // First, create a database that we'll be restoring. `TestDirectory`
        // will automatically erase the database when it drops out of scope,
        // which is why we're creating a nested scope here.
        let test_doc = {
            let database_directory = TestDirectory::new("backup-restore.bonsaidb");
            let storage = Storage::open(StorageConfiguration::new(&database_directory)).await?;
            storage.register_schema::<Basic>().await?;
            storage.create_database::<Basic>("basic", false).await?;
            let db = storage.database::<Basic>("basic").await?;
            let test_doc = db
                .collection::<Basic>()
                .push(&Basic::new("somevalue"))
                .await?;
            db.set_numeric_key("somekey", 1_u64).await?;

            storage.backup(&*backup_destination.0).await.unwrap();

            test_doc
        };

        // `backup_destination` now contains an export of the database, time to try loading it:
        let database_directory = TestDirectory::new("backup-restore.bonsaidb");
        let restored_storage =
            Storage::open(StorageConfiguration::new(&database_directory)).await?;
        restored_storage.register_schema::<Basic>().await?;
        restored_storage
            .restore(&*backup_destination.0)
            .await
            .unwrap();

        let db = restored_storage.database::<Basic>("basic").await?;
        let doc = db
            .get::<Basic>(test_doc.id)
            .await?
            .expect("Backed up document.not found");
        let contents = doc.contents::<Basic>()?;
        assert_eq!(contents.value, "somevalue");
        assert_eq!(db.get_key("somekey").into_u64().await?, Some(1));

        // Calling restore again should generate an error.
        assert!(restored_storage
            .restore(&*backup_destination.0)
            .await
            .is_err());

        Ok(())
    }
}
