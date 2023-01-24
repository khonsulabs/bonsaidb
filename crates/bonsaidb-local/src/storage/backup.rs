use std::{
    fs::DirEntry,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use bonsaidb_core::{
    admin,
    connection::{LowLevelConnection, Range, Sort, StorageConnection},
    document::DocumentId,
    schema::{Collection, Qualified, SchemaName},
    transaction::{Operation, Transaction},
    AnyError,
};

use crate::{
    database::{keyvalue::Entry, DatabaseNonBlocking},
    Database, Error, Storage,
};

/// A location to store and restore a database from.
pub trait BackupLocation: Send + Sync {
    /// The error type for the backup location.
    type Error: AnyError;

    /// Store `object` at `path` with `name`.
    fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Self::Error>;

    /// Lists all of the schemas stored in this backup location.
    fn list_schemas(&self) -> Result<Vec<SchemaName>, Self::Error>;

    /// List all of the names of the databases stored for `schema`.
    fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Self::Error>;

    /// List all stored named objects at `path`. The names should be the same that were provided when `store()` was called.
    fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Self::Error>;

    /// Load a previously stored object from `path` with `name`.
    fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Self::Error>;
}

impl Storage {
    /// Stores a copy of all data in this instance to `location`.
    pub fn backup<L: AnyBackupLocation>(&self, location: &L) -> Result<(), Error> {
        let databases = {
            self.instance
                .data
                .available_databases
                .read()
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };

        for name in databases {
            let database = self
                .instance
                .database_without_schema(&name, Some(self), None)?;
            Self::backup_database(&database, location)?;
        }

        Ok(())
    }

    /// Restores all data from a previously stored backup `location`.
    pub fn restore<L: AnyBackupLocation>(&self, location: &L) -> Result<(), Error> {
        for schema in location
            .list_schemas()
            .map_err(|err| Error::Backup(Box::new(err)))?
        {
            for database in location
                .list_databases(&schema)
                .map_err(|err| Error::Backup(Box::new(err)))?
            {
                // The admin database is already going to be created by the process of creating a database.
                self.create_database_with_schema(&database, schema.clone(), true)?;

                let database =
                    self.instance
                        .database_without_schema(&database, Some(self), None)?;
                Self::restore_database(&database, location)?;
            }
        }

        Ok(())
    }

    pub(crate) fn backup_database(
        database: &Database,
        location: &dyn AnyBackupLocation,
    ) -> Result<(), Error> {
        let schema = database.schematic().name.clone();
        for collection in database.schematic().collections() {
            let documents = database.list_from_collection(
                Range::from(..),
                Sort::Ascending,
                None,
                &collection,
            )?;
            let collection_name = collection.encoded();
            // TODO consider how to best parallelize -- perhaps a location can opt into parallelization?
            for document in documents {
                location.store(
                    &schema,
                    database.name(),
                    &collection_name,
                    &document.header.id.to_string(),
                    &document.contents,
                )?;
            }
            for ((namespace, key), entry) in database.all_key_value_entries()? {
                let full_name = format!("{}._key._{key}", namespace.as_deref().unwrap_or(""));
                location.store(
                    &schema,
                    database.name(),
                    "_kv",
                    &full_name,
                    &pot::to_vec(&entry)?,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn restore_database(
        database: &Database,
        location: &dyn AnyBackupLocation,
    ) -> Result<(), Error> {
        let schema = database.schematic().name.clone();
        let mut transaction = Transaction::new();
        // Restore all the collections. However, there's one collection we don't
        // want to restore: the Databases list. This will be recreated during
        // the process of restoring the backup, so we skip it.
        let database_collection = admin::Database::collection_name();
        for collection in database
            .schematic()
            .collections()
            .into_iter()
            .filter(|c| c != &database_collection)
        {
            let collection_name = collection.encoded();
            for (id, id_string) in location
                .list_stored(&schema, database.name(), &collection_name)?
                .into_iter()
                .filter_map(|id_string| {
                    id_string
                        .parse::<DocumentId>()
                        .ok()
                        .map(|id| (id, id_string))
                })
            {
                let contents =
                    location.load(&schema, database.name(), &collection_name, &id_string)?;
                transaction.push(Operation::insert(collection.clone(), Some(id), contents));
            }
        }
        database.apply_transaction(transaction)?;

        for full_key in location.list_stored(&schema, database.name(), "_kv")? {
            if let Some((namespace, key)) = full_key.split_once("._key._") {
                let entry = location.load(&schema, database.name(), "_kv", &full_key)?;
                let entry = pot::from_slice::<Entry>(&entry)?;
                let namespace = if namespace.is_empty() {
                    None
                } else {
                    Some(namespace.to_string())
                };
                entry.restore(namespace, key.to_string(), database)?;
            }
        }

        Ok(())
    }
}

pub trait AnyBackupLocation: Send + Sync {
    fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Error>;

    fn list_schemas(&self) -> Result<Vec<SchemaName>, Error>;

    fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Error>;

    fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Error>;

    fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Error>;
}

impl<L, E> AnyBackupLocation for L
where
    L: BackupLocation<Error = E>,
    E: AnyError,
{
    fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Error> {
        self.store(schema, database_name, container, name, object)
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    fn list_schemas(&self) -> Result<Vec<SchemaName>, Error> {
        self.list_schemas()
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Error> {
        self.list_databases(schema)
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Error> {
        self.list_stored(schema, database_name, container)
            .map_err(|err| Error::Backup(Box::new(err)))
    }

    fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Error> {
        self.load(schema, database_name, container, name)
            .map_err(|err| Error::Backup(Box::new(err)))
    }
}

impl BackupLocation for Path {
    type Error = std::io::Error;

    fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Self::Error> {
        let container_folder = container_folder(self, schema, database_name, container);
        std::fs::create_dir_all(&container_folder)?;
        std::fs::write(container_folder.join(name), object)?;

        Ok(())
    }

    fn list_schemas(&self) -> Result<Vec<SchemaName>, Self::Error> {
        iterate_directory(self, |entry, file_name| {
            if entry.file_type()?.is_dir() {
                if let Ok(schema_name) = SchemaName::parse_encoded(file_name.as_str()) {
                    return Ok(Some(schema_name));
                }
            }
            Ok(None)
        })
    }

    fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Self::Error> {
        iterate_directory(&schema_folder(self, schema), |entry, file_name| {
            if entry.file_type()?.is_dir() && file_name != "_kv" {
                return Ok(Some(file_name));
            }
            Ok(None)
        })
    }

    fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Self::Error> {
        iterate_directory(
            &container_folder(self, schema, database_name, container),
            |entry, file_name| {
                if entry.file_type()?.is_file() {
                    return Ok(Some(file_name));
                }
                Ok(None)
            },
        )
    }

    fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Self::Error> {
        std::fs::read(container_folder(self, schema, database_name, container).join(name))
    }
}

impl BackupLocation for PathBuf {
    type Error = std::io::Error;

    fn store(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
        object: &[u8],
    ) -> Result<(), Self::Error> {
        BackupLocation::store(
            self.as_path(),
            schema,
            database_name,
            container,
            name,
            object,
        )
    }

    fn list_schemas(&self) -> Result<Vec<SchemaName>, Self::Error> {
        BackupLocation::list_schemas(self.as_path())
    }

    fn list_databases(&self, schema: &SchemaName) -> Result<Vec<String>, Self::Error> {
        BackupLocation::list_databases(self.as_path(), schema)
    }

    fn list_stored(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
    ) -> Result<Vec<String>, Self::Error> {
        BackupLocation::list_stored(self.as_path(), schema, database_name, container)
    }

    fn load(
        &self,
        schema: &SchemaName,
        database_name: &str,
        container: &str,
        name: &str,
    ) -> Result<Vec<u8>, Self::Error> {
        BackupLocation::load(self.as_path(), schema, database_name, container, name)
    }
}

fn iterate_directory<T, F: FnMut(DirEntry, String) -> Result<Option<T>, std::io::Error>>(
    path: &Path,
    mut callback: F,
) -> Result<Vec<T>, std::io::Error> {
    let mut collected = Vec::new();
    let mut directories = if let Some(directories) = std::fs::read_dir(path).ignore_not_found()? {
        directories
    } else {
        return Ok(collected);
    };

    while let Some(entry) = directories
        .next()
        .map(IoResultExt::ignore_not_found)
        .transpose()?
        .flatten()
    {
        if let Ok(file_name) = entry.file_name().into_string() {
            if let Some(result) = callback(entry, file_name)? {
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
    base.join(schema.encoded())
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
        connection::{Connection as _, StorageConnection as _},
        keyvalue::KeyValue,
        schema::SerializedCollection,
        test_util::{Basic, TestDirectory},
    };

    use crate::{
        config::{Builder, KeyValuePersistence, PersistenceThreshold, StorageConfiguration},
        Storage,
    };

    #[test]
    fn backup_restore() -> anyhow::Result<()> {
        let backup_destination = TestDirectory::new("backup-restore.bonsaidb.backup");

        // First, create a database that we'll be restoring. `TestDirectory`
        // will automatically erase the database when it drops out of scope,
        // which is why we're creating a nested scope here.
        let test_doc = {
            let database_directory = TestDirectory::new("backup-restore.bonsaidb");
            let storage = Storage::open(
                StorageConfiguration::new(&database_directory)
                    .key_value_persistence(KeyValuePersistence::lazy([
                        PersistenceThreshold::after_changes(2),
                    ]))
                    .with_schema::<Basic>()?,
            )?;

            let db = storage.create_database::<Basic>("basic", false)?;
            let test_doc = db.collection::<Basic>().push(&Basic::new("somevalue"))?;
            db.set_numeric_key("key1", 1_u64).execute()?;
            db.set_numeric_key("key2", 2_u64).execute()?;
            // This key will not be persisted right away.
            db.set_numeric_key("key3", 3_u64).execute()?;

            storage.backup(&backup_destination.0).unwrap();

            test_doc
        };

        // `backup_destination` now contains an export of the database, time to try loading it:
        let database_directory = TestDirectory::new("backup-restore.bonsaidb");
        let restored_storage =
            Storage::open(StorageConfiguration::new(&database_directory).with_schema::<Basic>()?)?;
        restored_storage.restore(&backup_destination.0).unwrap();

        let db = restored_storage.database::<Basic>("basic")?;
        let doc = Basic::get(&test_doc.id, &db)?.expect("Backed up document.not found");
        assert_eq!(doc.contents.value, "somevalue");
        assert_eq!(db.get_key("key1").into_u64()?, Some(1));
        assert_eq!(db.get_key("key2").into_u64()?, Some(2));
        assert_eq!(db.get_key("key3").into_u64()?, Some(3));

        // Calling restore again should generate an error.
        assert!(restored_storage.restore(&backup_destination.0).is_err());

        Ok(())
    }
}
