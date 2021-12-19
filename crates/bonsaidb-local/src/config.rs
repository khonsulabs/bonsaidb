use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

#[cfg(feature = "encryption")]
use bonsaidb_core::document::KeyId;
use bonsaidb_core::schema::{Schema, SchemaName};

#[cfg(feature = "encryption")]
use crate::vault::AnyVaultKeyStorage;
use crate::{
    storage::{DatabaseOpener, StorageSchemaOpener},
    Error,
};

/// Configuration options for [`Storage`](crate::storage::Storage).
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct StorageConfiguration {
    /// The path to the database. Defaults to `db.bonsaidb` if not specified.
    pub path: Option<PathBuf>,

    /// The unique id of the server. If not specified, the server will randomly
    /// generate a unique id on startup. If the server generated an id and this
    /// value is subsequently set, the generated id will be overridden by the
    /// one specified here.
    pub unique_id: Option<u64>,

    /// The vault key storage to use. If not specified and running in debug
    /// mode, [`LocalVaultKeyStorage`](crate::vault::LocalVaultKeyStorage) will
    /// be used with the server's data folder as the path.
    #[cfg(feature = "encryption")]
    pub vault_key_storage: Option<Box<dyn AnyVaultKeyStorage>>,

    /// The default encryption key for the database. If specified, all documents
    /// will be stored encrypted at-rest using the key specified. Having this
    /// key specified will also encrypt views. Without this, views will be
    /// stored unencrypted.
    #[cfg(feature = "encryption")]
    pub default_encryption_key: Option<KeyId>,

    /// Configuration options related to background tasks.
    pub workers: Tasks,

    /// Configuration options related to views.
    pub views: Views,

    pub(crate) initial_schemas: HashMap<SchemaName, Box<dyn DatabaseOpener>>,
}

impl StorageConfiguration {
    /// Registers the schema provided.
    pub fn register_schema<S: Schema>(&mut self) -> Result<(), Error> {
        self.initial_schemas.insert(
            S::schema_name()?,
            Box::new(StorageSchemaOpener::<S>::new()?),
        );
        Ok(())
    }
}

/// Configujration options for background tasks.
#[derive(Debug)]
pub struct Tasks {
    /// Defines how many workers should be spawned to process tasks. Default
    /// value is `16`.
    pub worker_count: usize,
}

impl Default for Tasks {
    fn default() -> Self {
        Self {
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            worker_count: 16,
        }
    }
}

/// Configuration options for views.
#[derive(Clone, Debug, Default)]
pub struct Views {
    /// If true, the database will scan all views during the call to
    /// `open_local`. This will cause database opening to take longer, but once
    /// the database is open, no request will need to wait for the integrity to
    /// be checked. However, for faster startup time, you may wish to delay the
    /// integrity scan. Default value is `false`.
    pub check_integrity_on_open: bool,
}

/// Storage configuration builder methods.
pub trait Builder: Default {
    /// Creates a default configuration with `path` set.
    fn new<P: AsRef<Path>>(path: P) -> Self {
        Self::default().path(path)
    }

    /// Registers the schema and returns self.
    fn with_schema<S: Schema>(self) -> Result<Self, Error>;

    /// Sets [`StorageConfiguration::path`](StorageConfiguration#structfield.path) to `path` and returns self.
    fn path<P: AsRef<Path>>(self, path: P) -> Self;
    /// Sets [`StorageConfiguration::unique_id`](StorageConfiguration#structfield.unique_id) to `unique_id` and returns self.
    fn unique_id(self, unique_id: u64) -> Self;
    /// Sets [`StorageConfiguration::vault_key_storage`](StorageConfiguration#structfield.vault_key_storage) to `key_storage` and returns self.
    fn vault_key_storage<VaultKeyStorage: AnyVaultKeyStorage>(
        self,
        key_storage: VaultKeyStorage,
    ) -> Self;
    /// Sets [`StorageConfiguration::default_encryption_key`](StorageConfiguration#structfield.default_encryption_key) to `path` and returns self.
    fn default_encryption_key(self, key: KeyId) -> Self;
    /// Sets [`Tasks::worker_count`] to `worker_count` and returns self.
    fn tasks_worker_count(self, worker_count: usize) -> Self;
    /// Sets [`Views::check_integrity_on_open`] to `check` and returns self.
    fn check_view_integrity_on_open(self, check: bool) -> Self;
}

impl Builder for StorageConfiguration {
    fn with_schema<S: Schema>(mut self) -> Result<Self, Error> {
        self.register_schema::<S>()?;
        Ok(self)
    }

    fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().to_owned());
        self
    }

    fn unique_id(mut self, unique_id: u64) -> Self {
        self.unique_id = Some(unique_id);
        self
    }

    fn vault_key_storage<VaultKeyStorage: AnyVaultKeyStorage>(
        mut self,
        key_storage: VaultKeyStorage,
    ) -> Self {
        self.vault_key_storage = Some(Box::new(key_storage));
        self
    }

    fn default_encryption_key(mut self, key: KeyId) -> Self {
        self.default_encryption_key = Some(key);
        self
    }

    fn tasks_worker_count(mut self, worker_count: usize) -> Self {
        self.workers.worker_count = worker_count;
        self
    }

    fn check_view_integrity_on_open(mut self, check: bool) -> Self {
        self.views.check_integrity_on_open = check;
        self
    }
}
