#[cfg(feature = "encryption")]
use bonsaidb_core::document::KeyId;

#[cfg(feature = "encryption")]
use crate::vault::AnyVaultKeyStorage;

/// Configuration options for [`Storage`](crate::storage::Storage).
#[derive(Debug)]
pub struct Configuration {
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
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            #[cfg(feature = "encryption")]
            default_encryption_key: None,
            #[cfg(feature = "encryption")]
            vault_key_storage: None,
            unique_id: None,
            workers: Tasks::default(),
            views: Views::default(),
        }
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
#[derive(Clone, Debug)]
pub struct Views {
    /// If true, the database will scan all views during the call to
    /// `open_local`. This will cause database opening to take longer, but once
    /// the database is open, no request will need to wait for the integrity to
    /// be checked. However, for faster startup time, you may wish to delay the
    /// integrity scan. Default value is `false`.
    pub check_integrity_on_open: bool,
}

impl Default for Views {
    fn default() -> Self {
        Self {
            check_integrity_on_open: false,
        }
    }
}
