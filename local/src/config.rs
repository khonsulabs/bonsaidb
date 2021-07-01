use crate::vault::AnyMasterKeyStorage;

/// Configuration options for [`Storage`](crate::storage::Storage).
#[derive(Debug)]
pub struct Configuration {
    // TODO this isn't a server anymore.
    /// The unique id of the server. If not specified, the server will randomly
    /// generate a unique id on startup. If the server generated an id and this
    /// value is subsequently set, the generated id will be overridden by the
    /// one specified here.
    pub unique_id: Option<u64>,
    /// The master key storage to use with the vault. If not specified and
    /// running in debug mode,
    /// [`LocalMasterKeyStorage`](crate::vault::LocalMasterKeyStorage) will be
    /// used with the server's data folder as the path.
    pub master_key_storage: Option<Box<dyn AnyMasterKeyStorage>>,
    /// Configuration options related to background tasks.
    pub workers: Tasks,

    /// Configuration options related to views.
    pub views: Views,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            unique_id: None,
            master_key_storage: None,
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
