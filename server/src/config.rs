use bonsaidb_core::permissions::Permissions;
pub use bonsaidb_local::config::Configuration as StorageConfiguration;

/// Configuration options for [`Server`](crate::Server)
pub struct Configuration {
    /// Number of sumultaneous requests a single client can have in flight at a
    /// time. Default value is 16. It is important to have this number be tuned
    /// relative to `request_workers` such that one client cannot overwhelm the
    /// entire queue.
    pub client_simultaneous_request_limit: usize,
    /// Number of simultaneous requests to be processed. Default value is 16.
    pub request_workers: usize,
    /// Configuration options for individual databases.
    pub storage: StorageConfiguration,
    /// The permissions granted to all connections to this server.
    pub default_permissions: Permissions,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            client_simultaneous_request_limit: 16,
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            request_workers: 16,
            storage: bonsaidb_local::config::Configuration::default(),
            default_permissions: Permissions::default(),
        }
    }
}
