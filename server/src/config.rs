use pliantdb_core::permissions::Permissions;

/// Configuration options for [`Server`](crate::Server)
pub struct Configuration {
    /// Number of simultaneous requests to be processed. Default value is 16.
    pub request_workers: usize,
    /// Configuration options for individual databases.
    pub storage: pliantdb_local::config::Configuration,
    /// The permissions granted to all connections to this server.
    pub default_permissions: Permissions,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            request_workers: 16,
            storage: pliantdb_local::config::Configuration::default(),
            default_permissions: Permissions::default(),
        }
    }
}
