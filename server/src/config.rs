use pliantdb_core::{
    backend::{Backend, CustomApi, NoDispatcher},
    permissions::Permissions,
};

/// Configuration options for [`Server`](crate::Server)
pub struct Configuration<B: Backend> {
    /// Number of simultaneous requests to be processed. Default value is 16.
    pub request_workers: usize,
    /// Configuration options for individual databases.
    pub storage: pliantdb_local::config::Configuration,
    /// The permissions granted to all connections to this server.
    pub default_permissions: Permissions,
    /// The dispatcher for a custom API
    pub custom_api_dispatcher: <B::CustomApi as CustomApi>::Dispatcher,
}

impl Default for Configuration<()> {
    fn default() -> Self {
        Self::default_with_dispatcher(NoDispatcher)
    }
}

impl<B: Backend> Configuration<B> {
    /// Returns the default configuration, with the given dispatcher.
    pub fn default_with_dispatcher(
        custom_api_dispatcher: <B::CustomApi as CustomApi>::Dispatcher,
    ) -> Self {
        Self {
            custom_api_dispatcher,
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            request_workers: 16,
            storage: pliantdb_local::config::Configuration::default(),
            default_permissions: Permissions::default(),
        }
    }
}
