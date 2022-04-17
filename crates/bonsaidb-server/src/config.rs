use std::{collections::HashMap, marker::PhantomData, path::Path, sync::Arc};

#[cfg(feature = "encryption")]
use bonsaidb_core::document::KeyId;
use bonsaidb_core::{
    api,
    permissions::{Permissions, Statement},
    schema::{ApiName, Schema},
};
#[cfg(feature = "compression")]
use bonsaidb_local::config::Compression;
use bonsaidb_local::config::{Builder, KeyValuePersistence, StorageConfiguration};
#[cfg(feature = "encryption")]
use bonsaidb_local::vault::AnyVaultKeyStorage;

use crate::{
    api::{AnyHandler, AnyWrapper, Handler},
    Backend, Error, NoBackend,
};

/// Configuration options for [`Server`](crate::Server)
#[derive(Debug, Clone)]
#[must_use]
#[non_exhaustive]
pub struct ServerConfiguration<B: Backend = NoBackend> {
    /// The DNS name of the server.
    pub server_name: String,
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
    pub default_permissions: DefaultPermissions,
    /// The ACME settings for automatic TLS certificate management.
    #[cfg(feature = "acme")]
    pub acme: AcmeConfiguration,

    pub(crate) custom_apis: HashMap<ApiName, Arc<dyn AnyHandler<B>>>,
}

impl<B: Backend> ServerConfiguration<B> {
    /// Sets [`Self::server_name`](Self#structfield.server_name) to `server_name` and returns self.
    pub fn server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = server_name.into();
        self
    }

    /// Sets [`Self::client_simultaneous_request_limit`](Self#structfield.client_simultaneous_request_limit) to `request_limit` and returns self.
    pub fn client_simultaneous_request_limit(mut self, request_limit: usize) -> Self {
        self.client_simultaneous_request_limit = request_limit;
        self
    }

    /// Sets [`Self::request_workers`](Self#structfield.request_workers) to `workers` and returns self.
    pub fn request_workers(mut self, workers: usize) -> Self {
        self.request_workers = workers;
        self
    }

    /// Sets [`Self::default_permissions`](Self#structfield.default_permissions) to `default_permissions` and returns self.
    pub fn default_permissions<P: Into<DefaultPermissions>>(
        mut self,
        default_permissions: P,
    ) -> Self {
        self.default_permissions = default_permissions.into();
        self
    }

    /// Sets [`AcmeConfiguration::contact_email`] to `contact_email` and returns self.
    #[cfg(feature = "acme")]
    pub fn acme_contact_email(mut self, contact_email: impl Into<String>) -> Self {
        self.acme.contact_email = Some(contact_email.into());
        self
    }

    /// Sets [`AcmeConfiguration::directory`] to `directory` and returns self.
    #[cfg(feature = "acme")]
    pub fn acme_directory(mut self, directory: impl Into<String>) -> Self {
        self.acme.directory = directory.into();
        self
    }

    /// Registers a `handler` for a [`Api`][api::Api]. When an [`Api`][api::Api] is
    /// received by the server, the handler will be invoked
    pub fn register_custom_api<Dispatcher: Handler<B, Api> + 'static, Api: api::Api>(
        &mut self,
    ) -> Result<(), Error> {
        // TODO this should error on duplicate registration.
        self.custom_apis.insert(
            Api::name(),
            Arc::new(AnyWrapper::<Dispatcher, B, Api>(PhantomData)),
        );
        Ok(())
    }

    /// Registers the custom api dispatcher and returns self.
    pub fn with_api<Dispatcher: Handler<B, Api> + 'static, Api: api::Api>(
        mut self,
    ) -> Result<Self, Error> {
        self.register_custom_api::<Dispatcher, Api>()?;
        Ok(self)
    }
}

impl<B: Backend> Default for ServerConfiguration<B> {
    fn default() -> Self {
        Self {
            server_name: String::from("bonsaidb"),
            client_simultaneous_request_limit: 16,
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            request_workers: 16,
            storage: bonsaidb_local::config::StorageConfiguration::default(),
            default_permissions: DefaultPermissions::Permissions(Permissions::default()),
            custom_apis: HashMap::default(),
            #[cfg(feature = "acme")]
            acme: AcmeConfiguration::default(),
        }
    }
}

#[cfg(feature = "acme")]
mod acme {
    /// The Automated Certificate Management Environment (ACME) configuration.
    #[derive(Debug, Clone)]
    pub struct AcmeConfiguration {
        /// The contact email to register with the ACME directory for the account.
        pub contact_email: Option<String>,
        /// The ACME directory to use for registration. The default is
        /// [`LETS_ENCRYPT_PRODUCTION_DIRECTORY`].
        pub directory: String,
    }

    impl Default for AcmeConfiguration {
        fn default() -> Self {
            Self {
                contact_email: None,
                directory: LETS_ENCRYPT_PRODUCTION_DIRECTORY.to_string(),
            }
        }
    }

    pub use async_acme::acme::{LETS_ENCRYPT_PRODUCTION_DIRECTORY, LETS_ENCRYPT_STAGING_DIRECTORY};
}

#[cfg(feature = "acme")]
pub use acme::*;

/// The default permissions to use for all connections to the server.
#[derive(Debug, Clone)]
pub enum DefaultPermissions {
    /// Allow all permissions. Do not use outside of completely trusted environments.
    AllowAll,
    /// A defined set of permissions.
    Permissions(Permissions),
}

impl From<DefaultPermissions> for Permissions {
    fn from(permissions: DefaultPermissions) -> Self {
        match permissions {
            DefaultPermissions::Permissions(permissions) => permissions,
            DefaultPermissions::AllowAll => Self::allow_all(),
        }
    }
}

impl From<Permissions> for DefaultPermissions {
    fn from(permissions: Permissions) -> Self {
        Self::Permissions(permissions)
    }
}

impl From<Vec<Statement>> for DefaultPermissions {
    fn from(permissions: Vec<Statement>) -> Self {
        Self::from(Permissions::from(permissions))
    }
}

impl From<Statement> for DefaultPermissions {
    fn from(permissions: Statement) -> Self {
        Self::from(Permissions::from(permissions))
    }
}

impl<B: Backend> Builder for ServerConfiguration<B> {
    fn with_schema<S: Schema>(mut self) -> Result<Self, bonsaidb_local::Error> {
        self.storage.register_schema::<S>()?;
        Ok(self)
    }

    fn memory_only(mut self) -> Self {
        self.storage.memory_only = true;
        self
    }

    fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.storage.path = Some(path.as_ref().to_owned());
        self
    }

    fn unique_id(mut self, unique_id: u64) -> Self {
        self.storage.unique_id = Some(unique_id);
        self
    }

    #[cfg(feature = "encryption")]
    fn vault_key_storage<VaultKeyStorage: AnyVaultKeyStorage>(
        mut self,
        key_storage: VaultKeyStorage,
    ) -> Self {
        self.storage.vault_key_storage = Some(std::sync::Arc::new(key_storage));
        self
    }

    #[cfg(feature = "encryption")]
    fn default_encryption_key(mut self, key: KeyId) -> Self {
        self.storage.default_encryption_key = Some(key);
        self
    }

    fn tasks_worker_count(mut self, worker_count: usize) -> Self {
        self.storage.workers.worker_count = worker_count;
        self
    }

    fn tasks_parallelization(mut self, parallelization: usize) -> Self {
        self.storage.workers.parallelization = parallelization;
        self
    }

    fn check_view_integrity_on_open(mut self, check: bool) -> Self {
        self.storage.views.check_integrity_on_open = check;
        self
    }

    #[cfg(feature = "compression")]
    fn default_compression(mut self, compression: Compression) -> Self {
        self.storage.default_compression = Some(compression);
        self
    }

    fn key_value_persistence(mut self, persistence: KeyValuePersistence) -> Self {
        self.storage.key_value_persistence = persistence;
        self
    }

    fn authenticated_permissions<P: Into<Permissions>>(
        mut self,
        authenticated_permissions: P,
    ) -> Self {
        self.storage.authenticated_permissions = authenticated_permissions.into();
        self
    }

    #[cfg(feature = "password-hashing")]
    fn argon(mut self, argon: bonsaidb_local::config::ArgonConfiguration) -> Self {
        self.storage.argon = argon;
        self
    }
}
