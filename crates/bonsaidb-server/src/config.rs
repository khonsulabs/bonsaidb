use std::path::Path;

#[cfg(feature = "encryption")]
use bonsaidb_core::document::KeyId;
use bonsaidb_core::{permissions::Permissions, schema::Schema};
use bonsaidb_local::config::{Builder, KeyValuePersistence, StorageConfiguration};
#[cfg(feature = "encryption")]
use bonsaidb_local::vault::AnyVaultKeyStorage;

/// Configuration options for [`Server`](crate::Server)
#[derive(Debug)]
#[must_use]
#[non_exhaustive]
pub struct ServerConfiguration {
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
    /// The permissions granted to authenticated connections to this server.
    pub authenticated_permissions: DefaultPermissions,
    /// The ACME settings for automatic TLS certificate management.
    #[cfg(feature = "acme")]
    pub acme: AcmeConfiguration,
}

impl ServerConfiguration {
    /// Sets [`Self::server_name`](Self#structfield.server_name) to `server_name` and returns self.
    pub fn server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = server_name.into();
        self
    }

    /// Sets [`Self::client_simultaneous_request_limit`](Self#structfield.client_simultaneous_request_limit) to `request_limit` and returns self.
    pub const fn client_simultaneous_request_limit(mut self, request_limit: usize) -> Self {
        self.client_simultaneous_request_limit = request_limit;
        self
    }

    /// Sets [`Self::request_workers`](Self#structfield.request_workers) to `workers` and returns self.
    pub const fn request_workers(mut self, workers: usize) -> Self {
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

    /// Sets [`Self::authenticated_permissions`](Self#structfield.authenticated_permissions) to `authenticated_permissions` and returns self.
    pub fn authenticated_permissions<P: Into<DefaultPermissions>>(
        mut self,
        authenticated_permissions: P,
    ) -> Self {
        self.authenticated_permissions = authenticated_permissions.into();
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
}

impl Default for ServerConfiguration {
    fn default() -> Self {
        Self {
            server_name: String::from("bonsaidb"),
            client_simultaneous_request_limit: 16,
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            request_workers: 16,
            storage: bonsaidb_local::config::StorageConfiguration::default(),
            default_permissions: DefaultPermissions::Permissions(Permissions::default()),
            authenticated_permissions: DefaultPermissions::Permissions(Permissions::default()),
            #[cfg(feature = "acme")]
            acme: AcmeConfiguration::default(),
        }
    }
}

#[cfg(feature = "acme")]
mod acme {
    /// The Automated Certificate Management Environment (ACME) configuration.
    #[derive(Debug)]
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
#[derive(Debug)]
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

impl Builder for ServerConfiguration {
    fn with_schema<S: Schema>(mut self) -> Result<Self, bonsaidb_local::Error> {
        self.storage.register_schema::<S>()?;
        Ok(self)
    }

    fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.storage.path = Some(path.as_ref().to_owned());
        self
    }

    fn memory_only(mut self) -> Self {
        self.storage.memory_only = true;
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
        self.storage.vault_key_storage = Some(Box::new(key_storage));
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

    fn check_view_integrity_on_open(mut self, check: bool) -> Self {
        self.storage.views.check_integrity_on_open = check;
        self
    }

    fn key_value_persistence(mut self, persistence: KeyValuePersistence) -> Self {
        self.storage.key_value_persistence = persistence;
        self
    }
}
