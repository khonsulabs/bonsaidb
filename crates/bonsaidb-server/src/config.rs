use bonsaidb_core::permissions::Permissions;
pub use bonsaidb_local::config::Configuration as StorageConfiguration;

/// Configuration options for [`Server`](crate::Server)
pub struct Configuration {
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

impl Default for Configuration {
    fn default() -> Self {
        Self {
            server_name: String::from("bonsaidb"),
            client_simultaneous_request_limit: 16,
            // TODO this was arbitrarily picked, it probably should be higher,
            // but it also should probably be based on the cpu's capabilities
            request_workers: 16,
            storage: bonsaidb_local::config::Configuration::default(),
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
