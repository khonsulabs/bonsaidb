//! [`VaultKeyStorage`] using S3-compatible storage.
//!
//! This is the recommended method for securing your BonsaiDb database. There
//! are many ways to acquire secure, inexpensive S3-compatible storage, such as
//! Backblaze B2.
//!
//! Do not configure your bucket with public access. You should only allow
//! access from the IP addresses that your BonsaiDb server(s) are hosted on,
//! or only allow authenticated access.
//!
//! To use this, specify the `vault_key_storage` configuration parameter:
//!
//! ```rust
//! # use bonsaidb_keystorage_s3::S3VaultKeyStorage;
//! # use aws_sdk_s3::Endpoint;
//! # use bonsaidb_core::{document::KeyId, test_util::TestDirectory};
//! # use bonsaidb_local::config::{StorageConfiguration, Builder};
//! # use http::Uri;
//! #
//! # async fn test() {
//! let directory = TestDirectory::new("bonsaidb-keystorage-s3-basic");
//! let configuration = StorageConfiguration::new(&directory)
//!     .vault_key_storage(
//!         S3VaultKeyStorage::new("bucket_name")
//!             .endpoint("https://s3.us-west-001.backblazeb2.com"),
//!     )
//!     .default_encryption_key(KeyId::Master);
//! # }
//! ```
//!
//! The API calls are performed by the [`aws-sdk-s3`](aws_sdk_s3) crate.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::missing_panics_doc, // TODO clippy::missing_panics_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

use std::fmt::Display;
use std::future::Future;

use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bonsaidb_local::vault::{KeyPair, VaultKeyStorage};
use bonsaidb_local::StorageId;
use tokio::runtime::{self, Handle, Runtime};
pub use {aws_sdk_s3, http};

/// S3-compatible [`VaultKeyStorage`] implementor.
#[derive(Default, Debug)]
#[must_use]
pub struct S3VaultKeyStorage {
    runtime: Tokio,
    bucket: String,
    /// The S3 endpoint to use. If not specified, the endpoint will be
    /// determined automatically. This field can be used to support non-AWS S3
    /// providers.
    pub endpoint: Option<String>,
    /// The AWS region to use. If not specified, the region will be determined
    /// by the aws sdk.
    pub region: Option<Region>,
    /// The path prefix for keys to be stored within.
    pub path: String,
}

#[derive(Debug)]
enum Tokio {
    Runtime(Runtime),
    Handle(Handle),
}

impl Default for Tokio {
    fn default() -> Self {
        Handle::try_current().map_or_else(
            |_| {
                Self::Runtime(
                    runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap(),
                )
            },
            Self::Handle,
        )
    }
}

impl Tokio {
    pub fn block_on<F: Future<Output = R>, R>(&self, future: F) -> R {
        match self {
            Tokio::Runtime(rt) => rt.block_on(future),
            Tokio::Handle(rt) => rt.block_on(future),
        }
    }
}

impl S3VaultKeyStorage {
    /// Creates a new key storage instance for `bucket`. This instance will use
    /// the currently available Tokio runtime or create one if none is
    /// available.
    pub fn new(bucket: impl Display) -> Self {
        Self::new_with_runtime(bucket, tokio::runtime::Handle::current())
    }

    /// Creates a new key storage instance for `bucket`, which performs its
    /// networking operations on `runtime`.
    pub fn new_with_runtime(bucket: impl Display, runtime: tokio::runtime::Handle) -> Self {
        Self {
            bucket: bucket.to_string(),
            runtime: Tokio::Handle(runtime),
            ..Self::default()
        }
    }

    /// Sets the path prefix for vault keys to be stored within.
    pub fn path(mut self, prefix: impl Display) -> Self {
        self.path = prefix.to_string();
        self
    }

    /// Sets the endpoint to use. See [`Self::endpoint`] for more information.
    #[allow(clippy::missing_const_for_fn)] // destructors
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    fn path_for_id(&self, storage_id: StorageId) -> String {
        let mut path = self.path.clone();
        if !path.is_empty() && !path.ends_with('/') {
            path.push('/');
        }
        path.push_str(&storage_id.to_string());
        path
    }

    async fn client(&self) -> aws_sdk_s3::Client {
        let region_provider = RegionProviderChain::first_try(self.region.clone())
            .or_default_provider()
            .or_else(Region::new("us-east-1"));
        let config = aws_config::from_env().load().await;
        if let Some(endpoint) = self.endpoint.clone() {
            Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .endpoint_url(endpoint)
                    .region(region_provider.region().await)
                    .credentials_provider(config.credentials_provider().unwrap().clone())
                    .build(),
            )
        } else {
            Client::new(&config)
        }
    }
}

#[async_trait]
impl VaultKeyStorage for S3VaultKeyStorage {
    type Error = anyhow::Error;

    fn set_vault_key_for(&self, storage_id: StorageId, key: KeyPair) -> Result<(), Self::Error> {
        self.runtime.block_on(async {
            let client = self.client().await;
            let key = key.to_bytes()?;
            client
                .put_object()
                .bucket(&self.bucket)
                .key(self.path_for_id(storage_id))
                .body(ByteStream::from(key.to_vec()))
                .send()
                .await?;
            Ok(())
        })
    }

    fn vault_key_for(&self, storage_id: StorageId) -> Result<Option<KeyPair>, Self::Error> {
        self.runtime.block_on(async {
            let client = self.client().await;
            match client
                .get_object()
                .bucket(&self.bucket)
                .key(self.path_for_id(storage_id))
                .send()
                .await
            {
                Ok(response) => {
                    let bytes = response.body.collect().await?.into_bytes();
                    let key = KeyPair::from_bytes(&bytes)
                        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
                    Ok(Some(key))
                }
                Err(aws_smithy_client::SdkError::ServiceError(err))
                    if matches!(
                        err.err(),
                        aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_)
                    ) =>
                {
                    Ok(None)
                }
                Err(err) => Err(anyhow::anyhow!(err)),
            }
        })
    }
}

#[cfg(test)]
macro_rules! env_var {
    ($name:expr) => {{
        match std::env::var($name) {
            Ok(value) if !value.is_empty() => value,
            _ => {
                log::error!(
                    "Ignoring basic_test because of missing environment variable: {}",
                    $name
                );
                return;
            }
        }
    }};
}

#[cfg(test)]
#[tokio::test]
async fn basic_test() {
    use bonsaidb_core::connection::AsyncStorageConnection;
    use bonsaidb_core::document::KeyId;
    use bonsaidb_core::schema::SerializedCollection;
    use bonsaidb_core::test_util::{Basic, BasicSchema, TestDirectory};
    use bonsaidb_local::config::{Builder, StorageConfiguration};
    use bonsaidb_local::AsyncStorage;
    drop(dotenv::dotenv());

    let bucket = env_var!("S3_BUCKET");
    let endpoint = env_var!("S3_ENDPOINT");

    let directory = TestDirectory::new("bonsaidb-keystorage-s3-basic");

    let configuration = |prefix| {
        let mut vault_key_storage = S3VaultKeyStorage {
            bucket: bucket.clone(),
            endpoint: Some(endpoint.clone()),
            ..S3VaultKeyStorage::default()
        };
        if let Some(prefix) = prefix {
            vault_key_storage = vault_key_storage.path(prefix);
        }

        StorageConfiguration::new(&directory)
            .vault_key_storage(vault_key_storage)
            .default_encryption_key(KeyId::Master)
            .with_schema::<BasicSchema>()
            .unwrap()
    };
    let document = {
        let bonsai = AsyncStorage::open(configuration(None)).await.unwrap();
        let db = bonsai
            .create_database::<BasicSchema>("test", false)
            .await
            .unwrap();
        Basic::new("test").push_into_async(&db).await.unwrap()
    };

    {
        // Should be able to access the storage again
        let bonsai = AsyncStorage::open(configuration(None)).await.unwrap();

        let db = bonsai.database::<BasicSchema>("test").await.unwrap();
        let retrieved = Basic::get_async(&document.header.id, &db)
            .await
            .unwrap()
            .expect("document not found");
        assert_eq!(document, retrieved);
    }

    // Verify that we can't access the storage again without the vault
    assert!(
        AsyncStorage::open(configuration(Some(String::from("path-prefix"))))
            .await
            .is_err()
    );
}

#[test]
fn blocking_test() {
    use bonsaidb_core::connection::StorageConnection;
    use bonsaidb_core::document::KeyId;
    use bonsaidb_core::schema::SerializedCollection;
    use bonsaidb_core::test_util::{Basic, BasicSchema, TestDirectory};
    use bonsaidb_local::config::{Builder, StorageConfiguration};
    use bonsaidb_local::Storage;
    drop(dotenv::dotenv());

    let bucket = env_var!("S3_BUCKET");
    let endpoint = env_var!("S3_ENDPOINT");

    let directory = TestDirectory::new("bonsaidb-keystorage-s3-blocking");

    let configuration = |prefix| {
        let mut vault_key_storage = S3VaultKeyStorage {
            bucket: bucket.clone(),
            endpoint: Some(endpoint.clone()),
            ..S3VaultKeyStorage::default()
        };
        if let Some(prefix) = prefix {
            vault_key_storage = vault_key_storage.path(prefix);
        }

        StorageConfiguration::new(&directory)
            .vault_key_storage(vault_key_storage)
            .default_encryption_key(KeyId::Master)
            .with_schema::<BasicSchema>()
            .unwrap()
    };
    let document = {
        let bonsai = Storage::open(configuration(None)).unwrap();
        let db = bonsai
            .create_database::<BasicSchema>("test", false)
            .unwrap();
        Basic::new("test").push_into(&db).unwrap()
    };

    {
        // Should be able to access the storage again
        let bonsai = Storage::open(configuration(None)).unwrap();

        let db = bonsai.database::<BasicSchema>("test").unwrap();
        let retrieved = Basic::get(&document.header.id, &db)
            .unwrap()
            .expect("document not found");
        assert_eq!(document, retrieved);
    }

    // Verify that we can't access the storage again without the vault
    assert!(Storage::open(configuration(Some(String::from("path-prefix")))).is_err());
}
