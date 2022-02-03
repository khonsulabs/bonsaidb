//! [`VaultKeyStorage`] using S3-compatible storage.
//!
//! This is the recommended method for securing your `BonsaiDb` database. There
//! are many ways to acquire secure, inexpensive S3-compatible storage, such as
//! Backblaze B2.
//!
//! Do not configure your bucket with public access. You should only allow
//! access from the IP addresses that your `BonsaiDb` server(s) are hosted on,
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
//! let directory = TestDirectory::new("bonsaidb-keystorage-s3-basic");
//! let configuration = StorageConfiguration::new(&directory)
//!     .vault_key_storage(
//!         S3VaultKeyStorage::new("bucket_name").endpoint(Endpoint::immutable(
//!             Uri::try_from("https://s3.us-west-001.backblazeb2.com").unwrap(),
//!         )),
//!     )
//!     .default_encryption_key(KeyId::Master);
//! ```
//!
//! The API calls are performed by the [`aws-sdk-s3`](aws_sdk_s3) crate.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
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

use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
pub use aws_sdk_s3;
use aws_sdk_s3::{error::GetObjectErrorKind, Client, Endpoint, Region};
use bonsaidb_local::{
    vault::{KeyPair, VaultKeyStorage},
    StorageId,
};
pub use http;

/// S3-compatible [`VaultKeyStorage`] implementor.
#[derive(Debug, Default)]
pub struct S3VaultKeyStorage {
    bucket: String,
    /// The S3 endpoint to use. If not specified, the endpoint will be
    /// determined automatically. This field can be used to support non-AWS S3
    /// providers.
    pub endpoint: Option<Endpoint>,
    /// The AWS region to use. If not specified, the region will be determined
    /// by the aws sdk.
    pub region: Option<Region>,
    /// The path prefix for keys to be stored within.
    pub path: String,
}

impl S3VaultKeyStorage {
    /// Creates a new key storage instance for `bucket`.
    pub fn new(bucket: impl Display) -> Self {
        Self {
            bucket: bucket.to_string(),
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
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);
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
            Client::with_config(
                aws_smithy_client::Client::dyn_https(),
                aws_sdk_s3::Config::builder()
                    .endpoint_resolver(endpoint)
                    .region(region_provider.region().await)
                    .credentials_provider(config.credentials_provider().unwrap().clone())
                    .build(),
            )
        } else {
            aws_sdk_s3::Client::new(&config)
        }
    }
}

#[async_trait]
impl VaultKeyStorage for S3VaultKeyStorage {
    type Error = anyhow::Error;

    async fn set_vault_key_for(
        &self,
        storage_id: StorageId,
        key: KeyPair,
    ) -> Result<(), Self::Error> {
        let client = self.client().await;
        let key = key.to_bytes()?;
        client
            .put_object()
            .bucket(&self.bucket)
            .key(self.path_for_id(storage_id))
            .body(aws_sdk_s3::ByteStream::from(key.to_vec()))
            .send()
            .await?;
        Ok(())
    }

    async fn vault_key_for(&self, storage_id: StorageId) -> Result<Option<KeyPair>, Self::Error> {
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
                let key =
                    KeyPair::from_bytes(&bytes).map_err(|err| anyhow::anyhow!(err.to_string()))?;
                Ok(Some(key))
            }
            Err(aws_sdk_s3::SdkError::ServiceError {
                err:
                    aws_sdk_s3::error::GetObjectError {
                        kind: GetObjectErrorKind::NoSuchKey(_),
                        ..
                    },
                ..
            }) => Ok(None),
            Err(err) => Err(anyhow::anyhow!(err)),
        }
    }
}

#[cfg(test)]
#[tokio::test]
async fn basic_test() {
    use bonsaidb_core::{
        connection::StorageConnection,
        document::KeyId,
        schema::SerializedCollection,
        test_util::{Basic, BasicSchema, TestDirectory},
    };
    use bonsaidb_local::{
        config::{Builder, StorageConfiguration},
        Storage,
    };
    use http::Uri;
    drop(dotenv::dotenv());

    macro_rules! env_var {
        ($name:expr) => {{
            match std::env::var($name) {
                Ok(value) => value,
                Err(_) => {
                    log::error!(
                        "Ignoring basic_test because of missing environment variable: {}",
                        $name
                    );
                    return;
                }
            }
        }};
    }

    let bucket = env_var!("S3_BUCKET");
    let endpoint = env_var!("S3_ENDPOINT");

    let directory = TestDirectory::new("bonsaidb-keystorage-s3-basic");

    let configuration = |prefix| {
        let mut vault_key_storage = S3VaultKeyStorage {
            bucket: bucket.clone(),
            endpoint: Some(Endpoint::immutable(Uri::try_from(&endpoint).unwrap())),
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
        let bonsai = Storage::open(configuration(None)).await.unwrap();
        bonsai
            .create_database::<BasicSchema>("test", false)
            .await
            .unwrap();
        let db = bonsai.database::<BasicSchema>("test").await.unwrap();
        Basic::new("test").push_into(&db).await.unwrap()
    };

    {
        // Should be able to access the storage again
        let bonsai = Storage::open(configuration(None)).await.unwrap();

        let db = bonsai.database::<BasicSchema>("test").await.unwrap();
        let retrieved = Basic::get(document.header.id, &db)
            .await
            .unwrap()
            .expect("document not found");
        assert_eq!(document, retrieved);
    }

    // Verify that we can't access the storage again without the vault
    assert!(
        Storage::open(configuration(Some(String::from("path-prefix"))))
            .await
            .is_err()
    );
}
