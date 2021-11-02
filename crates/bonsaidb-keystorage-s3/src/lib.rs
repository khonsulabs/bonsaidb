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
//! # use bonsaidb_core::{
//! # connection::ServerConnection,
//! # document::KeyId,
//! # schema::Collection,
//! # test_util::{Basic, BasicSchema, TestDirectory},
//! # };
//! # use bonsaidb_local::{config::Configuration, Storage};
//! # use bonsaidb_keystorage_s3::S3VaultKeyStorage;
//! #
//! # let directory = TestDirectory::new("bonsaidb-keystorage-s3-basic");
//! # let configuration = Configuration {
//! vault_key_storage: Some(Box::new(S3VaultKeyStorage::from(
//!         s3::Bucket::new(
//!             "bucket-name",
//!             s3::Region::Custom {
//!                 endpoint: String::from("s3.us-west-001.backblazeb2.com"),
//!                 region: String::default(),
//!             },
//!             s3::creds::Credentials::new(Some(&"access key id"), Some(&"secret access key"), None, None, None).unwrap(),
//!         )
//!         .unwrap(),
//!     ))),
//! # default_encryption_key: Some(KeyId::Master),
//! # ..Configuration::default()
//! # };
//! ```
//!
//! The API calls are performed by the [`s3`] crate.

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
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::missing_panics_doc, // TODO clippy::missing_panics_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

use std::fmt::Display;

use async_trait::async_trait;
use bonsaidb_local::{
    vault::{PrivateKey, VaultKeyStorage},
    StorageId,
};
pub use s3;
use s3::Bucket;

/// S3-compatible [`VaultKeyStorage`] implementor.
#[derive(Debug)]
pub struct S3VaultKeyStorage {
    bucket: Bucket,
    /// The path prefix for keys to be stored within.
    pub path: String,
}

impl From<Bucket> for S3VaultKeyStorage {
    fn from(bucket: Bucket) -> Self {
        Self {
            bucket,
            path: String::from(""),
        }
    }
}

impl S3VaultKeyStorage {
    /// Sets the path prefix for vault keys to be stored within.
    pub fn path(mut self, prefix: impl Display) -> Self {
        self.path = prefix.to_string();
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
}

#[async_trait]
impl VaultKeyStorage for S3VaultKeyStorage {
    type Error = anyhow::Error;

    async fn set_vault_key_for(
        &self,
        storage_id: StorageId,
        key: PrivateKey,
    ) -> Result<(), Self::Error> {
        let (_bytes, code) = self
            .bucket
            .put_object(&self.path_for_id(storage_id), &key.to_bytes()?)
            .await?;
        if (200..300).contains(&code) {
            Ok(())
        } else {
            anyhow::bail!("error uploading to s3 bucket: {}", code)
        }
    }

    async fn vault_key_for(
        &self,
        storage_id: StorageId,
    ) -> Result<Option<PrivateKey>, Self::Error> {
        let (bytes, code) = self.bucket.get_object(self.path_for_id(storage_id)).await?;
        if (200..300).contains(&code) {
            let key =
                PrivateKey::from_bytes(&bytes).map_err(|err| anyhow::anyhow!(err.to_string()))?;
            Ok(Some(key))
        } else if code == 404 {
            Ok(None)
        } else {
            anyhow::bail!("unexpected status code from s3 bucket: {}", code)
        }
    }
}

#[cfg(test)]
#[tokio::test]
async fn basic_test() {
    use bonsaidb_core::{
        connection::ServerConnection,
        document::KeyId,
        schema::Collection,
        test_util::{Basic, BasicSchema, TestDirectory},
    };
    use bonsaidb_local::{config::Configuration, Storage};
    drop(dotenv::dotenv());

    macro_rules! env_var {
        ($name:expr) => {{
            match std::env::var($name) {
                Ok(value) => value,
                Err(_) => {
                    eprintln!(
                        "Ignoring basic_test because of missing environment variable: {}",
                        $name
                    );
                    return;
                }
            }
        }};
    }

    let key_id = env_var!("S3_ACCESS_KEY_ID");
    let key_secret = env_var!("S3_ACCESS_KEY_SECRET");
    let bucket_name = env_var!("S3_BUCKET");
    let endpoint = env_var!("S3_ENDPOINT");

    let directory = TestDirectory::new("bonsaidb-keystorage-s3-basic");

    let configuration = |prefix| {
        let mut vault_key_storage = S3VaultKeyStorage::from(
            Bucket::new(
                &bucket_name,
                s3::Region::Custom {
                    endpoint: endpoint.clone(),
                    region: String::default(),
                },
                s3::creds::Credentials::new(Some(&key_id), Some(&key_secret), None, None, None)
                    .unwrap(),
            )
            .unwrap(),
        );
        if let Some(prefix) = prefix {
            vault_key_storage = vault_key_storage.path(prefix);
        }

        Configuration {
            vault_key_storage: Some(Box::new(vault_key_storage)),
            default_encryption_key: Some(KeyId::Master),
            ..Configuration::default()
        }
    };
    let document = {
        let bonsai = Storage::open_local(&directory, configuration(None))
            .await
            .unwrap();
        bonsai.register_schema::<BasicSchema>().await.unwrap();
        bonsai
            .create_database::<BasicSchema>("test", false)
            .await
            .unwrap();
        let db = bonsai.database::<BasicSchema>("test").await.unwrap();
        Basic::new("test").insert_into(&db).await.unwrap()
    };

    {
        // Should be able to access the storage again
        let bonsai = Storage::open_local(&directory, configuration(None))
            .await
            .unwrap();

        let db = bonsai.database::<BasicSchema>("test").await.unwrap();
        let retrieved = Basic::get(document.header.id, &db)
            .await
            .unwrap()
            .expect("document not found");
        assert_eq!(document, retrieved);
    }

    // Verify that we can't access the storage again without the vault
    assert!(
        Storage::open_local(&directory, configuration(Some(String::from("path-prefix"))))
            .await
            .is_err()
    );
}
