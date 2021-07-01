use std::{
    convert::TryInto,
    fmt::Debug,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use chacha20poly1305::{
    aead::{generic_array::GenericArray, Aead, NewAead, Payload},
    XChaCha20Poly1305,
};
use futures::TryFutureExt;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::storage::StorageId;

#[derive(Debug)]
pub struct Vault {
    master_key: EncryptionKey,
    master_key_storage: Box<dyn AnyMasterKeyStorage>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with encryption: {0}")]
    Encryption(String),
    #[error("error from master key storage: {0}")]
    MasterKeyStorage(String),
    #[error("error occurred while initializing: {0}")]
    Initializing(String),
    #[error("master key not found")]
    MasterKeyNotFound,
}

impl From<chacha20poly1305::aead::Error> for Error {
    fn from(err: chacha20poly1305::aead::Error) -> Self {
        Self::Encryption(err.to_string())
    }
}

impl Vault {
    pub async fn initialize(
        server_id: StorageId,
        server_directory: &Path,
        master_key_storage: Box<dyn AnyMasterKeyStorage>,
    ) -> Result<Self, Error> {
        let sealing_key_path = server_directory.join("vault-key");
        if sealing_key_path.exists() {
            // The vault has been initilized previously. Do not overwrite this file voluntarily.
            let sealing_key = File::open(sealing_key_path)
                .and_then(|mut f| async move {
                    let mut bytes = Vec::new();
                    f.read_to_end(&mut bytes).await.map(|_| bytes)
                })
                .await
                .map_err(|err| {
                    Error::Initializing(format!("error reading sealing key: {:?}", err))
                })?;
            let sealing_key =
                bincode::deserialize::<EncryptionKey>(&sealing_key).map_err(|err| {
                    Error::Initializing(format!("error deserializing sealing key: {:?}", err))
                })?;
            if let Some(encrypted_key) = master_key_storage
                .master_key_for(server_id)
                .await
                .map_err(|err| Error::MasterKeyStorage(err.to_string()))?
            {
                Ok(Self {
                    master_key: encrypted_key.decrypt(&sealing_key)?,
                    master_key_storage,
                })
            } else {
                Err(Error::MasterKeyNotFound)
            }
        } else {
            let master_key = EncryptionKey::random();
            let (sealing_key, encrypted_master_key) = master_key.encrypt_key();
            master_key_storage
                .set_master_key_for(server_id, &encrypted_master_key)
                .await
                .map_err(|err| Error::MasterKeyStorage(err.to_string()))?;
            // Beacuse this is such a critical step, let's verify that we can
            // retrieve the key before we store the sealing key.
            let retrieved = master_key_storage
                .master_key_for(server_id)
                .await
                .map_err(|err| Error::MasterKeyStorage(err.to_string()))?;
            if retrieved == Some(encrypted_master_key) {
                let sealing_key_bytes =
                    bincode::serialize(&sealing_key).expect("error serializing sealing key");

                File::create(sealing_key_path)
                    .and_then(|mut file| async move { file.write_all(&sealing_key_bytes).await })
                    .await
                    .map_err(|err| {
                        Error::Initializing(format!("error saving sealing key: {:?}", err))
                    })?;

                Ok(Self {
                    master_key,
                    master_key_storage,
                })
            } else {
                Err(Error::MasterKeyStorage(String::from(
                    "master key storage failed to return the same stored key during initialization",
                )))
            }
        }
    }
}

#[async_trait]
pub trait MasterKeyStorage: Send + Sync + Debug + 'static {
    type Error: std::error::Error;
    // TODO make this support a serializable document that can contain multiple
    // keys, so that new keys can be added over time as part of a rotation
    // strategy.
    async fn master_key_for(
        &self,
        server_id: StorageId,
    ) -> Result<Option<EncryptedKey>, Self::Error>;
    async fn set_master_key_for(
        &self,
        server_id: StorageId,
        key: &EncryptedKey,
    ) -> Result<(), Self::Error>;
}

#[derive(Serialize, Deserialize)]
pub struct EncryptionKey([u8; 32]);

impl EncryptionKey {
    pub fn random() -> Self {
        Self(thread_rng().gen())
    }

    pub fn encrypt_key(&self) -> (Self, EncryptedKey) {
        let mut rng = thread_rng();
        let wrapping_key = Self::random();
        let nonce: [u8; 24] = rng.gen();
        let encrypted = XChaCha20Poly1305::new(GenericArray::from_slice(&wrapping_key.0))
            .encrypt(
                GenericArray::from_slice(&nonce),
                Payload {
                    msg: &self.0,
                    aad: b"",
                },
            )
            .unwrap();
        (
            wrapping_key,
            EncryptedKey {
                key: encrypted,
                nonce,
            },
        )
    }

    pub fn decrypt_payload(&self, payload: &[u8], nonce: &[u8; 24]) -> Result<Vec<u8>, Error> {
        Ok(
            XChaCha20Poly1305::new(GenericArray::from_slice(&self.0)).decrypt(
                GenericArray::from_slice(nonce),
                Payload {
                    msg: payload,
                    aad: b"",
                },
            )?,
        )
    }
}

impl Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateKey").finish_non_exhaustive()
    }
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct EncryptedKey {
    key: Vec<u8>,
    nonce: [u8; 24],
}

impl EncryptedKey {
    pub fn decrypt(&self, key: &EncryptionKey) -> Result<EncryptionKey, Error> {
        let decrypted = key.decrypt_payload(&self.key, &self.nonce)?;
        let decrypted = decrypted
            .try_into()
            .map_err(|err| Error::Encryption(format!("decrypted key length invalid: {:?}", err)))?;
        Ok(EncryptionKey(decrypted))
    }
}

impl Debug for EncryptedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedPrivateKey")
            .finish_non_exhaustive()
    }
}

#[async_trait]
pub trait AnyMasterKeyStorage: Send + Sync + Debug {
    async fn master_key_for(&self, server_id: StorageId) -> Result<Option<EncryptedKey>, Error>;
    async fn set_master_key_for(
        &self,
        server_id: StorageId,
        key: &EncryptedKey,
    ) -> Result<(), Error>;
}

#[async_trait]
impl<T> AnyMasterKeyStorage for T
where
    T: MasterKeyStorage,
{
    async fn master_key_for(&self, server_id: StorageId) -> Result<Option<EncryptedKey>, Error> {
        MasterKeyStorage::master_key_for(self, server_id)
            .await
            .map_err(|err| Error::MasterKeyStorage(err.to_string()))
    }

    async fn set_master_key_for(
        &self,
        server_id: StorageId,
        key: &EncryptedKey,
    ) -> Result<(), Error> {
        MasterKeyStorage::set_master_key_for(self, server_id, key)
            .await
            .map_err(|err| Error::MasterKeyStorage(err.to_string()))
    }
}

/// Stores master keys locally on disk. This is in general considered insecure,
/// and shouldn't be used without careful consideration.
///
/// The primary goal of encryption within `PliantDb` is to offer limited
/// encryption at-rest. Within these goals, the primary attack vector being
/// protected against is an attacker being able to copy the data off of the
/// disks, either by physically gaining access to the drives or having
/// filesystem access. By storing the master key on the same physical media, the
/// encryption should be considered insecure because if you can gain access to
/// the data, you have access to the keys as well.
///
/// For production environments, it is much more secure to store the master keys
/// in a separate location. We recommand any S3-compatible backend.
#[derive(Debug, Clone)]
pub struct LocalMasterKeyStorage {
    directory: PathBuf,
}

impl LocalMasterKeyStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            directory: path.as_ref().to_owned(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LocalMasterKeyStorageError {
    #[error("io error: {0}")]
    Io(#[from] tokio::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),
}

#[async_trait]
impl MasterKeyStorage for LocalMasterKeyStorage {
    type Error = LocalMasterKeyStorageError;

    async fn master_key_for(
        &self,
        server_id: StorageId,
    ) -> Result<Option<EncryptedKey>, Self::Error> {
        let server_folder = self.directory.join(server_id.to_string());
        if !server_folder.exists() {
            return Ok(None);
        }
        let key_path = server_folder.join("master_key");
        if key_path.exists() {
            let contents = File::open(key_path)
                .and_then(|mut f| async move {
                    let mut bytes = Vec::new();
                    f.read_to_end(&mut bytes).await.map(|_| bytes)
                })
                .await?;
            Ok(Some(bincode::deserialize(&contents)?))
        } else {
            Ok(None)
        }
    }

    async fn set_master_key_for(
        &self,
        server_id: StorageId,
        key: &EncryptedKey,
    ) -> Result<(), Self::Error> {
        let server_folder = self.directory.join(server_id.to_string());
        if !server_folder.exists() {
            fs::create_dir_all(&server_folder).await?;
        }
        let key_path = server_folder.join("master_key");
        let bytes = bincode::serialize(key)?;
        File::create(key_path)
            .and_then(|mut file| async move { file.write_all(&bytes).await })
            .await?;
        Ok(())
    }
}
