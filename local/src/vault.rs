//! Encryption and secret management.

use std::{
    borrow::Cow,
    collections::HashMap,
    convert::TryInto,
    fmt::{Debug, Display},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use chacha20poly1305::{
    aead::{generic_array::GenericArray, Aead, NewAead, Payload},
    XChaCha20Poly1305,
};
use futures::TryFutureExt;
use pliantdb_core::{
    document::KeyId,
    permissions::{
        pliant::{vault_key_resource_name, EncryptionKeyAction},
        Action, PermissionDenied, Permissions,
    },
};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
};
use x25519_xchacha20poly1305::{
    ephemeral::{PublicKeyExt, StaticSecretExt},
    x25519::{PublicKey, StaticSecret},
};
use zeroize::Zeroize;

use crate::storage::StorageId;

#[derive(Debug)]
pub(crate) struct Vault {
    vault_public_key: PublicKey,
    master_keys: HashMap<u32, EncryptionKey>,
    current_master_key_id: u32,
    master_key_storage: Box<dyn AnyVaultKeyStorage>,
}

/// Errors relating to encryption and/or secret storage.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred during encryption or decryption.
    #[error("error with encryption: {0}")]
    Encryption(String),
    /// An error occurred within the vault key storage.
    #[error("error from vault key storage: {0}")]
    VaultKeyStorage(String),
    /// An error occurred initializing the vault.
    #[error("error occurred while initializing: {0}")]
    Initializing(String),
    /// A previously initialized vault was found, but the vault key storage
    /// doesn't contain the key.
    #[error("vault key not found")]
    VaultKeyNotFound,
}

impl From<chacha20poly1305::aead::Error> for Error {
    fn from(err: chacha20poly1305::aead::Error) -> Self {
        Self::Encryption(err.to_string())
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::Initializing(err.to_string())
    }
}

impl Vault {
    pub async fn initialize(
        server_id: StorageId,
        server_directory: &Path,
        master_key_storage: Box<dyn AnyVaultKeyStorage>,
    ) -> Result<Self, Error> {
        let master_keys_path = server_directory.join("master-keys");
        if master_keys_path.exists() {
            // The vault has been initilized previously. Do not overwrite this file voluntarily.
            let encrypted_master_keys = File::open(master_keys_path)
                .and_then(|mut f| async move {
                    let mut bytes = Vec::new();
                    f.read_to_end(&mut bytes).await.map(|_| bytes)
                })
                .await
                .map_err(|err| {
                    Error::Initializing(format!("error reading master keys: {:?}", err))
                })?;
            let encrypted_master_keys = VaultPayload::from_slice(&encrypted_master_keys)?;
            if let Some(vault_key) = master_key_storage
                .vault_key_for(server_id)
                .await
                .map_err(|err| Error::VaultKeyStorage(err.to_string()))?
            {
                let master_keys = vault_key.decrypt(
                    encrypted_master_keys.payload.as_ref(),
                    b"",
                    encrypted_master_keys.nonce.as_ref(),
                )?;
                let master_keys =
                    bincode::deserialize::<HashMap<u32, EncryptionKey>>(&master_keys)?;
                let current_master_key_id = *master_keys.keys().max().unwrap();
                Ok(Self {
                    vault_public_key: PublicKey::from(&vault_key),
                    master_keys,
                    current_master_key_id,
                    master_key_storage,
                })
            } else {
                Err(Error::VaultKeyNotFound)
            }
        } else {
            let master_key = EncryptionKey::random();
            let vault_key = EncryptionKey::random();

            master_key_storage
                .set_vault_key_for(server_id, vault_key.as_static_secret())
                .await
                .map_err(|err| Error::VaultKeyStorage(err.to_string()))?;
            let mut master_keys = HashMap::new();
            master_keys.insert(0_u32, master_key);
            // Beacuse this is such a critical step, let's verify that we can
            // retrieve the key before we store the sealing key.
            let retrieved = master_key_storage
                .vault_key_for(server_id)
                .await
                .map_err(|err| Error::VaultKeyStorage(err.to_string()))?;
            let vault_public_key = vault_key.public_key();
            if retrieved
                .map(|r| PublicKey::from(&r) == vault_public_key)
                .unwrap_or_default()
            {
                drop(vault_key);

                let serialized_master_keys = bincode::serialize(&master_keys)?;
                let nonce = thread_rng().gen::<[u8; 24]>();
                let encrypted_master_keys =
                    vault_public_key.encrypt(&serialized_master_keys, b"", &nonce)?;
                let encrypted_master_keys_payload = bincode::serialize(&VaultPayload {
                    key_id: KeyId::None,
                    key_version: 0,
                    payload: Cow::Owned(encrypted_master_keys),
                    encryption: Encryption::X25519XChaCha20Poly1305,
                    nonce: Cow::Owned(nonce.to_vec()),
                })?;

                File::create(master_keys_path)
                    .and_then(|mut file| async move {
                        file.write_all(&encrypted_master_keys_payload).await?;
                        file.shutdown().await
                    })
                    .await
                    .map_err(|err| {
                        Error::Initializing(format!("error saving vault key: {:?}", err))
                    })?;

                Ok(Self {
                    vault_public_key,
                    master_keys,
                    current_master_key_id: 0,
                    master_key_storage,
                })
            } else {
                Err(Error::VaultKeyStorage(String::from(
                    "vault key storage failed to return the same stored key during initialization",
                )))
            }
        }
    }

    fn current_master_key(&self) -> &EncryptionKey {
        self.master_keys.get(&self.current_master_key_id).unwrap()
    }

    pub fn encrypt_payload(
        &self,
        key_id: &KeyId,
        payload: &[u8],
        permissions: Option<&Permissions>,
    ) -> Result<Vec<u8>, crate::Error> {
        if let Some(permissions) = permissions {
            if !permissions.allowed_to(
                vault_key_resource_name(key_id),
                &EncryptionKeyAction::Encrypt,
            ) {
                return Err(crate::Error::Core(pliantdb_core::Error::from(
                    PermissionDenied {
                        resource: vault_key_resource_name(key_id).to_owned(),
                        action: EncryptionKeyAction::Encrypt.name(),
                    },
                )));
            }
        }

        let (key, version) = match key_id {
            KeyId::Master => (self.current_master_key(), self.current_master_key_id),
            KeyId::Id(_) => todo!(),
            KeyId::None => unreachable!(),
        };
        let payload = key.encrypt_payload(key_id.clone(), version, payload);
        Ok(payload.to_vec())
    }

    pub fn decrypt_payload(
        &self,
        payload: &[u8],
        permissions: Option<&Permissions>,
    ) -> Result<Vec<u8>, crate::Error> {
        let payload = bincode::deserialize::<VaultPayload<'_>>(payload).map_err(|err| {
            Error::Encryption(format!("error deserializing encrypted payload: {:?}", err))
        })?;
        self.decrypt(&payload, permissions)
    }

    fn decrypt(
        &self,
        payload: &VaultPayload<'_>,
        permissions: Option<&Permissions>,
    ) -> Result<Vec<u8>, crate::Error> {
        if let Some(permissions) = permissions {
            if !permissions.allowed_to(
                vault_key_resource_name(&payload.key_id),
                &EncryptionKeyAction::Decrypt,
            ) {
                return Err(crate::Error::Core(pliantdb_core::Error::from(
                    PermissionDenied {
                        resource: vault_key_resource_name(&payload.key_id).to_owned(),
                        action: EncryptionKeyAction::Decrypt.name(),
                    },
                )));
            }
        }

        // TODO handle key version
        let key = match &payload.key_id {
            KeyId::Master => self.current_master_key(),
            KeyId::Id(_) => todo!(),
            KeyId::None => unreachable!(),
        };
        Ok(key.decrypt_payload(payload)?)
    }

    /// Deserializes `bytes` using bincode. First, it attempts to deserialize it
    /// as a `VaultPayload` for if it's encrypted. If it is, it will attempt to
    /// deserialize it after decrypting. If it is not a `VaultPayload`, it will be
    /// attempted to be deserialized as `D` directly.
    pub fn decrypt_serialized<D: for<'de> Deserialize<'de>>(
        &self,
        permissions: Option<&Permissions>,
        bytes: &[u8],
    ) -> Result<D, crate::Error> {
        match bincode::deserialize::<VaultPayload<'_>>(bytes) {
            Ok(encrypted) => {
                let decrypted = self.decrypt(&encrypted, permissions)?;
                Ok(bincode::deserialize(&decrypted)?)
            }
            Err(_) => Ok(bincode::deserialize(bytes)?),
        }
    }
}

/// Stores encrypted keys for a vault.
#[async_trait]
pub trait VaultKeyStorage: Send + Sync + Debug + 'static {
    /// The error type that the functions return.
    type Error: Display;
    /// Store a key. Each server id should have unique storage.
    async fn set_vault_key_for(
        &self,
        storage_id: StorageId,
        key: StaticSecret,
    ) -> Result<(), Self::Error>;

    /// Retrieve all previously stored vault key for a given storage id.
    async fn vault_key_for(
        &self,
        storage_id: StorageId,
    ) -> Result<Option<StaticSecret>, Self::Error>;
}

#[derive(Serialize, Deserialize)]
struct EncryptionKey([u8; 32], #[serde(skip)] Option<region::LockGuard>);

impl EncryptionKey {
    pub fn new(secret: &StaticSecret) -> Self {
        let mut new_key = Self(secret.to_bytes(), None); // TODO if we can convince the x25519-dalek crate to expose an as_bytes() method, we could lock the page on their behalf.
        new_key.lock_memory();
        new_key
    }

    pub fn key(&self) -> &[u8] {
        &self.0
    }

    pub fn as_static_secret(&self) -> StaticSecret {
        StaticSecret::from(self.0)
    }

    pub fn public_key(&self) -> PublicKey {
        PublicKey::from(&self.as_static_secret())
    }

    pub fn lock_memory(&mut self) {
        if self.1.is_none() {
            match region::lock(self.key().as_ptr(), self.key().len()) {
                Ok(guard) => self.1 = Some(guard),
                Err(err) => eprintln!("Security Warning: Unable to lock memory {:?}", err),
            }
        }
    }

    pub fn random() -> Self {
        Self::new(&StaticSecret::new(x25519_rand::thread_rng()))
    }

    // pub fn encrypt_payload_with_public_key(
    //     &self,
    //     key_id: KeyId,
    //     key_version: u32,
    //     payload: &[u8],
    //     public_key: &PublicKey,
    // ) -> VaultPayload<'static> {
    //     let mut rng = thread_rng();
    //     let nonce: [u8; 24] = rng.gen();
    //     let payload = (&self.as_static_secret())
    //         .encrypt(payload, b"", &nonce)
    //         .unwrap();
    //     VaultPayload {
    //         key_id,
    //         key_version,
    //         encryption: Encryption::X25519XChaCha20Poly1305,
    //         payload: Cow::Owned(payload),
    //         nonce: Cow::Owned(nonce.to_vec()),
    //     }
    // }

    pub fn encrypt_payload(
        &self,
        key_id: KeyId,
        key_version: u32,
        payload: &[u8],
    ) -> VaultPayload<'static> {
        let mut rng = thread_rng();
        let nonce: [u8; 24] = rng.gen();
        self.encrypt_payload_with_nonce(key_id, key_version, payload, &nonce)
    }

    pub fn encrypt_payload_with_nonce(
        &self,
        key_id: KeyId,
        key_version: u32,
        payload: &[u8],
        nonce: &[u8],
    ) -> VaultPayload<'static> {
        let encrypted = XChaCha20Poly1305::new(GenericArray::from_slice(&self.0))
            .encrypt(
                GenericArray::from_slice(nonce),
                Payload {
                    msg: payload,
                    aad: b"",
                },
            )
            .unwrap();
        VaultPayload {
            key_id,
            encryption: Encryption::XChaCha20Poly1305,
            payload: Cow::Owned(encrypted),
            nonce: Cow::Owned(nonce.to_vec()),
            key_version,
        }
    }

    pub fn decrypt_payload(&self, payload: &VaultPayload<'_>) -> Result<Vec<u8>, Error> {
        // This is a no-op, but it will cause a compiler error if we introduce additional encryption methods
        let encrypted = match payload.encryption {
            Encryption::XChaCha20Poly1305 => {
                XChaCha20Poly1305::new(GenericArray::from_slice(&self.0)).decrypt(
                    GenericArray::from_slice(&payload.nonce),
                    Payload {
                        msg: &payload.payload,
                        aad: b"",
                    },
                )?
            }
            Encryption::X25519XChaCha20Poly1305 => self
                .as_static_secret()
                .decrypt(payload.payload.as_ref(), b"", payload.nonce.as_ref())
                .unwrap(),
        };
        Ok(encrypted)
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateKey").finish_non_exhaustive()
    }
}

/// A [`VaultKeyStorage`] trait that wraps the Error type before returning. This
/// type is used to allow the Vault to operate without any generic parameters.
/// This trait is auto-implemented for all [`VaultKeyStorage`] implementors.
#[async_trait]
pub trait AnyVaultKeyStorage: Send + Sync + Debug {
    /// Retrieve all previously stored master keys for a given storage id.
    async fn vault_key_for(&self, storage_id: StorageId) -> Result<Option<StaticSecret>, Error>;

    /// Store a key. Each server id should have unique storage. The keys are
    /// uniquely encrypted per storage id and can only be decrypted by keys
    /// contained in the storage itself.
    async fn set_vault_key_for(
        &self,
        storage_id: StorageId,
        key: StaticSecret,
    ) -> Result<(), Error>;
}

#[async_trait]
impl<T> AnyVaultKeyStorage for T
where
    T: VaultKeyStorage,
{
    async fn vault_key_for(&self, server_id: StorageId) -> Result<Option<StaticSecret>, Error> {
        VaultKeyStorage::vault_key_for(self, server_id)
            .await
            .map_err(|err| Error::VaultKeyStorage(err.to_string()))
    }

    async fn set_vault_key_for(
        &self,
        server_id: StorageId,
        key: StaticSecret,
    ) -> Result<(), Error> {
        VaultKeyStorage::set_vault_key_for(self, server_id, key)
            .await
            .map_err(|err| Error::VaultKeyStorage(err.to_string()))
    }
}

/// Stores vault key locally on disk. This is in general considered insecure,
/// and shouldn't be used without careful consideration.
///
/// The primary goal of encryption within `PliantDb` is to offer limited
/// encryption at-rest. Within these goals, the primary attack vector being
/// protected against is an attacker being able to copy the data off of the
/// disks, either by physically gaining access to the drives or having
/// filesystem access. By storing the vault key on the same physical media, the
/// encryption should be considered insecure because if you can gain access to
/// the data, you have access to the keys as well.
///
/// For production environments, it is much more secure to store the vault key
/// in a separate location. We recommand any S3-compatible backend.
#[derive(Debug, Clone)]
pub struct LocalVaultKeyStorage {
    directory: PathBuf,
}

impl LocalVaultKeyStorage {
    /// Creates a new file-based vaultr key storage, storing files within
    /// `path`. The path provided shouod be a directory. If it doesn't exist, it
    /// will be created.
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, tokio::io::Error> {
        let directory = path.as_ref().to_owned();
        if !directory.exists() {
            fs::create_dir_all(&directory).await?
        }
        Ok(Self { directory })
    }
}

/// Errors from local vault key storage.
#[derive(thiserror::Error, Debug)]
pub enum LocalVaultKeyStorageError {
    /// An error interacting with the filesystem.
    #[error("io error: {0}")]
    Io(#[from] tokio::io::Error),

    /// An error serializing or deserializing the keys.
    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// The file was not the correct size.
    #[error("file was not the correct size")]
    InvalidFile,
}

#[async_trait]
impl VaultKeyStorage for LocalVaultKeyStorage {
    type Error = LocalVaultKeyStorageError;

    async fn vault_key_for(
        &self,
        server_id: StorageId,
    ) -> Result<Option<StaticSecret>, Self::Error> {
        let server_file = self.directory.join(server_id.to_string());
        if !server_file.exists() {
            return Ok(None);
        }
        let contents = File::open(server_file)
            .and_then(|mut f| async move {
                let mut bytes = Vec::new();
                f.read_to_end(&mut bytes).await.map(|_| bytes)
            })
            .await?;

        let as_array: [u8; 32] = contents
            .try_into()
            .map_err(|_| LocalVaultKeyStorageError::InvalidFile)?;

        Ok(Some(StaticSecret::from(as_array)))
    }

    async fn set_vault_key_for(
        &self,
        server_id: StorageId,
        key: StaticSecret,
    ) -> Result<(), Self::Error> {
        let server_file = self.directory.join(server_id.to_string());
        let bytes = bincode::serialize(&key)?;
        File::create(server_file)
            .and_then(|mut file| async move {
                file.write_all(&bytes).await?;
                file.shutdown().await
            })
            .await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct VaultPayload<'a> {
    // TODO make key_id be the additional data
    key_id: KeyId,
    key_version: u32,
    encryption: Encryption,
    payload: Cow<'a, [u8]>,
    nonce: Cow<'a, [u8]>,
}

impl<'a> VaultPayload<'a> {
    fn from_slice(bytes: &'a [u8]) -> Result<Self, Error> {
        bincode::deserialize(bytes).map_err(|err| {
            Error::Encryption(format!("error deserializing encrypted payload: {:?}", err))
        })
    }

    fn to_vec(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
enum Encryption {
    XChaCha20Poly1305,
    X25519XChaCha20Poly1305,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct NullKeyStorage;
    #[async_trait]
    impl VaultKeyStorage for NullKeyStorage {
        type Error = anyhow::Error;

        async fn set_vault_key_for(
            &self,
            _storage_id: StorageId,
            _key: StaticSecret,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn vault_key_for(
            &self,
            _storage_id: StorageId,
        ) -> Result<Option<StaticSecret>, Self::Error> {
            unreachable!()
        }
    }

    fn random_null_vault() -> Vault {
        let mut master_keys = HashMap::new();
        master_keys.insert(0, EncryptionKey::random());

        Vault {
            vault_public_key: PublicKey::from(thread_rng().gen::<[u8; 32]>()),
            master_keys,
            current_master_key_id: 0,
            master_key_storage: Box::new(NullKeyStorage),
        }
    }

    #[test]
    fn vault_encryption_test() {
        let vault = random_null_vault();
        let encrypted = vault
            .encrypt_payload(&KeyId::Master, b"hello", None)
            .unwrap();
        let decrypted = vault.decrypt_payload(&encrypted, None).unwrap();

        assert_eq!(decrypted, b"hello");
    }

    #[test]
    fn vault_permissions_test() {
        let vault = random_null_vault();
        assert!(matches!(
            vault.encrypt_payload(&KeyId::Master, b"hello", Some(&Permissions::default()),),
            Err(crate::Error::Core(pliantdb_core::Error::PermissionDenied(
                _
            )))
        ));
        let encrypted = vault
            .encrypt_payload(&KeyId::Master, b"hello", None)
            .unwrap();
        assert!(matches!(
            vault.decrypt_payload(&encrypted, Some(&Permissions::default())),
            Err(crate::Error::Core(pliantdb_core::Error::PermissionDenied(
                _
            )))
        ));
    }
}
