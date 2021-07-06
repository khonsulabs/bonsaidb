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
use zeroize::Zeroize;

use crate::storage::StorageId;

#[derive(Debug)]
pub(crate) struct Vault {
    master_keys: HashMap<u32, EncryptionKey>,
    current_master_key_id: u32,
    master_key_storage: Box<dyn AnyMasterKeyStorage>,
}

/// Errors relating to encryption and/or secret storage.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred during encryption or decryption.
    #[error("error with encryption: {0}")]
    Encryption(String),
    /// An error occurred within the master key storage.
    #[error("error from master key storage: {0}")]
    MasterKeyStorage(String),
    /// An error occurred initializing the vault.
    #[error("error occurred while initializing: {0}")]
    Initializing(String),
    /// A previously initialized vault was found, but the master key storage
    /// doesn't contain the master key.
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
            let mut sealing_key =
                bincode::deserialize::<EncryptionKey>(&sealing_key).map_err(|err| {
                    Error::Initializing(format!("error deserializing sealing key: {:?}", err))
                })?;
            sealing_key.lock_memory();
            if let Some(encrypted_keys) = master_key_storage
                .master_keys_for(server_id)
                .await
                .map_err(|err| Error::MasterKeyStorage(err.to_string()))?
            {
                let current_master_key_id = *encrypted_keys.keys().max().unwrap();
                Ok(Self {
                    master_keys: encrypted_keys
                        .into_iter()
                        .map(|(id, key)| key.decrypt(&sealing_key).map(|key| (id, key)))
                        .collect::<Result<_, _>>()?,
                    current_master_key_id,
                    master_key_storage,
                })
            } else {
                Err(Error::MasterKeyNotFound)
            }
        } else {
            let master_key = EncryptionKey::random();
            let (sealing_key, encrypted_master_key) = master_key.encrypt_key();
            master_key_storage
                .set_master_key_for(server_id, 0, &encrypted_master_key)
                .await
                .map_err(|err| Error::MasterKeyStorage(err.to_string()))?;
            let mut master_keys = HashMap::new();
            master_keys.insert(0_u32, master_key);
            // Beacuse this is such a critical step, let's verify that we can
            // retrieve the key before we store the sealing key.
            let retrieved = master_key_storage
                .master_keys_for(server_id)
                .await
                .map_err(|err| Error::MasterKeyStorage(err.to_string()))?;
            if retrieved
                .map(|r| r.get(&0) == Some(&encrypted_master_key))
                .unwrap_or_default()
            {
                let sealing_key_bytes =
                    bincode::serialize(&sealing_key).expect("error serializing sealing key");

                File::create(sealing_key_path)
                    .and_then(|mut file| async move {
                        file.write_all(&sealing_key_bytes).await?;
                        file.shutdown().await
                    })
                    .await
                    .map_err(|err| {
                        Error::Initializing(format!("error saving sealing key: {:?}", err))
                    })?;

                Ok(Self {
                    master_keys,
                    current_master_key_id: 0,
                    master_key_storage,
                })
            } else {
                Err(Error::MasterKeyStorage(String::from(
                    "master key storage failed to return the same stored key during initialization",
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
pub trait MasterKeyStorage: Send + Sync + Debug + 'static {
    /// The error type that the functions return.
    type Error: Display;
    /// Store a key. Each server id should have unique storage. The keys are
    /// uniquely encrypted per storage id and can only be decrypted by keys
    /// contained in the storage itself.
    async fn set_master_key_for(
        &self,
        storage_id: StorageId,
        key_id: u32,
        keys: &EncryptedKey,
    ) -> Result<(), Self::Error>;

    /// Retrieve all previously stored master keys for a given storage id.
    async fn master_keys_for(
        &self,
        storage_id: StorageId,
    ) -> Result<Option<HashMap<u32, EncryptedKey>>, Self::Error>;
}

#[derive(Serialize, Deserialize)]
struct EncryptionKey([u8; 32], #[serde(skip)] Option<region::LockGuard>);

impl EncryptionKey {
    pub fn new(key: [u8; 32]) -> Self {
        let mut new_key = Self(key, None);
        new_key.lock_memory();
        new_key
    }

    pub fn lock_memory(&mut self) {
        if self.1.is_none() {
            match region::lock(self.0.as_ptr(), self.0.len()) {
                Ok(guard) => self.1 = Some(guard),
                Err(err) => eprintln!("Security Warning: Unable to lock memory {:?}", err),
            }
        }
    }

    pub fn random() -> Self {
        Self::new(thread_rng().gen())
    }

    pub fn encrypt_key(&self) -> (Self, EncryptedKey) {
        let wrapping_key = Self::random();
        let encrypted = wrapping_key.encrypt_payload(KeyId::None, 0, &self.0);
        (wrapping_key, EncryptedKey(encrypted.to_vec()))
    }

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
        let Encryption::XChaCha20Poly1305 = &payload.encryption;
        Ok(
            XChaCha20Poly1305::new(GenericArray::from_slice(&self.0)).decrypt(
                GenericArray::from_slice(&payload.nonce),
                Payload {
                    msg: &payload.payload,
                    aad: b"",
                },
            )?,
        )
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

/// An encrypted encryption key.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Zeroize)]
#[zeroize(drop)]
pub struct EncryptedKey(Vec<u8>);

impl EncryptedKey {
    fn decrypt(&self, key: &EncryptionKey) -> Result<EncryptionKey, Error> {
        let decrypted = key.decrypt_payload(&VaultPayload::from_slice(&self.0)?)?;
        let decrypted = decrypted
            .try_into()
            .map_err(|err| Error::Encryption(format!("decrypted key length invalid: {:?}", err)))?;
        Ok(EncryptionKey::new(decrypted))
    }
}

impl Debug for EncryptedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedPrivateKey")
            .finish_non_exhaustive()
    }
}

/// A `MasterKeyStorage` trait that wraps the Error type before returning. This
/// type is used to allow the Vault to operate without any generic parameters.
/// This trait is auto-implemented for all `MasterKeyStorage` implementors.
#[async_trait]
pub trait AnyMasterKeyStorage: Send + Sync + Debug {
    /// Retrieve all previously stored master keys for a given storage id.
    async fn master_keys_for(
        &self,
        server_id: StorageId,
    ) -> Result<Option<HashMap<u32, EncryptedKey>>, Error>;

    /// Store a key. Each server id should have unique storage. The keys are
    /// uniquely encrypted per storage id and can only be decrypted by keys
    /// contained in the storage itself.
    async fn set_master_key_for(
        &self,
        server_id: StorageId,
        key_id: u32,
        key: &EncryptedKey,
    ) -> Result<(), Error>;
}

#[async_trait]
impl<T> AnyMasterKeyStorage for T
where
    T: MasterKeyStorage,
{
    async fn master_keys_for(
        &self,
        server_id: StorageId,
    ) -> Result<Option<HashMap<u32, EncryptedKey>>, Error> {
        MasterKeyStorage::master_keys_for(self, server_id)
            .await
            .map_err(|err| Error::MasterKeyStorage(err.to_string()))
    }

    async fn set_master_key_for(
        &self,
        server_id: StorageId,
        key_id: u32,
        key: &EncryptedKey,
    ) -> Result<(), Error> {
        MasterKeyStorage::set_master_key_for(self, server_id, key_id, key)
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
    /// Creates a new file-based master key storage, storing files within
    /// `path`. The path provided shouod be a directory. If it doesn't exist, it
    /// will be created.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            directory: path.as_ref().to_owned(),
        }
    }
}

/// Errors from local master key storage.
#[derive(thiserror::Error, Debug)]
pub enum LocalMasterKeyStorageError {
    /// An error interacting with the filesystem.
    #[error("io error: {0}")]
    Io(#[from] tokio::io::Error),

    /// An error serializing or deserializing the keys.
    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),
}

#[async_trait]
impl MasterKeyStorage for LocalMasterKeyStorage {
    type Error = LocalMasterKeyStorageError;

    async fn master_keys_for(
        &self,
        server_id: StorageId,
    ) -> Result<Option<HashMap<u32, EncryptedKey>>, Self::Error> {
        let server_folder = self.directory.join(server_id.to_string());
        if !server_folder.exists() {
            return Ok(None);
        }
        let mut entries = tokio::fs::read_dir(&server_folder).await?;
        let mut keys = HashMap::new();
        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().as_os_str().to_str() {
                let parts = name.split('.').collect::<Vec<_>>();
                if parts.len() == 3 && parts[0] == "master" && parts[2] == "key" {
                    let id = match parts[1].parse::<u32>() {
                        Ok(id) => id,
                        Err(_) => continue,
                    };
                    let contents = File::open(entry.path())
                        .and_then(|mut f| async move {
                            let mut bytes = Vec::new();
                            f.read_to_end(&mut bytes).await.map(|_| bytes)
                        })
                        .await?;
                    keys.insert(id, bincode::deserialize(&contents)?);
                }
            }
        }
        if keys.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keys))
        }
    }

    async fn set_master_key_for(
        &self,
        server_id: StorageId,
        key_id: u32,
        key: &EncryptedKey,
    ) -> Result<(), Self::Error> {
        let server_folder = self.directory.join(server_id.to_string());
        if !server_folder.exists() {
            fs::create_dir_all(&server_folder).await?;
        }
        let key_path = server_folder.join(format!("master.{}.key", key_id));
        let bytes = bincode::serialize(key)?;
        File::create(key_path)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct NullKeyStorage;
    #[async_trait]
    impl MasterKeyStorage for NullKeyStorage {
        type Error = anyhow::Error;

        async fn master_keys_for(
            &self,
            _server_id: StorageId,
        ) -> Result<Option<HashMap<u32, EncryptedKey>>, Self::Error> {
            unreachable!()
        }

        async fn set_master_key_for(
            &self,
            _server_id: StorageId,
            _key_id: u32,
            _key: &EncryptedKey,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }
    }

    fn random_null_vault() -> Vault {
        let mut master_keys = HashMap::new();
        master_keys.insert(0, EncryptionKey::random());

        Vault {
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
            vault.encrypt_payload(&KeyId::Master, b"hello", Some(&Permissions::default())),
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
