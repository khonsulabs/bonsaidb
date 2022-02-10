//! Encryption and secret management.
//!
//! BonsaiDb's vault is the core of encryption and secret management. To offer
//! this security, BonsaiDb relies on external [`VaultKeyStorage`] to provide
//! the key needed to decrypt the master keys. After the master keys have been
//! decrypted, the vault is able to function without [`VaultKeyStorage`]. This
//! design ensures that if a copy of a database was stolen, the data that is
//! stored at-rest cannot be decrypted without gaining access to
//! [`VaultKeyStorage`].
//!
//! ## At-Rest Encryption
//!
//! At-rest encryption only ensures that if the database files are stolen that
//! an attacker cannot access the data without the encryption key. BonsaiDb
//! will regularly decrypt data to process it, and while the data is in-memory,
//! it is subject to the security of the machine running it. If using BonsaiDb
//! over a network, the network transport layer's encryption is what ensures
//! your data's safety.
//!
//! ## Security Best Practices
//!
//! ### Vault Key Storage
//!
//! In most situations, do not use [`LocalVaultKeyStorage`] in a production
//! environment. If you store the vault keys on the same disk as the database,
//! it's similar to hiding a key to your house under your doormat. It might stop
//! the casual person from entering your house, but give any attacker a few
//! minutes, and they'll find the key.
//!
//! Instead, you should use a storage location that provides authentication and
//! encryption. Our recommendation for production enviroments is to find an
//! Amazon S3-compatible storage service and use
//! [`S3VaultKeyStorage`](https://dev.bonsaidb.io/main/docs/bonsaidb_keystorage_s3/struct.S3VaultKeyStorage.html).
//! Eventually, other BonsaiDb servers will be able to operate as key storage
//! for each other.
//!
//! ## Encryption Algorithms Used
//!
//! BonsaiDb uses the [`hpke`](https://github.com/rozbb/rust-hpke) crate to
//! provide Hybrid Public Key Encryption (HPKE) when public key encryption is
//! being used. This is currently only utilized for encrypting the master keys
//! with the vault key. Our HPKE uses `P256+HKDF-SHA256+ChaCha20Poly1305`.
//! Long term, we plan to offer public key encryption APIs on top of these same
//! choices.
//!
//! For at-rest data encryption, the [`AEAD`
//! `XChaCha20Poly1305`](https://github.com/RustCrypto/AEADs) implementation is
//! used directly. This variant of `ChaCha20Poly1305` extends the nonce from 12
//! bytes to 24 bytes, which allows for random nonces to be used.

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{Debug, Display},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    document::KeyId,
    permissions::{
        bonsai::{encryption_key_resource_name, EncryptionKeyAction},
        Permissions,
    },
};
use chacha20poly1305::{
    aead::{generic_array::GenericArray, Aead, NewAead, Payload},
    XChaCha20Poly1305,
};
use futures::TryFutureExt;
use hpke::{
    self,
    aead::{AeadTag, ChaCha20Poly1305},
    kdf::HkdfSha256,
    kem::DhP256HkdfSha256,
    Deserializable, Kem, OpModeS, Serializable,
};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
};
use zeroize::{Zeroize, Zeroizing};

/// A private encryption key.
#[derive(Serialize, Deserialize)]
pub enum KeyPair {
    /// A P256 keypair.
    P256 {
        /// The private key.
        private: <DhP256HkdfSha256 as Kem>::PrivateKey,
        /// The public key.
        public: <DhP256HkdfSha256 as Kem>::PublicKey,
    },
}

impl KeyPair {
    /// Serializes the private key into bytes.
    pub fn to_bytes(&self) -> Result<Zeroizing<Vec<u8>>, Error> {
        Ok(Zeroizing::new(bincode::serialize(self)?))
    }

    /// Deserializes the private key.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        bincode::deserialize(bytes).map_err(Error::from)
    }
}

/// A public key corresponding to a [`KeyPair`].
#[derive(Serialize, Deserialize)]
pub enum PublicKey {
    /// A P256 public key.
    P256(<DhP256HkdfSha256 as Kem>::PublicKey),
}

impl PublicKey {
    /// Serializes the public key into bytes.
    pub fn to_bytes(&self) -> Result<Zeroizing<Vec<u8>>, Error> {
        Ok(Zeroizing::new(bincode::serialize(self)?))
    }

    /// Deserializes the public key.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        bincode::deserialize(bytes).map_err(Error::from)
    }
}

impl<'a> From<&'a KeyPair> for PublicKey {
    fn from(key: &'a KeyPair) -> Self {
        match key {
            KeyPair::P256 { public, .. } => PublicKey::P256(public.clone()),
        }
    }
}

use crate::storage::StorageId;

pub(crate) struct Vault {
    _vault_public_key: PublicKey,
    master_keys: HashMap<u32, EncryptionKey>,
    current_master_key_id: u32,
    master_key_storage: Box<dyn AnyVaultKeyStorage>,
}

impl Debug for Vault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vault")
            .field("master_keys", &self.master_keys)
            .field("current_master_key_id", &self.current_master_key_id)
            .field("master_key_storage", &self.master_key_storage)
            .finish_non_exhaustive()
    }
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

impl From<hpke::HpkeError> for Error {
    fn from(err: hpke::HpkeError) -> Self {
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
            Self::unseal(&master_keys_path, server_id, master_key_storage).await
        } else {
            Self::initialize_vault_key_storage(&master_keys_path, server_id, master_key_storage)
                .await
        }
    }

    async fn initialize_vault_key_storage(
        master_keys_path: &Path,
        server_id: StorageId,
        master_key_storage: Box<dyn AnyVaultKeyStorage>,
    ) -> Result<Self, Error> {
        let master_key = EncryptionKey::random();
        let (private, public) = DhP256HkdfSha256::gen_keypair(&mut thread_rng());

        master_key_storage
            .set_vault_key_for(
                server_id,
                KeyPair::P256 {
                    private: private.clone(),
                    public: public.clone(),
                },
            )
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
        let expected_public_key_bytes = PublicKey::P256(public.clone()).to_bytes().unwrap();
        let retrieved_key_matches = retrieved
            .map(|r| PublicKey::from(&r).to_bytes().ok() == Some(expected_public_key_bytes))
            .unwrap_or_default();
        if retrieved_key_matches {
            let mut serialized_master_keys = bincode::serialize(&master_keys)?;

            let (encapsulated_key, aead_tag) = hpke::single_shot_seal_in_place_detached::<
                ChaCha20Poly1305,
                HkdfSha256,
                DhP256HkdfSha256,
                _,
            >(
                &OpModeS::Base,
                &public,
                b"",
                &mut serialized_master_keys,
                b"",
                &mut thread_rng(),
            )?;
            let mut tag = [0_u8; 16];
            tag.copy_from_slice(&aead_tag.to_bytes());

            let encrypted_master_keys_payload = bincode::serialize(&HpkePayload {
                encryption: PublicKeyEncryption::DhP256HkdfSha256ChaCha20,
                payload: Bytes::from(serialized_master_keys),
                encapsulated_key,
                tag,
            })?;

            File::create(master_keys_path)
                .and_then(|mut file| async move {
                    file.write_all(&encrypted_master_keys_payload).await?;
                    file.shutdown().await
                })
                .await
                .map_err(|err| Error::Initializing(format!("error saving vault key: {:?}", err)))?;

            Ok(Self {
                _vault_public_key: PublicKey::P256(public),
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

    async fn unseal(
        master_keys_path: &Path,
        server_id: StorageId,
        master_key_storage: Box<dyn AnyVaultKeyStorage>,
    ) -> Result<Self, Error> {
        // The vault has been initilized previously. Do not overwrite this file voluntarily.
        let encrypted_master_keys = tokio::fs::read(master_keys_path)
            .await
            .map_err(|err| Error::Initializing(format!("error reading master keys: {:?}", err)))?;
        let mut encrypted_master_keys =
            bincode::deserialize::<HpkePayload>(&encrypted_master_keys)?;
        let PublicKeyEncryption::DhP256HkdfSha256ChaCha20 = &encrypted_master_keys.encryption;
        if let Some(vault_key) = master_key_storage
            .vault_key_for(server_id)
            .await
            .map_err(|err| Error::VaultKeyStorage(err.to_string()))?
        {
            let master_keys = match &vault_key {
                KeyPair::P256 { private, .. } => {
                    let mut decryption_context =
                        hpke::setup_receiver::<ChaCha20Poly1305, HkdfSha256, DhP256HkdfSha256>(
                            &hpke::OpModeR::Base,
                            private,
                            &encrypted_master_keys.encapsulated_key,
                            b"",
                        )
                        .unwrap();

                    decryption_context
                        .open_in_place_detached(
                            &mut encrypted_master_keys.payload.0,
                            b"",
                            &AeadTag::<ChaCha20Poly1305>::from_bytes(&encrypted_master_keys.tag)
                                .unwrap(),
                        )
                        .unwrap();

                    bincode::deserialize::<HashMap<u32, EncryptionKey>>(
                        &encrypted_master_keys.payload,
                    )?
                }
            };

            let current_master_key_id = *master_keys.keys().max().unwrap();
            Ok(Self {
                _vault_public_key: PublicKey::from(&vault_key),
                master_keys,
                current_master_key_id,
                master_key_storage,
            })
        } else {
            Err(Error::VaultKeyNotFound)
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
            permissions.check(
                encryption_key_resource_name(key_id),
                &EncryptionKeyAction::Encrypt,
            )?;
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
        if let Ok(payload) = VaultPayload::from_slice(payload).map_err(|err| {
            Error::Encryption(format!("error deserializing encrypted payload: {:?}", err))
        }) {
            self.decrypt(&payload, permissions)
        } else {
            // If we can't parse it as a VaultPayload, it might have been stored
            // decrypted originally.
            Ok(payload.to_vec())
        }
    }

    fn decrypt(
        &self,
        payload: &VaultPayload<'_>,
        permissions: Option<&Permissions>,
    ) -> Result<Vec<u8>, crate::Error> {
        if let Some(permissions) = permissions {
            permissions.check(
                encryption_key_resource_name(&payload.key_id),
                &EncryptionKeyAction::Decrypt,
            )?;
        }

        // TODO handle key version
        let key = match &payload.key_id {
            KeyId::Master => self.current_master_key(),
            KeyId::Id(_) => todo!(),
            KeyId::None => unreachable!(),
        };
        Ok(key.decrypt_payload(payload)?)
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
        key: KeyPair,
    ) -> Result<(), Self::Error>;

    /// Retrieve all previously stored vault key for a given storage id.
    async fn vault_key_for(&self, storage_id: StorageId) -> Result<Option<KeyPair>, Self::Error>;
}

#[derive(Serialize, Deserialize)]
struct EncryptionKey([u8; 32], #[serde(skip)] Option<region::LockGuard>);

impl EncryptionKey {
    pub fn new(secret: [u8; 32]) -> Self {
        let mut new_key = Self(secret, None);
        new_key.lock_memory();
        new_key
    }

    pub fn key(&self) -> &[u8] {
        &self.0
    }

    pub fn lock_memory(&mut self) {
        if self.1.is_none() {
            match region::lock(self.key().as_ptr(), self.key().len()) {
                Ok(guard) => self.1 = Some(guard),
                Err(err) => log::error!("Security Warning: Unable to lock memory {:?}", err),
            }
        }
    }

    pub fn random() -> Self {
        Self::new(thread_rng().gen())
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
pub trait AnyVaultKeyStorage: Send + Sync + Debug + 'static {
    /// Retrieve all previously stored master keys for a given storage id.
    async fn vault_key_for(&self, storage_id: StorageId) -> Result<Option<KeyPair>, Error>;

    /// Store a key. Each server id should have unique storage. The keys are
    /// uniquely encrypted per storage id and can only be decrypted by keys
    /// contained in the storage itself.
    async fn set_vault_key_for(&self, storage_id: StorageId, key: KeyPair) -> Result<(), Error>;
}

#[async_trait]
impl<T> AnyVaultKeyStorage for T
where
    T: VaultKeyStorage + 'static,
{
    async fn vault_key_for(&self, server_id: StorageId) -> Result<Option<KeyPair>, Error> {
        VaultKeyStorage::vault_key_for(self, server_id)
            .await
            .map_err(|err| Error::VaultKeyStorage(err.to_string()))
    }

    async fn set_vault_key_for(&self, server_id: StorageId, key: KeyPair) -> Result<(), Error> {
        VaultKeyStorage::set_vault_key_for(self, server_id, key)
            .await
            .map_err(|err| Error::VaultKeyStorage(err.to_string()))
    }
}

/// Stores vault key locally on disk. This is in general considered insecure,
/// and shouldn't be used without careful consideration.
///
/// The primary goal of encryption within BonsaiDb is to offer limited
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
            fs::create_dir_all(&directory).await?;
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

    async fn vault_key_for(&self, server_id: StorageId) -> Result<Option<KeyPair>, Self::Error> {
        let server_file = self.directory.join(server_id.to_string());
        if !server_file.exists() {
            return Ok(None);
        }
        let mut contents = File::open(server_file)
            .and_then(|mut f| async move {
                let mut bytes = Vec::new();
                f.read_to_end(&mut bytes).await.map(|_| bytes)
            })
            .await?;

        let key = bincode::deserialize::<KeyPair>(&contents)?;
        contents.zeroize();

        Ok(Some(key))
    }

    async fn set_vault_key_for(
        &self,
        server_id: StorageId,
        key: KeyPair,
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
struct HpkePayload {
    encryption: PublicKeyEncryption,
    payload: Bytes,
    tag: [u8; 16],
    encapsulated_key: <DhP256HkdfSha256 as Kem>::EncappedKey,
}

#[derive(Serialize, Deserialize)]
enum Encryption {
    XChaCha20Poly1305,
}

#[derive(Serialize, Deserialize)]
enum PublicKeyEncryption {
    DhP256HkdfSha256ChaCha20,
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
            _key: KeyPair,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn vault_key_for(
            &self,
            _storage_id: StorageId,
        ) -> Result<Option<KeyPair>, Self::Error> {
            unreachable!()
        }
    }

    fn random_null_vault() -> Vault {
        let mut master_keys = HashMap::new();
        master_keys.insert(0, EncryptionKey::random());

        let (_, public_key) = <DhP256HkdfSha256 as Kem>::gen_keypair(&mut thread_rng());

        Vault {
            _vault_public_key: PublicKey::P256(public_key),
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
            Err(crate::Error::Core(bonsaidb_core::Error::PermissionDenied(
                _
            )))
        ));
        let encrypted = vault
            .encrypt_payload(&KeyId::Master, b"hello", None)
            .unwrap();
        assert!(matches!(
            vault.decrypt_payload(&encrypted, Some(&Permissions::default())),
            Err(crate::Error::Core(bonsaidb_core::Error::PermissionDenied(
                _
            )))
        ));
    }
}
