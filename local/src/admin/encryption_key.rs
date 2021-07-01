use std::{borrow::Cow, io::Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use pliantdb_core::{
    connection::Connection,
    document::Document,
    schema::{
        view, Collection, CollectionName, InvalidNameError, Key, MapResult, MappedValue, Name,
        Schematic, View,
    },
    Error,
};
use serde::{Deserialize, Serialize};

use crate::{vault, Database};

use super::Admin;

// TODO validations: this type should prevent changing the key_id.
#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::module_name_repetitions)]
pub struct EncryptionKeyVersion {
    /// The ID of the key. Multiple keys can exist for the same id.
    pub key_id: String,

    /// The key, encrypted with the master key.
    pub encrypted_key: vault::EncryptedKey,
}

impl EncryptionKeyVersion {
    pub async fn latest(key_id: &str, db: &Database<Admin>) -> Result<Option<Self>, crate::Error> {
        let encrypted_key = db
            .view::<ByKeyId>()
            .with_key_range(
                // TODO this should be an inclusive range
                KeyVersionId {
                    key_id: key_id.to_string(),
                    version_id: 0,
                }..KeyVersionId {
                    key_id: key_id.to_string(),
                    version_id: u32::MAX,
                },
            )
            .reduce()
            .await?;

        Ok(encrypted_key.map(|encrypted_key| Self {
            key_id: key_id.to_string(),
            encrypted_key,
        }))
    }
}

impl Collection for EncryptionKeyVersion {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "encryption-key")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ByKeyId)
    }
}

#[derive(Debug)]
pub struct ByKeyId;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyVersionId {
    pub key_id: String,
    pub version_id: u32,
}

impl Key for KeyVersionId {
    fn as_big_endian_bytes(&self) -> anyhow::Result<std::borrow::Cow<'_, [u8]>> {
        let mut bytes = Vec::with_capacity(self.key_id.len() + 4);
        {
            let writer = &mut bytes;
            writer.write_all(self.key_id.as_bytes())?;
            writer.write_u32::<BigEndian>(self.version_id)?;
        }
        assert!(bytes.len() == bytes.capacity());
        Ok(Cow::from(bytes))
    }

    fn from_big_endian_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let id_len = bytes.len() - 4;
        let mut key_id = Vec::with_capacity(id_len);
        key_id.extend_from_slice(&bytes[0..id_len]);
        let key_id = String::from_utf8(key_id)?;

        let version_id = (&bytes[id_len..]).read_u32::<BigEndian>()?;
        Ok(Self { key_id, version_id })
    }
}

impl View for ByKeyId {
    type Collection = EncryptionKeyVersion;
    type Key = KeyVersionId;
    type Value = Option<vault::EncryptedKey>;

    fn unique(&self) -> bool {
        true
    }

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-key-id")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let key = document.contents::<EncryptionKeyVersion>()?;
        Ok(Some(document.emit_key_and_value(
            KeyVersionId {
                key_id: key.key_id,
                version_id: document.header.revision.id,
            },
            Some(key.encrypted_key),
        )))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings
            .iter()
            .max_by_key(|mapping| mapping.key.version_id)
            .map(|mapping| mapping.value.clone())
            .expect("mapping shouldn't ever be empty"))
    }
}
