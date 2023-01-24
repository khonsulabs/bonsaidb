use std::any::type_name;
use std::collections::HashMap;
use std::marker::PhantomData;

use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::document::DocumentId;
use bonsaidb_core::schema::CollectionName;
use bonsaidb_core::transaction::{ChangedDocument, ChangedKey, Changes, DocumentChanges};
use serde::{Deserialize, Serialize};
use transmog_versions::Versioned;

#[derive(thiserror::Error)]
pub struct UnknownVersion<T>(PhantomData<T>);

impl<T> Default for UnknownVersion<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> std::fmt::Debug for UnknownVersion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UnknownVersion")
            .field(&type_name::<T>())
            .finish()
    }
}

impl<T> std::fmt::Display for UnknownVersion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "incompatilbe version of {}", type_name::<T>())
    }
}

#[derive(Clone, Copy, Debug)]
enum ChangesVersions {
    Legacy = 0,
    V1 = 1,
}

impl Versioned for ChangesVersions {
    fn version(&self) -> u64 {
        *self as u64
    }
}

impl TryFrom<u64> for ChangesVersions {
    type Error = UnknownVersion<Changes>;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ChangesVersions::Legacy),
            1 => Ok(ChangesVersions::V1),
            _ => Err(UnknownVersion::default()),
        }
    }
}

pub fn deserialize_executed_transaction_changes(data: &[u8]) -> Result<Changes, crate::Error> {
    let (version, data) = transmog_versions::unwrap_version(data);
    match ChangesVersions::try_from(version)? {
        ChangesVersions::Legacy => {
            let legacy: ChangesV0 = match pot::from_slice(data) {
                Ok(changes) => changes,
                Err(pot::Error::NotAPot) => ChangesV0::Documents(bincode::deserialize(data)?),
                other => other?,
            };
            Changes::try_from(legacy).map_err(crate::Error::from)
        }
        ChangesVersions::V1 => pot::from_slice(data).map_err(crate::Error::from),
    }
}

pub fn serialize_executed_transaction_changes(changes: &Changes) -> Result<Vec<u8>, crate::Error> {
    let mut serialized = Vec::new();
    transmog_versions::write_header(&ChangesVersions::V1, &mut serialized)?;
    pot::to_writer(changes, &mut serialized)?;
    Ok(serialized)
}

/// A list of changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChangesV0 {
    /// A list of changed documents.
    Documents(Vec<ChangedDocumentV0>),
    /// A list of changed keys.
    Keys(Vec<ChangedKey>),
}

impl TryFrom<ChangesV0> for Changes {
    type Error = bonsaidb_core::Error;

    fn try_from(legacy: ChangesV0) -> Result<Self, Self::Error> {
        match legacy {
            ChangesV0::Documents(legacy_documents) => {
                let mut changed_documents = Vec::with_capacity(legacy_documents.len());
                let mut collections = Vec::new();
                let mut collection_indexes = HashMap::new();
                for changed in legacy_documents {
                    let collection = if let Some(id) = collection_indexes.get(&changed.collection) {
                        *id
                    } else {
                        let id = u16::try_from(collections.len()).unwrap();
                        collection_indexes.insert(changed.collection.clone(), id);
                        collections.push(changed.collection);
                        id
                    };
                    changed_documents.push(ChangedDocument {
                        collection,
                        id: changed.id.try_into()?,
                        deleted: changed.deleted,
                    });
                }
                Ok(Self::Documents(DocumentChanges {
                    collections,
                    documents: changed_documents,
                }))
            }
            ChangesV0::Keys(changes) => Ok(Self::Keys(changes)),
        }
    }
}

/// A record of a changed document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedDocumentV0 {
    /// The id of the `Collection` of the changed `Document`.
    pub collection: CollectionName,

    /// The id of the changed `Document`.
    pub id: LegacyDocumentId,

    /// If the `Document` has been deleted, this will be `true`.
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LegacyDocumentId {
    U64(u64),
    Document(Bytes),
}

impl TryFrom<LegacyDocumentId> for DocumentId {
    type Error = bonsaidb_core::Error;

    fn try_from(id: LegacyDocumentId) -> Result<Self, Self::Error> {
        match id {
            LegacyDocumentId::Document(id) => DocumentId::try_from(&id[..]),
            LegacyDocumentId::U64(version) => Ok(DocumentId::from_u64(version)),
        }
    }
}
