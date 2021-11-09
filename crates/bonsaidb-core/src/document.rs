use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::schema::{Collection, CollectionSerializer, Key, Map};

mod revision;
pub use revision::Revision;

/// The header of a `Document`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// The id of the Document. Unique across the collection `C`
    pub id: u64,

    /// The revision of the stored document.
    pub revision: Revision,
}

/// Contains a serialized document in the database.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Document<'a> {
    /// The header of the document, which contains the id and `Revision`.
    pub header: Cow<'a, Header>,

    /// The serialized bytes of the stored item.
    pub contents: Cow<'a, [u8]>,
}

impl<'a> Document<'a> {
    /// Creates a new document with `contents`.
    #[must_use]
    pub fn new(id: u64, contents: Cow<'a, [u8]>) -> Self {
        let revision = Revision::new(&contents);
        Self {
            header: Cow::Owned(Header { id, revision }),
            contents,
        }
    }

    /// Creates a new document with serialized bytes from `contents`.
    pub fn with_contents<S: Collection + Serialize>(
        id: u64,
        contents: &S,
    ) -> Result<Self, crate::Error> {
        let contents = Cow::from(<S as Collection>::serializer().serialize(contents)?);
        Ok(Self::new(id, contents))
    }

    /// Retrieves `contents` through deserialization into the type `D`.
    pub fn contents<D: Collection + Deserialize<'a>>(&'a self) -> Result<D, crate::Error> {
        match D::serializer() {
            CollectionSerializer::Pot => {
                pot::from_slice(&self.contents).map_err(crate::Error::from)
            }
            #[cfg(feature = "json")]
            CollectionSerializer::Json => {
                serde_json::from_slice(&self.contents).map_err(crate::Error::from)
            }
            #[cfg(feature = "cbor")]
            CollectionSerializer::Cbor => {
                serde_cbor::from_slice(&self.contents).map_err(crate::Error::from)
            }
            #[cfg(feature = "bincode")]
            CollectionSerializer::Bincode => {
                bincode::deserialize(&self.contents).map_err(crate::Error::from)
            }
        }
    }

    /// Serializes and stores `contents` into this document.
    pub fn set_contents<S: Collection + Serialize>(
        &mut self,
        contents: &S,
    ) -> Result<(), crate::Error> {
        self.contents = Cow::from(<S as Collection>::serializer().serialize(contents)?);
        Ok(())
    }

    /// Creates a new revision.
    ///
    /// **WARNING: This normally should not be used** outside of implementing a
    /// backend for `BonsaiDb`. To update a document, use `set_contents()` and
    /// send the document with the existing `Revision` information.
    #[must_use]
    pub fn create_new_revision(&self, contents: Cow<'a, [u8]>) -> Option<Self> {
        self.header
            .revision
            .next_revision(&contents)
            .map(|revision| Self {
                header: Cow::Owned(Header {
                    id: self.header.id,
                    revision,
                }),
                contents,
            })
    }

    /// Creates a `Map` result with an empty key and value.
    #[must_use]
    pub fn emit(&self) -> Map<(), ()> {
        self.emit_key_and_value((), ())
    }

    /// Creates a `Map` result with a `key` and an empty value.
    #[must_use]
    pub fn emit_key<K: Key>(&self, key: K) -> Map<K, ()> {
        self.emit_key_and_value(key, ())
    }

    /// Creates a `Map` result with `value` and an empty key.
    #[must_use]
    pub fn emit_value<Value: Serialize>(&self, value: Value) -> Map<(), Value> {
        self.emit_key_and_value((), value)
    }

    /// Creates a `Map` result with a `key` and `value`.
    #[must_use]
    pub fn emit_key_and_value<K: Key, Value: Serialize>(
        &self,
        key: K,
        value: Value,
    ) -> Map<K, Value> {
        Map::new(self.header.id, key, value)
    }

    /// Clone the document's data so that it's no longer borrowed in the original lifetime `'a`.
    #[must_use]
    pub fn to_owned(&self) -> Document<'static> {
        Document::<'static> {
            header: Cow::Owned(self.header.as_ref().clone()),
            contents: Cow::Owned(self.contents.as_ref().to_vec()),
        }
    }
}

/// The ID of an encryption key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum KeyId {
    /// A key with no id.
    None,
    /// The master key of the vault.
    Master,
    /// A specific named key in the vault.
    Id(Cow<'static, str>),
}

#[test]
fn emissions_tests() -> Result<(), crate::Error> {
    use crate::{schema::Map, test_util::Basic};

    let doc = Document::with_contents(1, &Basic::default())?;

    assert_eq!(doc.emit(), Map::new(doc.header.id, (), ()));

    assert_eq!(doc.emit_key(1), Map::new(doc.header.id, 1, ()));

    assert_eq!(doc.emit_value(1), Map::new(doc.header.id, (), 1));

    assert_eq!(doc.emit_key_and_value(1, 2), Map::new(doc.header.id, 1, 2));

    Ok(())
}