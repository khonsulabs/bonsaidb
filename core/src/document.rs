use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{collection, map, Map};

mod revision;
pub use revision::Revision;

/// the header of a `Document`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// the id of the Document. Unique across the collection `C`
    pub id: Uuid,

    /// the revision of the stored document.
    pub revision: Revision,
}

/// a struct representing a document in the database
#[derive(Serialize, Deserialize, Debug)]
pub struct Document<'a> {
    /// the `Id` of the `Collection` this document belongs to
    pub collection: collection::Id,

    /// the header of the document, which contains the id and `Revision`
    #[serde(borrow)]
    pub header: Cow<'a, Header>,

    /// the serialized bytes of the stored item
    #[serde(borrow)]
    pub contents: Cow<'a, [u8]>,
}

impl<'a> Document<'a> {
    /// create a new document with `contents`
    #[must_use]
    pub fn new(contents: Cow<'a, [u8]>, collection: collection::Id) -> Self {
        let revision = Revision::new(&contents);
        Self {
            header: Cow::Owned(Header {
                id: Uuid::new_v4(),
                revision,
            }),
            contents,
            collection,
        }
    }

    /// create a new document with serialized bytes from `contents`
    pub fn with_contents<S: Serialize>(
        contents: &S,
        collection: collection::Id,
    ) -> Result<Self, serde_cbor::Error> {
        let contents = Cow::from(serde_cbor::to_vec(contents)?);
        Ok(Self::new(contents, collection))
    }

    /// retrieves `contents` through deserialization into the type `D`
    pub fn contents<D: Deserialize<'a>>(&'a self) -> Result<D, serde_cbor::Error> {
        serde_cbor::from_slice(&self.contents)
    }

    /// serializes and stores `contents` into this document
    pub fn set_contents<S: Serialize>(&mut self, contents: &S) -> Result<(), serde_cbor::Error> {
        self.contents = Cow::from(serde_cbor::to_vec(contents)?);
        Ok(())
    }

    /// create a new revision. **WARNING:** This normally should not be used
    /// outside of implementing a backend for `PliantDB`. To update a document,
    /// use `set_contents()` and send the document with the existing `Revision`
    /// information.
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
                collection: self.collection.clone(),
            })
    }

    /// create a `Map` result with an empty key and value
    #[must_use]
    pub fn emit(&self) -> Map<'static, (), ()> {
        self.emit_key_and_value((), ())
    }

    /// create a `Map` result with a `key` and an empty value
    #[must_use]
    pub fn emit_key<'k, Key: map::Key<'k>>(&self, key: Key) -> Map<'k, Key, ()> {
        self.emit_key_and_value(key, ())
    }

    /// create a `Map` result with `value` and an empty key
    #[must_use]
    pub fn emit_value<Value: Serialize>(&self, value: Value) -> Map<'static, (), Value> {
        self.emit_key_and_value((), value)
    }

    /// create a `Map` result with a `key` and `value`
    #[must_use]
    pub fn emit_key_and_value<'k, Key: map::Key<'k>, Value: Serialize>(
        &self,
        key: Key,
        value: Value,
    ) -> Map<'k, Key, Value> {
        Map::new(self.header.id, key, value)
    }

    /// clone the document's data so that it's no longer borrowed in the original lifetime `'a`
    #[must_use]
    pub fn to_owned(&self) -> Document<'static> {
        Document::<'static> {
            collection: self.collection.clone(),
            header: Cow::Owned(self.header.as_ref().clone()),
            contents: Cow::Owned(self.contents.as_ref().to_vec()),
        }
    }
}

#[test]
fn emissions_tests() -> Result<(), crate::Error> {
    use crate::{
        schema::{Collection, Map},
        test_util::{Basic, BasicCollection},
    };

    let doc = Document::with_contents(
        &Basic {
            value: String::default(),
            parent_id: None,
        },
        BasicCollection::id(),
    )?;

    assert_eq!(doc.emit(), Map::new(doc.header.id, (), ()));

    assert_eq!(doc.emit_key(1), Map::new(doc.header.id, 1, ()));

    assert_eq!(doc.emit_value(1), Map::new(doc.header.id, (), 1));

    assert_eq!(doc.emit_key_and_value(1, 2), Map::new(doc.header.id, 1, 2));

    Ok(())
}
