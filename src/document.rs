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

    pub(crate) fn create_new_revision(&self, contents: Cow<'a, [u8]>) -> Option<Self> {
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

#[cfg(test)]
mod tests {
    use super::Document;
    use crate::{
        connection::Connection,
        schema::{Collection, Map},
        storage::Storage,
        test_util::{Basic, BasicCollection},
        Error,
    };

    #[tokio::test]
    async fn store_retrieve_update() -> Result<(), Error> {
        let path = std::env::temp_dir().join("store_retrieve_tests.pliantdb");
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }
        let db = Storage::<BasicCollection>::open_local(path)?;

        let original_value = Basic {
            value: String::from("initial_value"),
            parent_id: None,
        };
        let collection = db.collection::<BasicCollection>()?;
        let header = collection.push(&original_value).await?;

        let mut doc = collection
            .get(header.id)
            .await?
            .expect("couldn't retrieve stored item");
        let mut value = doc.contents::<Basic>()?;
        assert_eq!(original_value, value);
        let old_revision = doc.header.revision.clone();

        // Update the value
        value.value = String::from("updated_value");
        doc.set_contents(&value)?;
        db.update(&mut doc).await?;

        // update should cause the revision to be changed
        assert_ne!(doc.header.revision, old_revision);

        // Check the value in the database to ensure it has the new document
        let doc = collection
            .get(header.id)
            .await?
            .expect("couldn't retrieve stored item");
        assert_eq!(doc.contents::<Basic>()?, value);

        Ok(())
    }

    #[test]
    fn emissions() -> Result<(), Error> {
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
}
