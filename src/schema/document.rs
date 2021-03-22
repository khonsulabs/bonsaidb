use std::{borrow::Cow, marker::PhantomData};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{map, Collection, Map, Revision};

/// a struct representing a document in the database
#[derive(Serialize, Deserialize, Debug)]
pub struct Document<'a, C> {
    /// the id of the Document. Unique across the collection `C`
    pub id: Uuid,

    /// the revision of the stored document.
    pub revision: Revision,

    /// the serialized bytes of the stored item
    #[serde(borrow)]
    pub contents: Cow<'a, [u8]>,

    #[serde(skip)]
    _collection: PhantomData<C>,
}

impl<'a, C> Document<'a, C>
where
    C: Collection,
{
    /// create a new document with serialized bytes from `contents`
    pub fn new<S: Serialize>(contents: &S) -> Result<Self, serde_cbor::Error> {
        let contents = Cow::from(serde_cbor::to_vec(contents)?);
        let revision = Revision::new(&contents);
        Ok(Self {
            id: Uuid::new_v4(),
            revision,
            contents,
            _collection: PhantomData::default(),
        })
    }

    /// retrieves `contents` through deserialization into the type `D`
    pub fn contents<D: Deserialize<'a>>(&'a self) -> Result<D, serde_cbor::Error> {
        serde_cbor::from_slice(&self.contents)
    }

    pub(crate) fn update_with<S: Serialize>(
        &self,
        contents: &S,
    ) -> Result<Option<Self>, serde_cbor::Error> {
        let contents = Cow::from(serde_cbor::to_vec(contents)?);
        Ok(self.revision.next_revision(&contents).map(|revision| Self {
            id: self.id,
            revision,
            contents,
            _collection: PhantomData::default(),
        }))
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
        Map::new(self.id, key, value)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        connection::Connection,
        schema::Map,
        storage::Storage,
        test_util::{Basic, BasicCollection},
        Error,
    };

    use super::Document;

    #[tokio::test]
    #[ignore] // TODO make this test work
    async fn store_retrieve() -> Result<(), Error> {
        let path = std::env::temp_dir().join("store_retrieve_tests.pliantdb");
        if path.exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }
        let db = Storage::<BasicCollection>::open_local(path)?;

        let original_value = Basic { parent_id: None };
        let collection = db.collection::<BasicCollection>()?;
        let doc = collection.push(&original_value).await?;

        let doc = collection
            .get(&doc.id)
            .await?
            .expect("couldn't retrieve stored item");

        assert_eq!(original_value, doc.contents::<Basic>()?);

        Ok(())
    }

    #[test]
    fn emissions() -> Result<(), Error> {
        let doc = Document::<BasicCollection>::new(&Basic { parent_id: None })?;

        assert_eq!(doc.emit(), Map::new(doc.id, (), ()));

        assert_eq!(doc.emit_key(1), Map::new(doc.id, 1, ()));

        assert_eq!(doc.emit_value(1), Map::new(doc.id, (), 1));

        assert_eq!(doc.emit_key_and_value(1, 2), Map::new(doc.id, 1, 2));

        Ok(())
    }
}
