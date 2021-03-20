use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{Map, Revision};

use super::Collection;

pub struct Document<C> {
    pub id: Uuid,
    pub revision: Revision,
    pub collection: Option<C>,
    pub contents: Vec<u8>,
}

impl<C> Document<C>
where
    C: Collection + Clone,
{
    pub fn new<S: Serialize>(contents: &S, collection: C) -> Result<Self, serde_cbor::Error> {
        let contents = serde_cbor::to_vec(contents)?;
        let revision = Revision::new(&contents);
        Ok(Self {
            id: Uuid::new_v4(),
            collection: Some(collection),
            revision,
            contents,
        })
    }

    pub fn contents<'a, D: Deserialize<'a>>(&'a self) -> Result<D, serde_cbor::Error> {
        serde_cbor::from_slice(&self.contents)
    }

    pub fn update_with<S: Serialize>(
        &self,
        contents: &S,
    ) -> Result<Option<Self>, serde_cbor::Error> {
        let contents = serde_cbor::to_vec(contents)?;
        Ok(self.revision.next_revision(&contents).map(|revision| Self {
            id: self.id,
            revision,
            collection: self.collection.clone(),
            contents,
        }))
    }

    pub fn emit_nothing(&self) -> Map<(), ()> {
        self.emit_with((), ())
    }

    pub fn emit<Key: Serialize>(&self, key: Key) -> Map<Key, ()> {
        self.emit_with(key, ())
    }

    pub fn emit_with<Key: Serialize, Value: Serialize>(
        &self,
        key: Key,
        value: Value,
    ) -> Map<Key, Value> {
        Map {
            source: self.id,
            key,
            value,
        }
    }
}
