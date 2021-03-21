use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{Map, Revision};

use super::Collection;

pub struct Document<C> {
    pub id: Uuid,
    pub revision: Revision,
    pub contents: Vec<u8>,
    _collection: PhantomData<C>,
}

impl<C> Document<C>
where
    C: Collection,
{
    pub fn new<S: Serialize>(contents: &S) -> Result<Self, serde_cbor::Error> {
        let contents = serde_cbor::to_vec(contents)?;
        let revision = Revision::new(&contents);
        Ok(Self {
            id: Uuid::new_v4(),
            revision,
            contents,
            _collection: PhantomData::default(),
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
            contents,
            _collection: PhantomData::default(),
        }))
    }

    #[must_use]
    pub fn emit_nothing(&self) -> Map<(), ()> {
        self.emit_with((), ())
    }

    #[must_use]
    pub fn emit<Key: Serialize>(&self, key: Key) -> Map<Key, ()> {
        self.emit_with(key, ())
    }

    #[must_use]
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
