use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{Map, Revision, SerializableValue};

pub struct Document<C> {
    pub id: Uuid,
    pub revision: Revision,
    pub collection: Option<C>,
    pub contents: Vec<u8>,
}

impl<C> Document<C> {
    pub fn contents<'a, D: Deserialize<'a>>(&'a self) -> Result<D, serde_cbor::Error> {
        serde_cbor::from_slice(&self.contents)
    }

    pub fn set_contents<S: Serialize>(&mut self, contents: &S) -> Result<(), serde_cbor::Error> {
        self.contents = serde_cbor::to_vec(contents)?;
        Ok(())
    }

    pub fn emit_nothing(&self) -> Map<'static, (), ()> {
        self.emit_with((), ())
    }

    pub fn emit<'a, K: Into<SerializableValue<'a, Key>>, Key: Serialize>(
        &self,
        key: K,
    ) -> Map<'a, Key, ()> {
        self.emit_with(key, ())
    }

    pub fn emit_with<
        'a,
        K: Into<SerializableValue<'a, Key>>,
        V: Into<SerializableValue<'a, Value>>,
        Key: Serialize,
        Value: Serialize,
    >(
        &self,
        key: K,
        value: V,
    ) -> Map<'a, Key, Value> {
        Map {
            source: self.id,
            key: key.into(),
            value: value.into(),
        }
    }
}
