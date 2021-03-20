use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{view, Document};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error deserializing document {0}")]
    SerializationError(#[from] serde_cbor::Error),
    #[error("reduce is unimplemented")]
    ReduceUnimplemented,
}

pub type MapResult<K = (), V = ()> = Result<Option<Map<K, V>>, Error>;

pub trait View<C> {
    type MapKey: Serialize + for<'de> Deserialize<'de>;
    type MapValue: Serialize + for<'de> Deserialize<'de>;
    type Reduce: for<'de> Deserialize<'de>;

    fn name(&self) -> Cow<'static, str>;

    fn map(&self, document: &Document<C>) -> MapResult<Self::MapKey, Self::MapValue>;

    #[allow(unused_variables)]
    fn reduce(
        &self,
        mappings: &[Map<Self::MapKey, Self::MapValue>],
        rereduce: bool,
    ) -> Result<Self::Reduce, Error> {
        Err(Error::ReduceUnimplemented)
    }

    fn boxed(self) -> Box<dyn view::Serialized<C>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub enum SerializableValue<'a, T: Serialize> {
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T> From<&'a T> for SerializableValue<'a, T>
where
    T: Serialize,
{
    fn from(other: &'a T) -> SerializableValue<'a, T> {
        SerializableValue::Borrowed(other)
    }
}

impl<'a, T> AsRef<T> for SerializableValue<'a, T>
where
    T: Serialize,
{
    fn as_ref(&self) -> &T {
        match self {
            Self::Owned(value) => value,
            Self::Borrowed(value) => value,
        }
    }
}

pub struct Map<K: Serialize = (), V: Serialize = ()> {
    pub source: Uuid,
    pub key: K,
    pub value: V,
}

pub struct SerializedMap {
    pub source: Uuid,
    pub key: serde_cbor::Value,
    pub value: serde_cbor::Value,
}

pub trait Serialized<C> {
    fn name(&self) -> Cow<'static, str>;
    fn map(&self, document: &Document<C>) -> Result<Option<SerializedMap>, Error>;
}

impl<C, T> Serialized<C> for T
where
    T: View<C>,
{
    fn name(&self) -> Cow<'static, str> {
        View::<C>::name(self)
    }

    fn map(&self, document: &Document<C>) -> Result<Option<SerializedMap>, Error> {
        let map = View::<C>::map(self, document)?;

        match map {
            Some(map) => Ok(Some(SerializedMap {
                source: map.source,
                key: serde_cbor::value::to_value(&map.key)?,
                value: serde_cbor::value::to_value(&map.value)?,
            })),
            None => Ok(None),
        }
    }
}
