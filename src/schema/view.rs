use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::Document;

#[derive(thiserror::Error, Debug)]
pub enum ViewError {
    #[error("error deserializing document {0}")]
    SerializationError(#[from] serde_cbor::Error),
    #[error("reduce is unimplemented")]
    ReduceUnimplemented,
}

pub type MapResult<'d, K = (), V = ()> = Result<Option<Map<'d, K, V>>, ViewError>;

pub trait View<C> {
    type MapKey: Serialize + for<'de> Deserialize<'de>;
    type MapValue: Serialize + for<'de> Deserialize<'de>;
    type Reduce: for<'de> Deserialize<'de>;

    fn name(&self) -> Cow<'static, str>;

    fn map<'d>(&self, document: &'d Document<C>) -> MapResult<'d, Self::MapKey, Self::MapValue>;

    #[allow(unused_variables)]
    fn reduce(
        &self,
        mappings: &[Map<'_, Self::MapKey, Self::MapValue>],
        rereduce: bool,
    ) -> Result<Self::Reduce, ViewError> {
        Err(ViewError::ReduceUnimplemented)
    }

    fn boxed(self) -> Box<dyn AnyView<C>>
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

impl<'a> Into<SerializableValue<'a, ()>> for () {
    fn into(self) -> SerializableValue<'a, Self> {
        SerializableValue::Owned(())
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

pub struct Map<'a, K: Serialize = (), V: Serialize = ()> {
    pub source: Uuid,
    pub key: SerializableValue<'a, K>,
    pub value: SerializableValue<'a, V>,
}

pub struct SerializedMap {
    pub source: Uuid,
    pub key: serde_cbor::Value,
    pub value: serde_cbor::Value,
}

pub trait AnyView<C> {
    fn name(&self) -> Cow<'static, str>;
    fn map(&self, document: &Document<C>) -> Result<Option<SerializedMap>, ViewError>;
}

impl<C, T> AnyView<C> for T
where
    T: View<C>,
{
    fn name(&self) -> Cow<'static, str> {
        View::<C>::name(self)
    }

    fn map(&self, document: &Document<C>) -> Result<Option<SerializedMap>, ViewError> {
        let map = View::<C>::map(self, document)?;

        match map {
            Some(map) => Ok(Some(SerializedMap {
                source: map.source,
                key: serde_cbor::value::to_value(map.key.as_ref())?,
                value: serde_cbor::value::to_value(map.value.as_ref())?,
            })),
            None => Ok(None),
        }
    }
}
