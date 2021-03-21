use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::Document;

/// errors that arise when interacting with views
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// an error occurred while serializing or deserializing
    #[error("error deserializing document {0}")]
    SerializationError(#[from] serde_cbor::Error),

    /// returned when
    #[error("reduce is unimplemented")]
    ReduceUnimplemented,
}

/// a type alias for the result of `View::map()`
pub type MapResult<K = (), V = ()> = Result<Option<Map<K, V>>, Error>;

/// a map/reduce powered indexing and aggregation schema
///
/// inspired by [`CouchDB`'s view system](https://docs.couchdb.org/en/stable/ddocs/views/index.html)
// TODO write our own view docs
pub trait View<C> {
    /// the key for this view
    type MapKey: Serialize + for<'de> Deserialize<'de>;

    /// an associated type that can be stored with each entry in the view
    type MapValue: Serialize + for<'de> Deserialize<'de>;

    /// when implementing reduce(), this is the returned type. If you're using
    /// ranged queries, this type must be meaningfully sortable when converted
    /// to bytes. Additionally, the conversion process to bytes must be done
    /// using a consistent endianness.
    // TODO: Don't use serialize here, use something that converts to bytes
    type Reduce: for<'de> Deserialize<'de>;

    /// TODO need versioning

    /// the name of the view. Must be unique per collection.
    fn name() -> Cow<'static, str>;

    /// the map function for this view. This function is responsible for
    /// emitting entries for any documents that should be contained in this
    /// View. If None is returned, the View will not include the document.
    fn map(document: &Document<'_, C>) -> MapResult<Self::MapKey, Self::MapValue>;

    /// the reduce function for this view. If `Err(Error::ReduceUnimplemented)`
    /// is returned, queries that ask for a reduce operation will return an
    /// error. See [`CouchDB`'s Reduce/Rereduce
    /// documentation](https://docs.couchdb.org/en/stable/ddocs/views/intro.html#reduce-rereduce)
    /// for the design this implementation will be inspired by
    #[allow(unused_variables)]
    fn reduce(
        mappings: &[Map<Self::MapKey, Self::MapValue>],
        rereduce: bool,
    ) -> Result<Self::Reduce, Error> {
        Err(Error::ReduceUnimplemented)
    }
}

/// an enum representing either an owned value or a borrowed value. Functionally
/// equivalent to `std::borrow::Cow` except this type doesn't require the
/// wrapped type to implement `Clone`.
pub enum SerializableValue<'a, T: Serialize> {
    /// an owned value
    Owned(T),
    /// a borrowed value
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

/// a structure representing a document's entry in a View's mappings
#[derive(PartialEq, Debug)]
pub struct Map<K: Serialize = (), V: Serialize = ()> {
    /// the id of the document that emitted this entry
    pub source: Uuid,

    /// the key used to index the View
    pub key: K,

    /// an associated value stored in the view
    pub value: V,
}

/// a structure representing a document's entry in a View's mappings, serialized and ready to store
pub(crate) struct SerializedMap {
    /// the id of the document that emitted this entry
    pub source: Uuid,

    /// the key used to index the View
    // TODO change this to bytes
    pub key: serde_cbor::Value,

    /// an associated value stored in the view
    pub value: serde_cbor::Value,
}

pub(crate) trait Serialized<C> {
    fn name() -> Cow<'static, str>;
    fn map(document: &Document<'_, C>) -> Result<Option<SerializedMap>, Error>;
}

impl<C, T> Serialized<C> for T
where
    T: View<C>,
{
    fn name() -> Cow<'static, str> {
        Self::name()
    }

    fn map(document: &Document<'_, C>) -> Result<Option<SerializedMap>, Error> {
        let map = Self::map(document)?;

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
