use std::{borrow::Cow, fmt::Debug};

use serde::{Deserialize, Serialize};

use crate::{document::Document, schema::Collection};

/// Types for defining a `Map` within a `View`.
pub mod map;
pub use map::{Key, Map};

use self::map::MappedValue;
use super::collection;

/// Errors that arise when interacting with views.
#[derive(thiserror::Error, Debug)]
// TODO add which view name and collection
pub enum Error {
    /// An error occurred while serializing or deserializing.
    #[error("error deserializing document {0}")]
    Serialization(#[from] serde_cbor::Error),

    /// An error occurred while serializing or deserializing keys emitted in a view.
    #[error("error serializing view keys {0}")]
    KeySerialization(anyhow::Error),

    /// Returned when the reduce() function is unimplemented.
    #[error("reduce is unimplemented")]
    ReduceUnimplemented,
}

/// A type alias for the result of `View::map()`.
pub type MapResult<K = (), V = ()> = Result<Option<Map<K, V>>, Error>;

/// A map/reduce powered indexing and aggregation schema.
///
/// Inspired by [`CouchDB`'s view
/// system](https://docs.couchdb.org/en/stable/ddocs/views/index.html)
///
/// This implementation is under active development, our own docs explaining our
/// implementation will be written as things are solidified.
// TODO write our own view docs
pub trait View: Send + Sync + Debug + 'static {
    /// The collection this view belongs to
    type Collection: Collection;

    /// The key for this view.
    type Key: Key + 'static;

    /// An associated type that can be stored with each entry in the view.
    type Value: Serialize + for<'de> Deserialize<'de> + Send + Sync;

    /// The version of the view. Changing this value will cause indexes to be rebuilt.
    fn version(&self) -> u64;

    /// The name of the view. Must be unique per collection.
    fn name(&self) -> Cow<'static, str>;

    /// The map function for this view. This function is responsible for
    /// emitting entries for any documents that should be contained in this
    /// View. If None is returned, the View will not include the document.
    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value>;

    /// The reduce function for this view. If `Err(Error::ReduceUnimplemented)`
    /// is returned, queries that ask for a reduce operation will return an
    /// error. See [`CouchDB`'s Reduce/Rereduce
    /// documentation](https://docs.couchdb.org/en/stable/ddocs/views/intro.html#reduce-rereduce)
    /// for the design this implementation will be inspired by
    #[allow(unused_variables)]
    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        rereduce: bool,
    ) -> Result<Self::Value, Error> {
        Err(Error::ReduceUnimplemented)
    }
}

/// Represents either an owned value or a borrowed value. Functionally
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

/// Wraps a [`View`] with serialization to erase the associated types
pub trait Serialized: Send + Sync + Debug {
    /// Wraps returing [`<View::Collection as Collection>::collection_id()`](crate::schema::Collection::collection_id)
    fn collection(&self) -> collection::Id;
    /// Wraps [`View::version`]
    fn version(&self) -> u64;
    /// Wraps [`View::name`]
    fn name(&self) -> Cow<'static, str>;
    /// Wraps [`View::map`]
    fn map(&self, document: &Document<'_>) -> Result<Option<map::Serialized>, Error>;
    /// Wraps [`View::reduce`]
    fn reduce(&self, mappings: &[(&[u8], &[u8])], rereduce: bool) -> Result<Vec<u8>, Error>;
}

#[allow(clippy::use_self)] // Using Self here instead of T inside of reduce() breaks compilation. The alternative is much more verbose and harder to read.
impl<T> Serialized for T
where
    T: View,
    <T as View>::Key: 'static,
{
    fn collection(&self) -> collection::Id {
        <<Self as View>::Collection as Collection>::collection_id()
    }

    fn version(&self) -> u64 {
        self.version()
    }

    fn name(&self) -> Cow<'static, str> {
        self.name()
    }

    fn map(&self, document: &Document<'_>) -> Result<Option<map::Serialized>, Error> {
        let map = self.map(document)?;

        match map {
            Some(map) => Ok(Some(map::Serialized {
                source: map.source,
                key: map
                    .key
                    .as_big_endian_bytes()
                    .map_err(Error::KeySerialization)?
                    .to_vec(),
                value: serde_cbor::to_vec(&map.value)?,
            })),
            None => Ok(None),
        }
    }

    fn reduce(&self, mappings: &[(&[u8], &[u8])], rereduce: bool) -> Result<Vec<u8>, Error> {
        let mappings = mappings
            .iter()
            .map(
                |(key, value)| match <T::Key as Key>::from_big_endian_bytes(key) {
                    Ok(key) => match serde_cbor::from_slice::<T::Value>(value) {
                        Ok(value) => Ok(MappedValue { key, value }),
                        Err(err) => Err(Error::from(err)),
                    },
                    Err(err) => Err(Error::KeySerialization(err)),
                },
            )
            .collect::<Result<Vec<_>, Error>>()?;

        let reduced_value = match self.reduce(&mappings, rereduce) {
            Ok(value) => value,
            Err(Error::ReduceUnimplemented) => return Ok(Vec::new()),
            Err(other) => return Err(other),
        };

        serde_cbor::to_vec(&reduced_value).map_err(Error::from)
    }
}
