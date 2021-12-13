use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{
    document::Document,
    schema::{
        view::map::{MappedValue, Mappings},
        Collection, CollectionDocument, CollectionName, InvalidNameError, Name, ViewName,
    },
    AnyError,
};

/// Types for defining a `Map` within a `View`.
pub mod map;

mod key;
pub use self::key::*;

/// Errors that arise when interacting with views.
#[derive(thiserror::Error, Debug)]
// TODO add which view name and collection
pub enum Error {
    /// An error occurred while serializing or deserializing keys emitted in a view.
    #[error("error serializing view keys {0}")]
    KeySerialization(Box<dyn AnyError>),

    /// Returned when the reduce() function is unimplemented.
    #[error("reduce is unimplemented")]
    ReduceUnimplemented,

    /// Range queries are not supported on collections with encryptable keys.
    #[error("range queries are not supported on collections with encryptable keys")]
    RangeQueryNotSupported,

    /// An error unrelated to views.
    #[error("core error: {0}")]
    Core(#[from] crate::Error),
}

impl Error {
    /// Returns a [`Self::KeySerialization`] instance after boxing the error.
    pub fn key_serialization<E: AnyError>(error: E) -> Self {
        Self::KeySerialization(Box::new(error))
    }
}

impl From<pot::Error> for Error {
    fn from(err: pot::Error) -> Self {
        Self::Core(crate::Error::from(err))
    }
}

#[cfg(feature = "json")]
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Core(crate::Error::from(err))
    }
}

#[cfg(feature = "cbor")]
impl From<serde_cbor::Error> for Error {
    fn from(err: serde_cbor::Error) -> Self {
        Self::Core(crate::Error::from(err))
    }
}

#[cfg(feature = "bincode")]
impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::Core(crate::Error::from(err))
    }
}

/// A type alias for the result of `View::map()`.
pub type MapResult<K = (), V = ()> = Result<Mappings<K, V>, Error>;

/// A map/reduce powered indexing and aggregation schema.
///
/// Inspired by [`CouchDB`'s view
/// system](https://docs.couchdb.org/en/stable/ddocs/views/index.html)
///
/// This implementation is under active development, our own docs explaining our
/// implementation will be written as things are solidified. The guide [has an
/// overview](https://dev.bonsaidb.io/guide/about/concepts/view.html).
// TODO write our own view docs
pub trait View: Send + Sync + Debug + 'static {
    /// The collection this view belongs to
    type Collection: Collection;

    /// The key for this view.
    type Key: Key + 'static;

    /// An associated type that can be stored with each entry in the view.
    type Value: Serialize + for<'de> Deserialize<'de> + Send + Sync;

    /// If true, no two documents may emit the same key. Unique views are
    /// updated when the document is saved, allowing for this check to be done
    /// atomically. When a document is updated, all unique views will be
    /// updated, and if any of them fail, the document will not be allowed to
    /// update and an
    /// [`Error::UniqueKeyViolation`](crate::Error::UniqueKeyViolation) will be
    /// returned.
    fn unique(&self) -> bool {
        false
    }

    /// The version of the view. Changing this value will cause indexes to be rebuilt.
    fn version(&self) -> u64;

    /// The name of the view. Must be unique per collection.
    fn name(&self) -> Result<Name, InvalidNameError>;

    /// The namespaced name of the view.
    fn view_name(&self) -> Result<ViewName, InvalidNameError> {
        Ok(ViewName {
            collection: Self::Collection::collection_name()?,
            name: self.name()?,
        })
    }

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

/// A [`View`] for a [`Collection`] that stores serde-compatible documents. The
/// only difference between implmementing this and [`View`] is that the `map`
/// function receives a [`CollectionDocument`] instead of a [`Document`].
pub trait CollectionView: Send + Sync + Debug + 'static {
    /// The collection this view belongs to
    type Collection: Collection + Serialize + for<'de> Deserialize<'de>;

    /// The key for this view.
    type Key: Key + 'static;

    /// An associated type that can be stored with each entry in the view.
    type Value: Serialize + for<'de> Deserialize<'de> + Send + Sync;

    /// If true, no two documents may emit the same key. Unique views are
    /// updated when the document is saved, allowing for this check to be done
    /// atomically. When a document is updated, all unique views will be
    /// updated, and if any of them fail, the document will not be allowed to
    /// update and an
    /// [`Error::UniqueKeyViolation`](crate::Error::UniqueKeyViolation) will be
    /// returned.
    fn unique(&self) -> bool {
        false
    }

    /// The version of the view. Changing this value will cause indexes to be rebuilt.
    fn version(&self) -> u64;

    /// The name of the view. Must be unique per collection.
    fn name(&self) -> Result<Name, InvalidNameError>;

    /// The namespaced name of the view.
    fn view_name(&self) -> Result<ViewName, InvalidNameError> {
        Ok(ViewName {
            collection: Self::Collection::collection_name()?,
            name: self.name()?,
        })
    }

    /// The map function for this view. This function is responsible for
    /// emitting entries for any documents that should be contained in this
    /// View. If None is returned, the View will not include the document.
    fn map(
        &self,
        document: CollectionDocument<Self::Collection>,
    ) -> MapResult<Self::Key, Self::Value>;

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

impl<T> View for T
where
    T: CollectionView,
{
    type Collection = T::Collection;
    type Key = T::Key;
    type Value = T::Value;

    fn version(&self) -> u64 {
        T::version(self)
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        T::name(self)
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        T::map(self, CollectionDocument::try_from(document)?)
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        rereduce: bool,
    ) -> Result<Self::Value, Error> {
        T::reduce(self, mappings, rereduce)
    }

    fn unique(&self) -> bool {
        T::unique(self)
    }

    fn view_name(&self) -> Result<ViewName, InvalidNameError> {
        T::view_name(self)
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
    /// Wraps returing [`<View::Collection as Collection>::collection_name()`](crate::schema::Collection::collection_name)
    fn collection(&self) -> Result<CollectionName, InvalidNameError>;
    /// Wraps [`View::unique`]
    fn unique(&self) -> bool;
    /// Wraps [`View::version`]
    fn version(&self) -> u64;
    /// Wraps [`View::view_name`]
    fn view_name(&self) -> Result<ViewName, InvalidNameError>;
    /// Wraps [`View::map`]
    fn map(&self, document: &Document<'_>) -> Result<Vec<map::Serialized>, Error>;
    /// Wraps [`View::reduce`]
    fn reduce(&self, mappings: &[(&[u8], &[u8])], rereduce: bool) -> Result<Vec<u8>, Error>;
}

#[allow(clippy::use_self)] // Using Self here instead of T inside of reduce() breaks compilation. The alternative is much more verbose and harder to read.
impl<T> Serialized for T
where
    T: View,
    <T as View>::Key: 'static,
{
    fn collection(&self) -> Result<CollectionName, InvalidNameError> {
        <<Self as View>::Collection as Collection>::collection_name()
    }

    fn unique(&self) -> bool {
        self.unique()
    }

    fn version(&self) -> u64 {
        self.version()
    }

    fn view_name(&self) -> Result<ViewName, InvalidNameError> {
        self.view_name()
    }

    fn map(&self, document: &Document<'_>) -> Result<Vec<map::Serialized>, Error> {
        let map = self.map(document)?;

        map.into_iter()
            .map(|map| map.serialized())
            .collect::<Result<Vec<_>, Error>>()
    }

    fn reduce(&self, mappings: &[(&[u8], &[u8])], rereduce: bool) -> Result<Vec<u8>, Error> {
        let mappings = mappings
            .iter()
            .map(
                |(key, value)| match <T::Key as Key>::from_big_endian_bytes(key) {
                    Ok(key) => match pot::from_slice::<T::Value>(value) {
                        Ok(value) => Ok(MappedValue { key, value }),
                        Err(err) => Err(Error::from(err)),
                    },
                    Err(err) => Err(Error::key_serialization(err)),
                },
            )
            .collect::<Result<Vec<_>, Error>>()?;

        let reduced_value = match self.reduce(&mappings, rereduce) {
            Ok(value) => value,
            Err(Error::ReduceUnimplemented) => return Ok(Vec::new()),
            Err(other) => return Err(other),
        };

        pot::to_vec(&reduced_value).map_err(Error::from)
    }
}

/// Defines an unique view named `$view_name` for `$collection` with the
/// mapping provided.
#[macro_export(local_inner_macros)]
macro_rules! define_basic_unique_mapped_view {
    ($view_name:ident, $collection:ty, $version:literal, $name:literal, $key:ty, $mapping:expr $(,)?) => {
        define_mapped_view!(
            $view_name,
            $collection,
            $version,
            $name,
            $key,
            (),
            true,
            $mapping
        );
    };
    ($view_name:ident, $collection:ty, $version:literal, $name:literal, $key:ty, $value:ty, $mapping:expr $(,)?) => {
        define_mapped_view!(
            $view_name,
            $collection,
            $version,
            $name,
            $key,
            $value,
            true,
            $mapping
        );
    };
}

/// Defines a non-unique view named `$view_name` for `$collection` with the
/// mapping provided.
#[macro_export(local_inner_macros)]
macro_rules! define_basic_mapped_view {
    ($view_name:ident, $collection:ty, $version:literal, $name:literal, $key:ty, $mapping:expr $(,)?) => {
        define_mapped_view!(
            $view_name,
            $collection,
            $version,
            $name,
            $key,
            (),
            false,
            $mapping
        );
    };
    ($view_name:ident, $collection:ty, $version:literal, $name:literal, $key:ty, $value:ty, $mapping:expr $(,)?) => {
        define_mapped_view!(
            $view_name,
            $collection,
            $version,
            $name,
            $key,
            $value,
            false,
            $mapping
        );
    };
}

/// Defines a view using the mapping provided.
#[macro_export]
macro_rules! define_mapped_view {
    ($view_name:ident, $collection:ty, $version:literal, $name:literal, $key:ty, $value:ty, $unique:literal, $mapping:expr) => {
        #[derive(Debug)]
        pub struct $view_name;

        impl $crate::schema::CollectionView for $view_name {
            type Collection = $collection;
            type Key = $key;
            type Value = $value;

            fn unique(&self) -> bool {
                $unique
            }

            fn version(&self) -> u64 {
                $version
            }

            fn name(&self) -> Result<$crate::schema::Name, $crate::schema::InvalidNameError> {
                $crate::schema::Name::new($name)
            }

            fn map(
                &self,
                document: $crate::schema::CollectionDocument<Self::Collection>,
            ) -> $crate::schema::MapResult<Self::Key, Self::Value> {
                Ok($mapping(document))
            }
        }
    };
}
