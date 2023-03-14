use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Serialize;
use transmog::{Format, OwnedDeserializer};
use transmog_pot::Pot;

use crate::connection::{self, AsyncConnection, Connection};
use crate::document::{BorrowedDocument, CollectionDocument};
use crate::key::{ByteSource, Key, KeyDescription};
use crate::schema::view::map::{Mappings, ViewMappedValue};
use crate::schema::{Collection, CollectionName, Name, SerializedCollection, ViewName};
use crate::AnyError;

/// Types for defining a `Map` within a `View`.
pub mod map;

/// Errors that arise when interacting with views.
#[derive(thiserror::Error, Debug)]
// TODO add which view name and collection
pub enum Error {
    /// An error occurred while serializing or deserializing keys emitted in a view.
    #[error("error serializing view keys {0}")]
    KeySerialization(Box<dyn AnyError>),

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

/// A type alias for the result of `ViewSchema::map()`.
pub type ViewMapResult<V> = Result<Mappings<<V as View>::Key, <V as View>::Value>, crate::Error>;

/// A type alias for the result of `ViewSchema::reduce()`.
pub type ReduceResult<V> = Result<<V as View>::Value, crate::Error>;

/// An lazy index of mapped and/or reduced data from a [`Collection`].
///
/// A view provides an efficient way to query data within a collection. BonsaiDb
/// indexes the associated [`View::Collection`] by calling
/// [`CollectionViewSchema::map()`]/[`ViewSchema::map()`] every time a document
/// is created or updated. The result [`Mappings`] form a sorted index that can
/// be efficiently queried using the [`View::Key`] type.
///
/// A View behaves similarly to the standard library's BTreeMap with these
/// types: `BTreeMap<View::Key, Vec<(Header, View::Value)>>`
///
/// For a deeper dive on Views, see [the section in our user's
/// guide](https://dev.bonsaidb.io/main/guide/about/concepts/view.html).
#[doc = "\n"]
#[doc = include_str!("./view-overview.md")]
pub trait View: Sized + Send + Sync + Debug + 'static {
    /// The collection this view belongs to
    type Collection: Collection;
    /// The key for this view.
    type Key: for<'k> Key<'k> + PartialEq + 'static;
    /// An associated type that can be stored with each entry in the view.
    type Value: Send + Sync;

    /// The name of the view. Must be unique per collection.
    fn name(&self) -> Name;

    /// The namespaced name of the view.
    fn view_name(&self) -> ViewName {
        ViewName {
            collection: Self::Collection::collection_name(),
            name: self.name(),
        }
    }
}

/// The implementation of Map/Reduce for a [`View`].
#[doc = "\n"]
#[doc = include_str!("./view-overview.md")]
pub trait ViewSchema: Send + Sync + Debug + 'static {
    /// The view this schema is defined for.
    type View: SerializedView;

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

    /// Returns whether this view should be lazily updated. If true, views will
    /// be updated only when accessed. If false, views will be updated during
    /// the transaction that is updating the affected documents.
    fn lazy(&self) -> bool {
        true
    }

    /// The version of the view. Changing this value will cause indexes to be rebuilt.
    fn version(&self) -> u64 {
        0
    }

    /// The map function for this view. This function is responsible for
    /// emitting entries for any documents that should be contained in this
    /// View. If None is returned, the View will not include the document. See [the user guide's chapter on
    /// views for more information on how map
    /// works](https://dev.bonsaidb.io/main/guide/about/concepts/view.html#map).
    fn map(&self, document: &BorrowedDocument<'_>) -> ViewMapResult<Self::View>;

    /// Returns a value that is produced by reducing a list of `mappings` into a
    /// single value. If `rereduce` is true, the values contained in the
    /// mappings have already been reduced at least one time. If an error of
    /// [`ReduceUnimplemented`](crate::Error::ReduceUnimplemented) is returned,
    /// queries that ask for a reduce operation will return an error. See [the
    /// user guide's chapter on views for more information on how reduce
    /// works](https://dev.bonsaidb.io/main/guide/about/concepts/view.html#reduce).
    #[allow(unused_variables)]
    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        rereduce: bool,
    ) -> Result<<Self::View as View>::Value, crate::Error> {
        Err(crate::Error::ReduceUnimplemented)
    }
}

/// A [`View`] with additional tyes and logic to handle serializing view values.
pub trait SerializedView: View {
    /// The serialization format for this view.
    type Format: OwnedDeserializer<Self::Value>;

    /// Returns the configured instance of [`Self::Format`].
    // TODO allow configuration to be passed here, such as max allocation bytes.
    fn format() -> Self::Format;

    /// Deserialize `data` as `Self::Value` using this views's format.
    fn deserialize(data: &[u8]) -> Result<Self::Value, crate::Error> {
        Self::format()
            .deserialize_owned(data)
            .map_err(|err| crate::Error::other("serialization", err))
    }

    /// Serialize `item` using this views's format.
    fn serialize(item: &Self::Value) -> Result<Vec<u8>, crate::Error> {
        Self::format()
            .serialize(item)
            .map_err(|err| crate::Error::other("serialization", err.to_string()))
    }

    /// Returns a builder for a view query or view reduce.
    fn entries<Database: Connection>(
        database: &Database,
    ) -> connection::View<'_, Database, Self, Self::Key> {
        database.view::<Self>()
    }

    /// Returns a builder for a view query or view reduce.
    fn entries_async<Database: AsyncConnection>(
        database: &Database,
    ) -> connection::AsyncView<'_, Database, Self, Self::Key> {
        database.view::<Self>()
    }
}

/// A default serialization strategy for views. Uses equivalent settings as
/// [`DefaultSerialization`](crate::schema::DefaultSerialization).
pub trait DefaultViewSerialization: View {}

impl<T> SerializedView for T
where
    T: DefaultViewSerialization,
    T::Value: Serialize + DeserializeOwned,
{
    type Format = Pot;

    fn format() -> Self::Format {
        Pot::default()
    }
}

/// A [`View`] for a [`Collection`] that stores Serde-compatible documents. The
/// only difference between implmementing this and [`View`] is that the `map`
/// function receives a [`CollectionDocument`] instead of a [`BorrowedDocument`].
pub trait CollectionViewSchema: Send + Sync + Debug + 'static
where
    <Self::View as View>::Collection: SerializedCollection,
{
    /// The view this schema is an implementation of.
    type View: SerializedView;

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

    /// Returns whether this view should be lazily updated. If true, views will
    /// be updated only when accessed. If false, views will be updated during
    /// the transaction that is updating the affected documents.
    fn lazy(&self) -> bool {
        true
    }

    /// The version of the view. Changing this value will cause indexes to be rebuilt.
    fn version(&self) -> u64 {
        0
    }

    /// The map function for this view. This function is responsible for
    /// emitting entries for any documents that should be contained in this
    /// View. If None is returned, the View will not include the document.
    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View>;

    /// The reduce function for this view. If `Err(Error::ReduceUnimplemented)`
    /// is returned, queries that ask for a reduce operation will return an
    /// error. See [`CouchDB`'s Reduce/Rereduce
    /// documentation](https://docs.couchdb.org/en/stable/ddocs/views/intro.html#reduce-rereduce)
    /// for the design this implementation will be inspired by
    #[allow(unused_variables)]
    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Err(crate::Error::ReduceUnimplemented)
    }
}

impl<T> ViewSchema for T
where
    T: CollectionViewSchema,
    T::View: SerializedView,
    <T::View as View>::Collection: SerializedCollection,
{
    type View = T::View;

    fn version(&self) -> u64 {
        T::version(self)
    }

    fn map(&self, document: &BorrowedDocument<'_>) -> ViewMapResult<Self::View> {
        T::map(self, CollectionDocument::try_from(document)?)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        rereduce: bool,
    ) -> Result<<Self::View as View>::Value, crate::Error> {
        T::reduce(self, mappings, rereduce)
    }

    fn unique(&self) -> bool {
        T::unique(self)
    }

    fn lazy(&self) -> bool {
        T::lazy(self)
    }
}

/// Wraps a [`View`] with serialization to erase the associated types
pub trait Serialized: Send + Sync + Debug {
    /// Wraps returing [`<View::Collection as Collection>::collection_name()`](crate::schema::Collection::collection_name)
    fn collection(&self) -> CollectionName;
    /// Returns the description of the view's `Key`.
    fn key_description(&self) -> KeyDescription;
    /// Wraps [`ViewSchema::unique`]
    fn unique(&self) -> bool;
    /// Wraps [`ViewSchema::lazy`]
    fn lazy(&self) -> bool;

    /// Returns true if this view should be eagerly updated during document
    /// updates.
    fn eager(&self) -> bool {
        self.unique() || !self.lazy()
    }

    /// Wraps [`ViewSchema::version`]
    fn version(&self) -> u64;
    /// Wraps [`View::view_name`]
    fn view_name(&self) -> ViewName;
    /// Wraps [`ViewSchema::map`]
    fn map(&self, document: &BorrowedDocument<'_>) -> Result<Vec<map::Serialized>, Error>;
    /// Wraps [`ViewSchema::reduce`]
    fn reduce(&self, mappings: &[(&[u8], &[u8])], rereduce: bool) -> Result<Vec<u8>, Error>;
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
        #[derive(Debug, Clone)]
        pub struct $view_name;

        impl $crate::schema::View for $view_name {
            type Collection = $collection;
            type Key = $key;
            type Value = $value;

            fn name(&self) -> $crate::schema::Name {
                $crate::schema::Name::new($name)
            }
        }

        impl $crate::schema::CollectionViewSchema for $view_name {
            type View = Self;

            fn unique(&self) -> bool {
                $unique
            }

            fn version(&self) -> u64 {
                $version
            }

            fn map(
                &self,
                document: $crate::document::CollectionDocument<$collection>,
            ) -> $crate::schema::ViewMapResult<Self::View> {
                $mapping(document)
            }
        }

        impl $crate::schema::view::DefaultViewSerialization for $view_name {}
    };
}
