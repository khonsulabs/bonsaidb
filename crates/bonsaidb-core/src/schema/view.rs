use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use transmog::{Format, OwnedDeserializer};
use transmog_pot::Pot;

use crate::connection::{self, AsyncConnection, Connection};
use crate::document::{BorrowedDocument, CollectionDocument};
use crate::key::{ByteSource, Key, KeyDescription};
use crate::schema::view::map::{MappedValue, Mappings, ViewMappedValue};
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
pub type ViewMapResult<'doc, V> = Result<
    Mappings<<V as ViewSchema>::MappedKey<'doc>, <<V as ViewSchema>::View as View>::Value>,
    crate::Error,
>;

/// A type alias for the result of `ViewSchema::reduce()`.
pub type ReduceResult<V> = Result<<V as View>::Value, crate::Error>;

/// An lazy index of mapped and/or reduced data from a [`Collection`].
///
/// A view provides an efficient way to query data within a collection. BonsaiDb
/// indexes the associated [`View::Collection`] by calling
/// [`CollectionMapReduce::map()`]/[`MapReduce::map()`] every time a document is
/// created or updated. The result [`Mappings`] form a sorted index that can be
/// efficiently queried using the [`View::Key`] type.
///
/// A View behaves similarly to the standard library's BTreeMap with these
/// types: `BTreeMap<View::Key, Vec<(Header, View::Value)>>`
///
/// This trait only defines the types and functionality required to interact
/// with the view's query system. The [`MapReduce`]/[`CollectionMapReduce`]
/// traits define the map/reduce behavior, and the [`ViewSchema`] trait defines
/// additional metadata such as the [`ViewUpdatePolicy`] and view version.
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

/// Schema information for a [`View`].
///
/// This trait controls several behaviors for a view:
///
/// - [`MappedKey<'doc>`](Self::MappedKey): A [`Key`] type that is compatible
///   with the associated [`View::Key`] type, but can borrow from the document
///   by using the `'doc` generic associated lifetime.
/// - [`policy()`](Self::policy): Controls when the view's data is updated and
///   any restrictions on the data contained in the view. The default policy for
///   views is [`ViewUpdatePolicy`]
/// - [`version()`](Self::version): An integer representing the view's version.
///   Changing this number will cause the view to be re-indexed. This is useful
///   when there are fundamental changes in how the view is implemented.
///
/// ## Where is this trait used?
///
/// This trait is currently only used by `bonsaidb-local`, but is provided in
/// `bonsaidb-core` to allow it to be used anywhere. It may be desireable to
/// keep the implementation of `ViewSchema` in the "server" binary, while only
/// exposing the `View` implementation to the "client".
///
/// ## Deriving this Trait
///
/// This trait can be derived, and all attributes are optional. The available
/// options are:
///
/// - `view`: Sets the associated [`View`](Self::View) type. If not provided,
///   `Self` will be used.
/// - `version`: Sets the [version number](Self::version) of the view. If not
///   provided, `0` will be used.
/// - `mapped_key`: Sets the [`MappedKey<'doc>`](Self::MappedKey) type to the
///   provided type. The `'doc` lifetime can be utilized to borrow data during
///   the [`MapReduce::map()`] function.
///
///   If not provided, the `ViewSchema` implementation uses [`View::Key`].
/// - `policy`: Sets the [`ViewUpdatePolicy`]. The accepted policies are:
///   - [`Lazy`](ViewUpdatePolicy::Lazy)
///   - [`Eager`](ViewUpdatePolicy::Eager)
///   - [`Unique`](ViewUpdatePolicy::Unique)
///
///   If not provided, the [`Lazy`](ViewUpdatePolicy::Lazy) policy will be used.
///
/// Here is an example that showcases most of the options:
/// ```rust
/// # mod collection {
/// # bonsaidb_core::__doctest_prelude!();
/// # }
/// # use collection::MyCollection;
/// use std::borrow::Cow;
///
/// use bonsaidb_core::document::{BorrowedDocument, Emit};
/// use bonsaidb_core::schema::view::{ReduceResult, ViewMapResult};
/// use bonsaidb_core::schema::{MapReduce, View, ViewMappedValue, ViewSchema};
///
/// #[derive(View, ViewSchema, Debug)]
/// #[view(collection = MyCollection, key = String, value = u32)]
/// #[view_schema(mapped_key = Cow<'doc, str>, policy = Unique)]
/// # #[view(core = bonsaidb_core)]
/// struct UniqueByName;
///
/// impl MapReduce for UniqueByName {
///     fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
///         let contents_as_str = std::str::from_utf8(&document.contents).expect("invalid utf-8");
///         document
///             .header
///             .emit_key_and_value(Cow::Borrowed(contents_as_str), 1)
///     }
///
///     fn reduce(
///         &self,
///         mappings: &[ViewMappedValue<'_, Self::View>],
///         _rereduce: bool,
///     ) -> ReduceResult<Self::View> {
///         Ok(mappings.iter().map(|mapping| mapping.value).sum())
///     }
/// }
/// ```
#[doc = "\n"]
#[doc = include_str!("./view-overview.md")]
pub trait ViewSchema: Send + Sync + Debug + 'static {
    /// The view this schema is defined for.
    type View: SerializedView;
    /// The key type used during the map/reduce operation.
    ///
    /// This can typically be specified as `<Self::View as View>::Key`. However,
    /// if the view can take advantage of utilizing borrowed data from the
    /// document in the `map()` and/or `reduce()` function calls, this type can
    /// utilize the generic associated lifetime `'doc`. For example, `Cow<'doc,
    /// str>` can be used when the related [`View::Key`] type is `String`, and
    /// the `map()` function would be able to return a string slice that
    /// borrowed from the document.
    type MappedKey<'doc>: Key<'doc>;

    /// Returns the update policy for this view, which controls when and how the
    /// view's data is updated. The provided implementation returns
    /// [`ViewUpdatePolicy::Lazy`].
    fn update_policy(&self) -> ViewUpdatePolicy {
        ViewUpdatePolicy::default()
    }

    /// The version of the view. Changing this value will cause indexes to be
    /// rebuilt.
    fn version(&self) -> u64 {
        0
    }
}

/// The policy under which a [`View`] is updated when documents are saved.
#[derive(Default, Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ViewUpdatePolicy {
    /// The view is updated when a query is made. If a document is updated
    /// multiple times between queries, the view will only be updated when the
    /// query is executed.
    #[default]
    Lazy,
    /// The view is updated during the transaction where documents are being
    /// inserted, updated, or removed.
    Eager,
    /// No two documents may emit the same key. Unique views are updated when
    /// the document is saved, allowing for this check to be done atomically.
    /// When a document is updated, all unique views will be updated, and if any
    /// of them fail, the document will not be allowed to update and an
    /// [`Error::UniqueKeyViolation`](crate::Error::UniqueKeyViolation) will be
    /// returned.
    Unique,
}

impl ViewUpdatePolicy {
    /// Returns true if the view should be updated eagerly.
    ///
    /// This returns true if the policy is either [`Eager`](Self::Eager) or
    /// [`Unique`](Self::Unique).
    #[must_use]
    pub const fn is_eager(&self) -> bool {
        matches!(self, Self::Eager | Self::Unique)
    }
}

impl std::fmt::Display for ViewUpdatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

/// The Map/Reduce functionality for a [`ViewSchema`].
///
/// This trait implementation provides the behavior for mapping data from
/// documents into their key/value pairs for this view, as well as reducing
/// multiple values into a single value.
pub trait MapReduce: ViewSchema {
    /// The map function for this view. This function is responsible for
    /// emitting entries for any documents that should be contained in this
    /// View. If None is returned, the View will not include the document. See [the user guide's chapter on
    /// views for more information on how map
    /// works](https://dev.bonsaidb.io/main/guide/about/concepts/view.html#map).
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self>;

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
        mappings: &[MappedValue<Self::MappedKey<'_>, <Self::View as View>::Value>],
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

/// A [`MapReduce`] implementation that automatically serializes/deserializes
/// using [`CollectionDocument`] and [`SerializedCollection`].
///
/// Implementing this trait automatically implements [`ViewSchema`] for the same
/// type.
pub trait CollectionMapReduce: ViewSchema
where
    <Self::View as View>::Collection: SerializedCollection,
{
    /// The map function for this view. This function is responsible for
    /// emitting entries for any documents that should be contained in this
    /// View. If None is returned, the View will not include the document.
    fn map<'doc>(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<'doc, Self>
    where
        CollectionDocument<<Self::View as View>::Collection>: 'doc;

    /// The reduce function for this view. If `Err(Error::ReduceUnimplemented)`
    /// is returned, queries that ask for a reduce operation will return an
    /// error. See [`CouchDB`'s Reduce/Rereduce
    /// documentation](https://docs.couchdb.org/en/stable/ddocs/views/intro.html#reduce-rereduce)
    /// for the design this implementation will be inspired by
    #[allow(unused_variables)]
    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Err(crate::Error::ReduceUnimplemented)
    }
}

impl<T> MapReduce for T
where
    T: CollectionMapReduce,
    T::View: SerializedView,
    <T::View as View>::Collection: SerializedCollection,
{
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        T::map(self, CollectionDocument::try_from(document)?)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        rereduce: bool,
    ) -> Result<<Self::View as View>::Value, crate::Error> {
        T::reduce(self, mappings, rereduce)
    }
}

/// Wraps a [`View`] with serialization to erase the associated types
pub trait Serialized: Send + Sync + Debug {
    /// Wraps returing [`<View::Collection as Collection>::collection_name()`](crate::schema::Collection::collection_name)
    fn collection(&self) -> CollectionName;
    /// Returns the description of the view's `Key`.
    fn key_description(&self) -> KeyDescription;
    /// Wraps [`ViewSchema::policy`]
    fn update_policy(&self) -> ViewUpdatePolicy;

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

        impl $crate::schema::ViewSchema for $view_name {
            type MappedKey<'doc> = <Self as $crate::schema::View>::Key;
            type View = Self;

            fn update_policy(&self) -> $crate::schema::view::ViewUpdatePolicy {
                if $unique {
                    $crate::schema::view::ViewUpdatePolicy::Unique
                } else {
                    $crate::schema::view::ViewUpdatePolicy::Lazy
                }
            }

            fn version(&self) -> u64 {
                $version
            }
        }

        impl $crate::schema::CollectionMapReduce for $view_name {
            fn map<'doc>(
                &self,
                document: $crate::document::CollectionDocument<$collection>,
            ) -> $crate::schema::ViewMapResult<'doc, Self> {
                $mapping(document)
            }
        }

        impl $crate::schema::view::DefaultViewSerialization for $view_name {}
    };
}
