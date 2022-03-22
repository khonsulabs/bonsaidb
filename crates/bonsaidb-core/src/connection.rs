use std::{marker::PhantomData, ops::Deref, sync::Arc};

use actionable::{Action, Identifier};
use arc_bytes::serde::Bytes;
use async_trait::async_trait;
use futures::{future::BoxFuture, Future, FutureExt};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::{
    document::{
        AnyDocumentId, CollectionDocument, CollectionHeader, Document, HasHeader, Header,
        OwnedDocument,
    },
    key::{IntoPrefixRange, Key},
    permissions::Permissions,
    schema::{
        self,
        view::{self, map::MappedDocuments},
        Map, MappedValue, Nameable, NamedReference, Schema, SchemaName, SerializedCollection,
    },
    transaction::{self},
    Error,
};

mod lowlevel;

pub use lowlevel::{AsyncLowLevelConnection, LowLevelConnection};

/// A connection to a database's [`Schema`](schema::Schema), giving access to
/// [`Collection`s](crate::schema::Collection) and
/// [`Views`s](crate::schema::View). This trait is not safe to use within async
/// contexts and will block the current thread. For async access, use
/// [`AsyncConnection`].
pub trait Connection: LowLevelConnection + Sized + Send + Sync {
    /// The [`StorageConnection`] type that is paired with this type.
    type Storage: StorageConnection<Database = Self>;

    /// Returns the [`StorageConnection`] implementor that this database belongs to.
    fn storage(&self) -> Self::Storage;

    /// Returns the currently authenticated session, if any.
    fn session(&self) -> Option<&Session>;

    /// Accesses a collection for the connected [`Schema`](schema::Schema).
    fn collection<C: schema::Collection>(&self) -> Collection<'_, Self, C> {
        Collection::new(self)
    }

    /// Accesses a [`schema::View`] from this connection.
    fn view<V: schema::SerializedView>(&'_ self) -> View<'_, Self, V> {
        View::new(self)
    }

    /// Lists [executed transactions](transaction::Executed) from this
    /// [`Schema`](schema::Schema). By default, a maximum of 1000 entries will
    /// be returned, but that limit can be overridden by setting `result_limit`.
    /// A hard limit of 100,000 results will be returned. To begin listing after
    /// another known `transaction_id`, pass `transaction_id + 1` into
    /// `starting_id`.
    fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<transaction::Executed>, Error>;

    /// Fetches the last transaction id that has been committed, if any.
    fn last_transaction_id(&self) -> Result<Option<u64>, Error>;

    /// Compacts the entire database to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::Io`]: an error occurred while compacting the database.
    fn compact(&self) -> Result<(), crate::Error>;

    /// Compacts the collection to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::CollectionNotFound`]: database `name` does not exist.
    /// * [`Error::Io`]: an error occurred while compacting the database.
    fn compact_collection<C: schema::Collection>(&self) -> Result<(), crate::Error> {
        self.compact_collection_by_name(C::collection_name())
    }

    /// Compacts the key value store to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::Io`]: an error occurred while compacting the database.
    fn compact_key_value_store(&self) -> Result<(), crate::Error>;
}

/// Interacts with a collection over a `Connection`.
///
/// These examples in this type use this basic collection definition:
///
/// ```rust
/// use bonsaidb_core::{schema::Collection, Error};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, Default, Collection)]
/// #[collection(name = "MyCollection")]
/// # #[collection(core = bonsaidb_core)]
/// pub struct MyCollection {
///     pub rank: u32,
///     pub score: f32,
/// }
/// ```
pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, /* allows for extension traits to be written for collections of specific types */
}

impl<'a, Cn, Cl> Clone for Collection<'a, Cn, Cl> {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection,
            _phantom: PhantomData,
        }
    }
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection,
    Cl: schema::Collection,
{
    /// Creates a new instance using `connection`.
    fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            _phantom: PhantomData::default(),
        }
    }

    /// Adds a new `Document<Cl>` with the contents `item`.
    ///
    /// ## Automatic ID Assignment
    ///
    /// This function calls [`SerializedCollection::natural_id()`] to try to
    /// retrieve a primary key value from `item`. If an id is returned, the item
    /// is inserted with that id. If an id is not returned, an id will be
    /// automatically assigned, if possible, by the storage backend, which uses the [`Key`]
    /// trait to assign ids.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// let inserted_header = db
    ///     .collection::<MyCollection>()
    ///     .push(&MyCollection::default())?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn push(
        &self,
        item: &<Cl as SerializedCollection>::Contents,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        let contents = Cl::serialize(item)?;
        if let Some(natural_id) = Cl::natural_id(item) {
            self.insert_bytes(natural_id, contents)
        } else {
            self.push_bytes(contents)
        }
    }

    /// Adds a new `Document<Cl>` with the `contents`.
    ///
    /// ## Automatic ID Assignment
    ///
    /// An id will be automatically assigned, if possible, by the storage backend, which uses
    /// the [`Key`] trait to assign ids.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// let inserted_header = db.collection::<MyCollection>().push_bytes(vec![])?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn push_bytes<B: Into<Bytes> + Send>(
        &self,
        contents: B,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        self.connection
            .insert::<Cl, _, B>(Option::<Cl::PrimaryKey>::None, contents)
    }

    /// Adds a new `Document<Cl>` with the given `id` and contents `item`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// let inserted_header = db
    ///     .collection::<MyCollection>()
    ///     .insert(42, &MyCollection::default())?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert<PrimaryKey>(
        &self,
        id: PrimaryKey,
        item: &<Cl as SerializedCollection>::Contents,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>> + Send + Sync,
    {
        let contents = Cl::serialize(item)?;
        self.connection.insert::<Cl, _, _>(Some(id), contents)
    }

    /// Adds a new `Document<Cl>` with the the given `id` and `contents`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// let inserted_header = db.collection::<MyCollection>().insert_bytes(42, vec![])?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_bytes<B: Into<Bytes> + Send>(
        &self,
        id: Cl::PrimaryKey,
        contents: B,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        self.connection.insert::<Cl, _, B>(Some(id), contents)
    }

    /// Updates an existing document. Upon success, `doc.revision` will be
    /// updated with the new revision.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// if let Some(mut document) = db.collection::<MyCollection>().get(42)? {
    ///     // modify the document
    ///     db.collection::<MyCollection>().update(&mut document);
    ///     println!("Updated revision: {:?}", document.header.revision);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn update<D: Document<Cl> + Send + Sync>(&self, doc: &mut D) -> Result<(), Error> {
        self.connection.update::<Cl, D>(doc)
    }

    /// Overwrites an existing document, or inserts a new document. Upon success,
    /// `doc.revision` will be updated with the new revision information.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// if let Some(mut document) = db.collection::<MyCollection>().get(42)? {
    ///     // modify the document
    ///     db.collection::<MyCollection>().overwrite(&mut document);
    ///     println!("Updated revision: {:?}", document.header.revision);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn overwrite<D: Document<Cl> + Send + Sync>(&self, doc: &mut D) -> Result<(), Error> {
        let contents = doc.bytes()?;
        doc.set_collection_header(self.connection.overwrite::<Cl, _>(doc.key(), contents)?)
    }

    /// Retrieves a `Document<Cl>` with `id` from the connection.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// if let Some(doc) = db.collection::<MyCollection>().get(42)? {
    ///     println!(
    ///         "Retrieved bytes {:?} with revision {}",
    ///         doc.contents, doc.header.revision
    ///     );
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get<PrimaryKey>(&self, id: PrimaryKey) -> Result<Option<OwnedDocument>, Error>
    where
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>> + Send,
    {
        self.connection.get::<Cl, _>(id)
    }

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// for doc in db.collection::<MyCollection>().get_multiple([42, 43])? {
    ///     println!("Retrieved #{} with bytes {:?}", doc.header.id, doc.contents);
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_multiple<DocumentIds, PrimaryKey, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        DocumentIds: IntoIterator<Item = PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PrimaryKey> + Send + Sync,
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>> + Send + Sync,
    {
        self.connection.get_multiple::<Cl, _, _, _>(ids)
    }

    /// Retrieves all documents matching the range of `ids`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// for doc in db
    ///     .collection::<MyCollection>()
    ///     .list(42..)
    ///     .descending()
    ///     .limit(20)
    ///     .query()?
    /// {
    ///     println!("Retrieved #{} with bytes {:?}", doc.header.id, doc.contents);
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list<PrimaryKey, R>(&'a self, ids: R) -> List<'a, Cn, Cl>
    where
        R: Into<Range<PrimaryKey>>,
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>>,
    {
        List::new(
            PossiblyOwned::Borrowed(self),
            ids.into().map(PrimaryKey::into),
        )
    }

    /// Retrieves all documents with ids that start with `prefix`.
    ///
    /// ```rust
    /// use bonsaidb_core::{
    ///     connection::Connection,
    ///     document::OwnedDocument,
    ///     schema::{Collection, Schematic, SerializedCollection},
    ///     Error,
    /// };
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Serialize, Deserialize, Default, Collection)]
    /// #[collection(name = "MyCollection", primary_key = String)]
    /// # #[collection(core = bonsaidb_core)]
    /// pub struct MyCollection;
    ///
    /// fn starts_with_a<C: Connection>(db: &C) -> Result<Vec<OwnedDocument>, Error> {
    ///     db.collection::<MyCollection>()
    ///         .list_with_prefix(String::from("a"))
    ///         .query()
    /// }
    /// ```
    pub fn list_with_prefix(&'a self, prefix: Cl::PrimaryKey) -> List<'a, Cn, Cl>
    where
        Cl::PrimaryKey: IntoPrefixRange,
    {
        List::new(
            PossiblyOwned::Borrowed(self),
            prefix.into_prefix_range().map(AnyDocumentId::Deserialized),
        )
    }

    /// Retrieves all documents.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// for doc in db.collection::<MyCollection>().all().query()? {
    ///     println!("Retrieved #{} with bytes {:?}", doc.header.id, doc.contents);
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn all(&'a self) -> List<'a, Cn, Cl> {
        List::new(PossiblyOwned::Borrowed(self), Range::from(..))
    }

    /// Removes a `Document` from the database.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// if let Some(doc) = db.collection::<MyCollection>().get(42)? {
    ///     db.collection::<MyCollection>().delete(&doc)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete<H: HasHeader + Send + Sync>(&self, doc: &H) -> Result<(), Error> {
        self.connection.delete::<Cl, H>(doc)
    }
}

/// Retrieves a list of documents from a collection. This structure also offers
/// functions to customize the options for the operation.
#[must_use]
pub struct List<'a, Cn, Cl>
where
    Cl: schema::Collection,
{
    collection: PossiblyOwned<'a, Collection<'a, Cn, Cl>>,
    range: Range<AnyDocumentId<Cl::PrimaryKey>>,
    sort: Sort,
    limit: Option<u32>,
}

impl<'a, Cn, Cl> List<'a, Cn, Cl>
where
    Cl: schema::Collection,
    Cn: Connection,
{
    pub(crate) fn new(
        collection: PossiblyOwned<'a, Collection<'a, Cn, Cl>>,
        range: Range<AnyDocumentId<Cl::PrimaryKey>>,
    ) -> Self {
        Self {
            collection,
            range,
            sort: Sort::Ascending,
            limit: None,
        }
    }

    /// Lists documents by id in ascending order.
    pub fn ascending(mut self) -> Self {
        self.sort = Sort::Ascending;
        self
    }

    /// Lists documents by id in descending order.
    pub fn descending(mut self) -> Self {
        self.sort = Sort::Descending;
        self
    }

    /// Sets the maximum number of results to return.
    pub fn limit(mut self, maximum_results: u32) -> Self {
        self.limit = Some(maximum_results);
        self
    }

    /// Returns the number of documents contained within the range.
    ///
    /// Order and limit are ignored if they were set.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// println!(
    ///     "Number of documents with id 42 or larger: {}",
    ///     db.collection::<MyCollection>().list(42..).count()?
    /// );
    /// println!(
    ///     "Number of documents in MyCollection: {}",
    ///     db.collection::<MyCollection>().all().count()?
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn count(self) -> Result<u64, Error> {
        let Self {
            collection, range, ..
        } = self;
        collection.connection.count::<Cl, _, _>(range)
    }

    /// Returns the list of headers for documents contained within the range.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// println!(
    ///     "Headers with id 42 or larger: {:?}",
    ///     db.collection::<MyCollection>().list(42..).headers()?
    /// );
    /// println!(
    ///     "Headers in MyCollection: {:?}",
    ///     db.collection::<MyCollection>().all().headers()?
    /// );
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn headers(self) -> Result<Vec<Header>, Error> {
        let Self {
            collection,
            range,
            sort,
            limit,
            ..
        } = self;
        collection
            .connection
            .list_headers::<Cl, _, _>(range, sort, limit)
    }

    /// Retrieves the matching documents.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: &C) -> Result<(), Error> {
    /// for doc in db.collection::<MyCollection>().all().query()? {
    ///     println!("Retrieved #{} with bytes {:?}", doc.header.id, doc.contents);
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(self) -> Result<Vec<OwnedDocument>, Error> {
        let Self {
            collection,
            range,
            sort,
            limit,
        } = self;
        collection.connection.list::<Cl, _, _>(range, sort, limit)
    }
}

/// Parameters to query a [`schema::View`].
///
/// The examples for this type use this view definition:
///
/// ```rust
/// # mod collection {
/// # bonsaidb_core::__doctest_prelude!();
/// # }
/// # use collection::MyCollection;
/// use bonsaidb_core::{
///     define_basic_unique_mapped_view,
///     document::{CollectionDocument, Emit},
///     schema::{
///         CollectionViewSchema, DefaultViewSerialization, Name, ReduceResult, View,
///         ViewMapResult, ViewMappedValue,
///     },
/// };
///
/// #[derive(Debug, Clone, View)]
/// #[view(collection = MyCollection, key = u32, value = f32, name = "scores-by-rank")]
/// # #[view(core = bonsaidb_core)]
/// pub struct ScoresByRank;
///
/// impl CollectionViewSchema for ScoresByRank {
///     type View = Self;
///     fn map(
///         &self,
///         document: CollectionDocument<<Self::View as View>::Collection>,
///     ) -> ViewMapResult<Self::View> {
///         document
///             .header
///             .emit_key_and_value(document.contents.rank, document.contents.score)
///     }
///
///     fn reduce(
///         &self,
///         mappings: &[ViewMappedValue<Self::View>],
///         rereduce: bool,
///     ) -> ReduceResult<Self::View> {
///         if mappings.is_empty() {
///             Ok(0.)
///         } else {
///             Ok(mappings.iter().map(|map| map.value).sum::<f32>() / mappings.len() as f32)
///         }
///     }
/// }
/// ```
#[must_use]
pub struct View<'a, Cn, V: schema::SerializedView> {
    connection: &'a Cn,

    /// Key filtering criteria.
    pub key: Option<QueryKey<V::Key>>,

    /// The view's data access policy. The default value is [`AccessPolicy::UpdateBefore`].
    pub access_policy: AccessPolicy,

    /// The sort order of the query.
    pub sort: Sort,

    /// The maximum number of results to return.
    pub limit: Option<u32>,
}

impl<'a, Cn, V> View<'a, Cn, V>
where
    V: schema::SerializedView,
    Cn: Connection,
{
    fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            key: None,
            access_policy: AccessPolicy::UpdateBefore,
            sort: Sort::Ascending,
            limit: None,
        }
    }

    /// Filters for entries in the view with `key`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().with_key(42).query()? {
    ///     assert_eq!(mapping.key, 42);
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_key(mut self, key: V::Key) -> Self {
        self.key = Some(QueryKey::Matches(key));
        self
    }

    /// Filters for entries in the view with `keys`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().with_keys([42, 43]).query()? {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_keys<IntoIter: IntoIterator<Item = V::Key>>(mut self, keys: IntoIter) -> Self {
        self.key = Some(QueryKey::Multiple(keys.into_iter().collect()));
        self
    }

    /// Filters for entries in the view with the range `keys`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().with_key_range(42..).query()? {
    ///     assert!(mapping.key >= 42);
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_key_range<R: Into<Range<V::Key>>>(mut self, range: R) -> Self {
        self.key = Some(QueryKey::Range(range.into()));
        self
    }

    /// Filters for entries in the view with keys that begin with `prefix`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// #[derive(View, Debug, Clone)]
    /// #[view(name = "by-name", key = String, collection = MyCollection)]
    /// # #[view(core = bonsaidb_core)]
    /// struct ByName;
    ///
    /// // score is an f32 in this example
    /// for mapping in db
    ///     .view::<ByName>()
    ///     .with_key_prefix(String::from("a"))
    ///     .query()?
    /// {
    ///     assert!(mapping.key.starts_with("a"));
    ///     println!("{} in document {:?}", mapping.key, mapping.source);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_key_prefix(mut self, prefix: V::Key) -> Self
    where
        V::Key: IntoPrefixRange,
    {
        self.key = Some(QueryKey::Range(prefix.into_prefix_range()));
        self
    }

    /// Sets the access policy for queries.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db
    ///     .view::<ScoresByRank>()
    ///     .with_access_policy(AccessPolicy::UpdateAfter)
    ///     .query()?
    /// {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_access_policy(mut self, policy: AccessPolicy) -> Self {
        self.access_policy = policy;
        self
    }

    /// Queries the view in ascending order. This is the default sorting
    /// behavior.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().ascending().query()? {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn ascending(mut self) -> Self {
        self.sort = Sort::Ascending;
        self
    }

    /// Queries the view in descending order.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().descending().query()? {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn descending(mut self) -> Self {
        self.sort = Sort::Descending;
        self
    }

    /// Sets the maximum number of results to return.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// let mappings = db.view::<ScoresByRank>().limit(10).query()?;
    /// assert!(mappings.len() <= 10);
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(mut self, maximum_results: u32) -> Self {
        self.limit = Some(maximum_results);
        self
    }

    /// Executes the query and retrieves the results.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().query()? {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(self) -> Result<ViewMappings<V>, Error> {
        self.connection
            .query::<V>(self.key, self.sort, self.limit, self.access_policy)
    }

    /// Executes the query and retrieves the results with the associated [`Document`s](crate::document::OwnedDocument).
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// for mapping in &db
    ///     .view::<ScoresByRank>()
    ///     .with_key_range(42..=44)
    ///     .query_with_docs()?
    /// {
    ///     println!(
    ///         "Mapping from #{} with rank: {} and score: {}. Document bytes: {:?}",
    ///         mapping.document.header.id, mapping.key, mapping.value, mapping.document.contents
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_with_docs(self) -> Result<MappedDocuments<OwnedDocument, V>, Error> {
        self.connection
            .query_with_docs::<V>(self.key, self.sort, self.limit, self.access_policy)
    }

    /// Executes the query and retrieves the results with the associated [`CollectionDocument`s](crate::document::CollectionDocument).
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// for mapping in &db
    ///     .view::<ScoresByRank>()
    ///     .with_key_range(42..=44)
    ///     .query_with_collection_docs()?
    /// {
    ///     println!(
    ///         "Mapping from #{} with rank: {} and score: {}. Deserialized Contents: {:?}",
    ///         mapping.document.header.id, mapping.key, mapping.value, mapping.document.contents
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_with_collection_docs(
        self,
    ) -> Result<MappedDocuments<CollectionDocument<V::Collection>, V>, Error>
    where
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
    {
        self.connection.query_with_collection_docs::<V>(
            self.key,
            self.sort,
            self.limit,
            self.access_policy,
        )
    }

    /// Executes a reduce over the results of the query
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// let score = db.view::<ScoresByRank>().reduce()?;
    /// println!("Average score: {:3}", score);
    /// # Ok(())
    /// # }
    /// ```
    pub fn reduce(self) -> Result<V::Value, Error> {
        self.connection.reduce::<V>(self.key, self.access_policy)
    }

    /// Executes a reduce over the results of the query, grouping by key.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().reduce_grouped()? {
    ///     println!(
    ///         "Rank {} has an average score of {:3}",
    ///         mapping.key, mapping.value
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn reduce_grouped(self) -> Result<GroupedReductions<V>, Error> {
        self.connection
            .reduce_grouped::<V>(self.key, self.access_policy)
    }

    /// Deletes all of the associated documents that match this view query.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// db.view::<ScoresByRank>().delete_docs()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_docs(self) -> Result<u64, Error> {
        self.connection
            .delete_docs::<V>(self.key, self.access_policy)
    }
}

/// This type is the result of `query()`. It is a list of mappings, which
/// contains:
///
/// - The key emitted during the map function.
/// - The value emitted during the map function.
/// - The source document header that the mappings originated from.
pub type ViewMappings<V> = Vec<Map<<V as schema::View>::Key, <V as schema::View>::Value>>;
/// This type is the result of `reduce_grouped()`. It is a list of all matching
/// keys and the reduced value of all mapped entries for that key.
pub type GroupedReductions<V> =
    Vec<MappedValue<<V as schema::View>::Key, <V as schema::View>::Value>>;

/// A connection to a database's [`Schema`](schema::Schema), giving access to
/// [`Collection`s](crate::schema::Collection) and
/// [`Views`s](crate::schema::View). All functions on this trait are safe to use
/// in an asynchronous context.
#[async_trait]
pub trait AsyncConnection: AsyncLowLevelConnection + Sized + Send + Sync {
    /// The [`AsyncStorageConnection`] type that is paired with this type.
    type Storage: AsyncStorageConnection<Database = Self>;

    /// Returns the [`StorageConnection`] implementor that this database belongs
    /// to.
    fn storage(&self) -> Self::Storage;

    /// Accesses a collection for the connected [`Schema`](schema::Schema).
    fn collection<C: schema::Collection>(&self) -> AsyncCollection<'_, Self, C> {
        AsyncCollection::new(self)
    }

    /// Initializes [`View`] for [`schema::View`] `V`.
    fn view<V: schema::SerializedView>(&'_ self) -> AsyncView<'_, Self, V> {
        AsyncView::new(self)
    }

    /// Lists [executed transactions](transaction::Executed) from this [`Schema`](schema::Schema). By default, a maximum of
    /// 1000 entries will be returned, but that limit can be overridden by
    /// setting `result_limit`. A hard limit of 100,000 results will be
    /// returned. To begin listing after another known `transaction_id`, pass
    /// `transaction_id + 1` into `starting_id`.
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<transaction::Executed>, Error>;

    /// Fetches the last transaction id that has been committed, if any.
    async fn last_transaction_id(&self) -> Result<Option<u64>, Error>;

    /// Compacts the entire database to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::Io`]: an error occurred while compacting the database.
    async fn compact(&self) -> Result<(), crate::Error>;

    /// Compacts the collection to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::CollectionNotFound`]: database `name` does not exist.
    /// * [`Error::Io`]: an error occurred while compacting the database.
    async fn compact_collection<C: schema::Collection>(&self) -> Result<(), crate::Error> {
        self.compact_collection_by_name(C::collection_name()).await
    }

    /// Compacts the key value store to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::Io`]: an error occurred while compacting the database.
    async fn compact_key_value_store(&self) -> Result<(), crate::Error>;
}

/// Interacts with a collection over a `Connection`.
///
/// These examples in this type use this basic collection definition:
///
/// ```rust
/// use bonsaidb_core::{schema::Collection, Error};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, Default, Collection)]
/// #[collection(name = "MyCollection")]
/// # #[collection(core = bonsaidb_core)]
/// pub struct MyCollection {
///     pub rank: u32,
///     pub score: f32,
/// }
/// ```
pub struct AsyncCollection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, /* allows for extension traits to be written for collections of specific types */
}

impl<'a, Cn, Cl> Clone for AsyncCollection<'a, Cn, Cl> {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection,
            _phantom: PhantomData,
        }
    }
}

impl<'a, Cn, Cl> AsyncCollection<'a, Cn, Cl>
where
    Cn: AsyncConnection,
    Cl: schema::Collection,
{
    /// Creates a new instance using `connection`.
    fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            _phantom: PhantomData::default(),
        }
    }

    /// Adds a new `Document<Cl>` with the contents `item`.
    ///
    /// ## Automatic ID Assignment
    ///
    /// This function calls [`SerializedCollection::natural_id()`] to try to
    /// retrieve a primary key value from `item`. If an id is returned, the item
    /// is inserted with that id. If an id is not returned, an id will be
    /// automatically assigned, if possible, by the storage backend, which uses the [`Key`]
    /// trait to assign ids.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let inserted_header = db
    ///     .collection::<MyCollection>()
    ///     .push(&MyCollection::default())
    ///     .await?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn push(
        &self,
        item: &<Cl as SerializedCollection>::Contents,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        let contents = Cl::serialize(item)?;
        if let Some(natural_id) = Cl::natural_id(item) {
            self.insert_bytes(natural_id, contents).await
        } else {
            self.push_bytes(contents).await
        }
    }

    /// Adds a new `Document<Cl>` with the `contents`.
    ///
    /// ## Automatic ID Assignment
    ///
    /// An id will be automatically assigned, if possible, by the storage backend, which uses
    /// the [`Key`] trait to assign ids.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let inserted_header = db.collection::<MyCollection>().push_bytes(vec![]).await?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn push_bytes<B: Into<Bytes> + Send>(
        &self,
        contents: B,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        self.connection
            .insert::<Cl, _, B>(Option::<Cl::PrimaryKey>::None, contents)
            .await
    }

    /// Adds a new `Document<Cl>` with the given `id` and contents `item`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let inserted_header = db
    ///     .collection::<MyCollection>()
    ///     .insert(42, &MyCollection::default())
    ///     .await?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn insert<PrimaryKey>(
        &self,
        id: PrimaryKey,
        item: &<Cl as SerializedCollection>::Contents,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>> + Send + Sync,
    {
        let contents = Cl::serialize(item)?;
        self.connection.insert::<Cl, _, _>(Some(id), contents).await
    }

    /// Adds a new `Document<Cl>` with the the given `id` and `contents`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let inserted_header = db
    ///     .collection::<MyCollection>()
    ///     .insert_bytes(42, vec![])
    ///     .await?;
    /// println!(
    ///     "Inserted id {} with revision {}",
    ///     inserted_header.id, inserted_header.revision
    /// );
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn insert_bytes<B: Into<Bytes> + Send>(
        &self,
        id: Cl::PrimaryKey,
        contents: B,
    ) -> Result<CollectionHeader<Cl::PrimaryKey>, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        self.connection.insert::<Cl, _, B>(Some(id), contents).await
    }

    /// Updates an existing document. Upon success, `doc.revision` will be
    /// updated with the new revision.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// if let Some(mut document) = db.collection::<MyCollection>().get(42).await? {
    ///     // modify the document
    ///     db.collection::<MyCollection>().update(&mut document);
    ///     println!("Updated revision: {:?}", document.header.revision);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn update<D: Document<Cl> + Send + Sync>(&self, doc: &mut D) -> Result<(), Error> {
        self.connection.update::<Cl, D>(doc).await
    }

    /// Overwrites an existing document, or inserts a new document. Upon success,
    /// `doc.revision` will be updated with the new revision information.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// if let Some(mut document) = db.collection::<MyCollection>().get(42).await? {
    ///     // modify the document
    ///     db.collection::<MyCollection>().overwrite(&mut document);
    ///     println!("Updated revision: {:?}", document.header.revision);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn overwrite<D: Document<Cl> + Send + Sync>(&self, doc: &mut D) -> Result<(), Error> {
        let contents = doc.bytes()?;
        doc.set_collection_header(
            self.connection
                .overwrite::<Cl, _>(doc.key(), contents)
                .await?,
        )
    }

    /// Retrieves a `Document<Cl>` with `id` from the connection.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// if let Some(doc) = db.collection::<MyCollection>().get(42).await? {
    ///     println!(
    ///         "Retrieved bytes {:?} with revision {}",
    ///         doc.contents, doc.header.revision
    ///     );
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn get<PrimaryKey>(&self, id: PrimaryKey) -> Result<Option<OwnedDocument>, Error>
    where
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>> + Send,
    {
        self.connection.get::<Cl, _>(id).await
    }

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// for doc in db
    ///     .collection::<MyCollection>()
    ///     .get_multiple([42, 43])
    ///     .await?
    /// {
    ///     println!("Retrieved #{} with bytes {:?}", doc.header.id, doc.contents);
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn get_multiple<DocumentIds, PrimaryKey, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        DocumentIds: IntoIterator<Item = PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PrimaryKey> + Send + Sync,
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>> + Send + Sync,
    {
        self.connection.get_multiple::<Cl, _, _, _>(ids).await
    }

    /// Retrieves all documents matching the range of `ids`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// for doc in db
    ///     .collection::<MyCollection>()
    ///     .list(42..)
    ///     .descending()
    ///     .limit(20)
    ///     .await?
    /// {
    ///     println!("Retrieved #{} with bytes {:?}", doc.header.id, doc.contents);
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn list<PrimaryKey, R>(&'a self, ids: R) -> AsyncList<'a, Cn, Cl>
    where
        R: Into<Range<PrimaryKey>>,
        PrimaryKey: Into<AnyDocumentId<Cl::PrimaryKey>>,
    {
        AsyncList::new(
            PossiblyOwned::Borrowed(self),
            ids.into().map(PrimaryKey::into),
        )
    }

    /// Retrieves all documents with ids that start with `prefix`.
    ///
    /// ```rust
    /// use bonsaidb_core::{
    ///     connection::AsyncConnection,
    ///     document::OwnedDocument,
    ///     schema::{Collection, Schematic, SerializedCollection},
    ///     Error,
    /// };
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Serialize, Deserialize, Default, Collection)]
    /// #[collection(name = "MyCollection", primary_key = String)]
    /// # #[collection(core = bonsaidb_core)]
    /// pub struct MyCollection;
    ///
    /// async fn starts_with_a<C: AsyncConnection>(db: &C) -> Result<Vec<OwnedDocument>, Error> {
    ///     db.collection::<MyCollection>()
    ///         .list_with_prefix(String::from("a"))
    ///         .await
    /// }
    /// ```
    pub fn list_with_prefix(&'a self, prefix: Cl::PrimaryKey) -> AsyncList<'a, Cn, Cl>
    where
        Cl::PrimaryKey: IntoPrefixRange,
    {
        AsyncList::new(
            PossiblyOwned::Borrowed(self),
            prefix.into_prefix_range().map(AnyDocumentId::Deserialized),
        )
    }

    /// Retrieves all documents.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// for doc in db.collection::<MyCollection>().all().await? {
    ///     println!("Retrieved #{} with bytes {:?}", doc.header.id, doc.contents);
    ///     let deserialized = MyCollection::document_contents(&doc)?;
    ///     println!("Deserialized contents: {:?}", deserialized);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn all(&'a self) -> AsyncList<'a, Cn, Cl> {
        AsyncList::new(PossiblyOwned::Borrowed(self), Range::from(..))
    }

    /// Removes a `Document` from the database.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// if let Some(doc) = db.collection::<MyCollection>().get(42).await? {
    ///     db.collection::<MyCollection>().delete(&doc).await?;
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn delete<H: HasHeader + Send + Sync>(&self, doc: &H) -> Result<(), Error> {
        self.connection.delete::<Cl, H>(doc).await
    }
}

pub(crate) struct AsyncListBuilder<'a, Cn, Cl>
where
    Cl: schema::Collection,
{
    collection: PossiblyOwned<'a, AsyncCollection<'a, Cn, Cl>>,
    range: Range<AnyDocumentId<Cl::PrimaryKey>>,
    sort: Sort,
    limit: Option<u32>,
}

pub(crate) enum PossiblyOwned<'a, Cl> {
    Owned(Cl),
    Borrowed(&'a Cl),
}

impl<'a, Cl> Deref for PossiblyOwned<'a, Cl> {
    type Target = Cl;

    fn deref(&self) -> &Self::Target {
        match self {
            PossiblyOwned::Owned(value) => value,
            PossiblyOwned::Borrowed(value) => value,
        }
    }
}

pub(crate) enum ListState<'a, Cn, Cl>
where
    Cl: schema::Collection,
{
    Pending(Option<AsyncListBuilder<'a, Cn, Cl>>),
    Executing(BoxFuture<'a, Result<Vec<OwnedDocument>, Error>>),
}

/// Retrieves a list of documents from a collection, when awaited. This
/// structure also offers functions to customize the options for the operation.
#[must_use]
pub struct AsyncList<'a, Cn, Cl>
where
    Cl: schema::Collection,
{
    state: ListState<'a, Cn, Cl>,
}

impl<'a, Cn, Cl> AsyncList<'a, Cn, Cl>
where
    Cl: schema::Collection,
    Cn: AsyncConnection,
{
    pub(crate) fn new(
        collection: PossiblyOwned<'a, AsyncCollection<'a, Cn, Cl>>,
        range: Range<AnyDocumentId<Cl::PrimaryKey>>,
    ) -> Self {
        Self {
            state: ListState::Pending(Some(AsyncListBuilder {
                collection,
                range,
                sort: Sort::Ascending,
                limit: None,
            })),
        }
    }

    fn builder(&mut self) -> &mut AsyncListBuilder<'a, Cn, Cl> {
        if let ListState::Pending(Some(builder)) = &mut self.state {
            builder
        } else {
            unreachable!("Attempted to use after retrieving the result")
        }
    }

    /// Lists documents by id in ascending order.
    pub fn ascending(mut self) -> Self {
        self.builder().sort = Sort::Ascending;
        self
    }

    /// Lists documents by id in descending order.
    pub fn descending(mut self) -> Self {
        self.builder().sort = Sort::Descending;
        self
    }

    /// Sets the maximum number of results to return.
    pub fn limit(mut self, maximum_results: u32) -> Self {
        self.builder().limit = Some(maximum_results);
        self
    }

    /// Returns the list of headers for documents contained within the range.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// println!(
    ///     "Number of documents with id 42 or larger: {:?}",
    ///     db.collection::<MyCollection>().list(42..).headers().await?
    /// );
    /// println!(
    ///     "Number of documents in MyCollection: {:?}",
    ///     db.collection::<MyCollection>().all().headers().await?
    /// );
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn headers(self) -> Result<Vec<Header>, Error> {
        match self.state {
            ListState::Pending(Some(AsyncListBuilder {
                collection,
                range,
                sort,
                limit,
                ..
            })) => {
                collection
                    .connection
                    .list_headers::<Cl, _, _>(range, sort, limit)
                    .await
            }
            _ => unreachable!("Attempted to use after retrieving the result"),
        }
    }

    /// Returns the number of documents contained within the range.
    ///
    /// Order and limit are ignored if they were set.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: &C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// println!(
    ///     "Number of documents with id 42 or larger: {}",
    ///     db.collection::<MyCollection>().list(42..).count().await?
    /// );
    /// println!(
    ///     "Number of documents in MyCollection: {}",
    ///     db.collection::<MyCollection>().all().count().await?
    /// );
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn count(self) -> Result<u64, Error> {
        match self.state {
            ListState::Pending(Some(AsyncListBuilder {
                collection, range, ..
            })) => collection.connection.count::<Cl, _, _>(range).await,
            _ => unreachable!("Attempted to use after retrieving the result"),
        }
    }
}

impl<'a, Cn, Cl> Future for AsyncList<'a, Cn, Cl>
where
    Cn: AsyncConnection,
    Cl: schema::Collection + Unpin,
    Cl::PrimaryKey: Unpin,
{
    type Output = Result<Vec<OwnedDocument>, Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.state {
            ListState::Executing(future) => future.as_mut().poll(cx),
            ListState::Pending(builder) => {
                let AsyncListBuilder {
                    collection,
                    range,
                    sort,
                    limit,
                } = builder.take().unwrap();

                let future = async move {
                    collection
                        .connection
                        .list::<Cl, _, _>(range, sort, limit)
                        .await
                }
                .boxed();

                self.state = ListState::Executing(future);
                self.poll(cx)
            }
        }
    }
}

/// Parameters to query a [`schema::View`].
///
/// The examples for this type use this view definition:
///
/// ```rust
/// # mod collection {
/// # bonsaidb_core::__doctest_prelude!();
/// # }
/// # use collection::MyCollection;
/// use bonsaidb_core::{
///     define_basic_unique_mapped_view,
///     document::{CollectionDocument, Emit},
///     schema::{
///         CollectionViewSchema, DefaultViewSerialization, Name, ReduceResult, View,
///         ViewMapResult, ViewMappedValue,
///     },
/// };
///
/// #[derive(Debug, Clone, View)]
/// #[view(collection = MyCollection, key = u32, value = f32, name = "scores-by-rank")]
/// # #[view(core = bonsaidb_core)]
/// pub struct ScoresByRank;
///
/// impl CollectionViewSchema for ScoresByRank {
///     type View = Self;
///     fn map(
///         &self,
///         document: CollectionDocument<<Self::View as View>::Collection>,
///     ) -> ViewMapResult<Self::View> {
///         document
///             .header
///             .emit_key_and_value(document.contents.rank, document.contents.score)
///     }
///
///     fn reduce(
///         &self,
///         mappings: &[ViewMappedValue<Self::View>],
///         rereduce: bool,
///     ) -> ReduceResult<Self::View> {
///         if mappings.is_empty() {
///             Ok(0.)
///         } else {
///             Ok(mappings.iter().map(|map| map.value).sum::<f32>() / mappings.len() as f32)
///         }
///     }
/// }
/// ```
#[must_use]
pub struct AsyncView<'a, Cn, V: schema::SerializedView> {
    connection: &'a Cn,

    /// Key filtering criteria.
    pub key: Option<QueryKey<V::Key>>,

    /// The view's data access policy. The default value is [`AccessPolicy::UpdateBefore`].
    pub access_policy: AccessPolicy,

    /// The sort order of the query.
    pub sort: Sort,

    /// The maximum number of results to return.
    pub limit: Option<u32>,
}

impl<'a, Cn, V> AsyncView<'a, Cn, V>
where
    V: schema::SerializedView,
    Cn: AsyncConnection,
{
    fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            key: None,
            access_policy: AccessPolicy::UpdateBefore,
            sort: Sort::Ascending,
            limit: None,
        }
    }

    /// Filters for entries in the view with `key`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().with_key(42).query().await? {
    ///     assert_eq!(mapping.key, 42);
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn with_key(mut self, key: V::Key) -> Self {
        self.key = Some(QueryKey::Matches(key));
        self
    }

    /// Filters for entries in the view with `keys`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db
    ///     .view::<ScoresByRank>()
    ///     .with_keys([42, 43])
    ///     .query()
    ///     .await?
    /// {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn with_keys<IntoIter: IntoIterator<Item = V::Key>>(mut self, keys: IntoIter) -> Self {
        self.key = Some(QueryKey::Multiple(keys.into_iter().collect()));
        self
    }

    /// Filters for entries in the view with the range `keys`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db
    ///     .view::<ScoresByRank>()
    ///     .with_key_range(42..)
    ///     .query()
    ///     .await?
    /// {
    ///     assert!(mapping.key >= 42);
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn with_key_range<R: Into<Range<V::Key>>>(mut self, range: R) -> Self {
        self.key = Some(QueryKey::Range(range.into()));
        self
    }

    /// Filters for entries in the view with keys that begin with `prefix`.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// #[derive(View, Debug, Clone)]
    /// #[view(name = "by-name", key = String, collection = MyCollection)]
    /// # #[view(core = bonsaidb_core)]
    /// struct ByName;
    ///
    /// // score is an f32 in this example
    /// for mapping in db
    ///     .view::<ByName>()
    ///     .with_key_prefix(String::from("a"))
    ///     .query()
    ///     .await?
    /// {
    ///     assert!(mapping.key.starts_with("a"));
    ///     println!("{} in document {:?}", mapping.key, mapping.source);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn with_key_prefix(mut self, prefix: V::Key) -> Self
    where
        V::Key: IntoPrefixRange,
    {
        self.key = Some(QueryKey::Range(prefix.into_prefix_range()));
        self
    }

    /// Sets the access policy for queries.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db
    ///     .view::<ScoresByRank>()
    ///     .with_access_policy(AccessPolicy::UpdateAfter)
    ///     .query()
    ///     .await?
    /// {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn with_access_policy(mut self, policy: AccessPolicy) -> Self {
        self.access_policy = policy;
        self
    }

    /// Queries the view in ascending order. This is the default sorting
    /// behavior.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().ascending().query().await? {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn ascending(mut self) -> Self {
        self.sort = Sort::Ascending;
        self
    }

    /// Queries the view in descending order.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().descending().query().await? {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn descending(mut self) -> Self {
        self.sort = Sort::Descending;
        self
    }

    /// Sets the maximum number of results to return.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// let mappings = db.view::<ScoresByRank>().limit(10).query().await?;
    /// assert!(mappings.len() <= 10);
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn limit(mut self, maximum_results: u32) -> Self {
        self.limit = Some(maximum_results);
        self
    }

    /// Executes the query and retrieves the results.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().query().await? {
    ///     println!("Rank {} has a score of {:3}", mapping.key, mapping.value);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn query(self) -> Result<Vec<Map<V::Key, V::Value>>, Error> {
        self.connection
            .query::<V>(self.key, self.sort, self.limit, self.access_policy)
            .await
    }

    /// Executes the query and retrieves the results with the associated [`Document`s](crate::document::OwnedDocument).
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// for mapping in &db
    ///     .view::<ScoresByRank>()
    ///     .with_key_range(42..=44)
    ///     .query_with_docs()
    ///     .await?
    /// {
    ///     println!(
    ///         "Mapping from #{} with rank: {} and score: {}. Document bytes: {:?}",
    ///         mapping.document.header.id, mapping.key, mapping.value, mapping.document.contents
    ///     );
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn query_with_docs(self) -> Result<MappedDocuments<OwnedDocument, V>, Error> {
        self.connection
            .query_with_docs::<V>(self.key, self.sort, self.limit, self.access_policy)
            .await
    }

    /// Executes the query and retrieves the results with the associated [`CollectionDocument`s](crate::document::CollectionDocument).
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// for mapping in &db
    ///     .view::<ScoresByRank>()
    ///     .with_key_range(42..=44)
    ///     .query_with_collection_docs()
    ///     .await?
    /// {
    ///     println!(
    ///         "Mapping from #{} with rank: {} and score: {}. Deserialized Contents: {:?}",
    ///         mapping.document.header.id, mapping.key, mapping.value, mapping.document.contents
    ///     );
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn query_with_collection_docs(
        self,
    ) -> Result<MappedDocuments<CollectionDocument<V::Collection>, V>, Error>
    where
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
    {
        self.connection
            .query_with_collection_docs::<V>(self.key, self.sort, self.limit, self.access_policy)
            .await
    }

    /// Executes a reduce over the results of the query
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// let score = db.view::<ScoresByRank>().reduce().await?;
    /// println!("Average score: {:3}", score);
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn reduce(self) -> Result<V::Value, Error> {
        self.connection
            .reduce::<V>(self.key, self.access_policy)
            .await
    }

    /// Executes a reduce over the results of the query, grouping by key.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// // score is an f32 in this example
    /// for mapping in db.view::<ScoresByRank>().reduce_grouped().await? {
    ///     println!(
    ///         "Rank {} has an average score of {:3}",
    ///         mapping.key, mapping.value
    ///     );
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn reduce_grouped(self) -> Result<Vec<MappedValue<V::Key, V::Value>>, Error> {
        self.connection
            .reduce_grouped::<V>(self.key, self.access_policy)
            .await
    }

    /// Deletes all of the associated documents that match this view query.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// db.view::<ScoresByRank>().delete_docs().await?;
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn delete_docs(self) -> Result<u64, Error> {
        self.connection
            .delete_docs::<V>(self.key, self.access_policy)
            .await
    }
}

/// A sort order.
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum Sort {
    /// Sort ascending (A -> Z).
    Ascending,
    /// Sort descending (Z -> A).
    Descending,
}

/// Filters a [`View`] by key.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum QueryKey<K> {
    /// Matches all entries with the key provided.
    Matches(K),

    /// Matches all entires with keys in the range provided.
    Range(Range<K>),

    /// Matches all entries that have keys that are included in the set provided.
    Multiple(Vec<K>),
}

#[allow(clippy::use_self)] // clippy is wrong, Self is different because of generic parameters
impl<K: for<'a> Key<'a>> QueryKey<K> {
    /// Converts this key to a serialized format using the [`Key`] trait.
    pub fn serialized(&self) -> Result<QueryKey<Bytes>, Error> {
        match self {
            Self::Matches(key) => key
                .as_ord_bytes()
                .map_err(|err| Error::Database(view::Error::key_serialization(err).to_string()))
                .map(|v| QueryKey::Matches(Bytes::from(v.to_vec()))),
            Self::Range(range) => Ok(QueryKey::Range(range.as_ord_bytes().map_err(|err| {
                Error::Database(view::Error::key_serialization(err).to_string())
            })?)),
            Self::Multiple(keys) => {
                let keys = keys
                    .iter()
                    .map(|key| {
                        key.as_ord_bytes()
                            .map(|key| Bytes::from(key.to_vec()))
                            .map_err(|err| {
                                Error::Database(view::Error::key_serialization(err).to_string())
                            })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Ok(QueryKey::Multiple(keys))
            }
        }
    }
}

#[allow(clippy::use_self)] // clippy is wrong, Self is different because of generic parameters
impl<'a, T> QueryKey<T>
where
    T: AsRef<[u8]>,
{
    /// Deserializes the bytes into `K` via the [`Key`] trait.
    pub fn deserialized<K: for<'k> Key<'k>>(&self) -> Result<QueryKey<K>, Error> {
        match self {
            Self::Matches(key) => K::from_ord_bytes(key.as_ref())
                .map_err(|err| Error::Database(view::Error::key_serialization(err).to_string()))
                .map(QueryKey::Matches),
            Self::Range(range) => Ok(QueryKey::Range(range.deserialize().map_err(|err| {
                Error::Database(view::Error::key_serialization(err).to_string())
            })?)),
            Self::Multiple(keys) => {
                let keys = keys
                    .iter()
                    .map(|key| {
                        K::from_ord_bytes(key.as_ref()).map_err(|err| {
                            Error::Database(view::Error::key_serialization(err).to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Ok(QueryKey::Multiple(keys))
            }
        }
    }
}

/// A range type that can represent all `std` range types and be serialized.
///
/// This type implements conversion operations from all range types defined in
/// `std`.
#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, Eq, PartialEq)]
#[must_use]
pub struct Range<T> {
    /// The start of the range.
    pub start: Bound<T>,
    /// The end of the range.
    pub end: Bound<T>,
}

/// A range bound that can be serialized.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
#[must_use]
pub enum Bound<T> {
    /// No bound.
    Unbounded,
    /// Bounded by the contained value (inclusive).
    Included(T),
    /// Bounded by the contained value (exclusive).
    Excluded(T),
}

impl<T> Default for Bound<T> {
    fn default() -> Self {
        Self::Unbounded
    }
}

impl<T> Range<T> {
    /// Sets the start bound of this range to [`Bound::Excluded`] with
    /// `excluded_start`. The range will represent values that are
    /// [`Ordering::Greater`](std::cmp::Ordering::Greater) than, but not
    /// including, `excluded_start`.
    #[allow(clippy::missing_const_for_fn)]
    pub fn after(mut self, excluded_start: T) -> Self {
        self.start = Bound::Excluded(excluded_start);
        self
    }

    /// Sets the start bound of this range to [`Bound::Included`] with
    /// `included_start`. The range will represent values that are
    /// [`Ordering::Greater`](std::cmp::Ordering::Greater) than or
    /// [`Ordering::Equal`](std::cmp::Ordering::Equal) to `included_start`.
    #[allow(clippy::missing_const_for_fn)]
    pub fn start_at(mut self, included_start: T) -> Self {
        self.start = Bound::Included(included_start);
        self
    }

    /// Sets the end bound of this range to [`Bound::Excluded`] with
    /// `excluded_end`. The range will represent values that are
    /// [`Ordering::Less`](std::cmp::Ordering::Less) than, but not including,
    /// `excluded_end`.
    #[allow(clippy::missing_const_for_fn)]
    pub fn before(mut self, excluded_end: T) -> Self {
        self.end = Bound::Excluded(excluded_end);
        self
    }

    /// Sets the end bound of this range to [`Bound::Included`] with
    /// `included_end`. The range will represent values that are
    /// [`Ordering:::Less`](std::cmp::Ordering::Less) than or
    /// [`Ordering::Equal`](std::cmp::Ordering::Equal) to `included_end`.
    #[allow(clippy::missing_const_for_fn)]
    pub fn end_at(mut self, included_end: T) -> Self {
        self.end = Bound::Included(included_end);
        self
    }

    /// Maps each contained value with the function provided.
    pub fn map<U, F: Fn(T) -> U>(self, map: F) -> Range<U> {
        Range {
            start: self.start.map(&map),
            end: self.end.map(&map),
        }
    }
    /// Maps each contained value with the function provided. The callback's
    /// return type is a Result, unlike with `map`.
    pub fn map_result<U, E, F: Fn(T) -> Result<U, E>>(self, map: F) -> Result<Range<U>, E> {
        Ok(Range {
            start: self.start.map_result(&map)?,
            end: self.end.map_result(&map)?,
        })
    }

    /// Maps each contained value as a reference.
    pub fn map_ref<U: ?Sized, F: Fn(&T) -> &U>(&self, map: F) -> Range<&U> {
        Range {
            start: self.start.map_ref(&map),
            end: self.end.map_ref(&map),
        }
    }
}

#[test]
fn range_constructors() {
    assert_eq!(
        Range::default().after(1_u32),
        Range {
            start: Bound::Excluded(1),
            end: Bound::Unbounded
        }
    );
    assert_eq!(
        Range::default().start_at(1_u32),
        Range {
            start: Bound::Included(1),
            end: Bound::Unbounded
        }
    );
    assert_eq!(
        Range::default().before(1_u32),
        Range {
            start: Bound::Unbounded,
            end: Bound::Excluded(1),
        }
    );
    assert_eq!(
        Range::default().end_at(1_u32),
        Range {
            start: Bound::Unbounded,
            end: Bound::Included(1),
        }
    );
}

impl<'a, T: Key<'a>> Range<T> {
    /// Serializes the range's contained values to big-endian bytes.
    pub fn as_ord_bytes(&'a self) -> Result<Range<Bytes>, T::Error> {
        Ok(Range {
            start: self.start.as_ord_bytes()?,
            end: self.end.as_ord_bytes()?,
        })
    }
}

impl<'a, B> Range<B>
where
    B: AsRef<[u8]>,
{
    /// Deserializes the range's contained values from big-endian bytes.
    pub fn deserialize<T: for<'k> Key<'k>>(&'a self) -> Result<Range<T>, <T as Key<'_>>::Error> {
        Ok(Range {
            start: self.start.deserialize()?,
            end: self.start.deserialize()?,
        })
    }
}

impl<T> Bound<T> {
    /// Maps the contained value, if any, and returns the resulting `Bound`.
    pub fn map<U, F: Fn(T) -> U>(self, map: F) -> Bound<U> {
        match self {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(value) => Bound::Included(map(value)),
            Bound::Excluded(value) => Bound::Excluded(map(value)),
        }
    }

    /// Maps the contained value with the function provided. The callback's
    /// return type is a Result, unlike with `map`.
    pub fn map_result<U, E, F: Fn(T) -> Result<U, E>>(self, map: F) -> Result<Bound<U>, E> {
        Ok(match self {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(value) => Bound::Included(map(value)?),
            Bound::Excluded(value) => Bound::Excluded(map(value)?),
        })
    }

    /// Maps each contained value as a reference.
    pub fn map_ref<U: ?Sized, F: Fn(&T) -> &U>(&self, map: F) -> Bound<&U> {
        match self {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(value) => Bound::Included(map(value)),
            Bound::Excluded(value) => Bound::Excluded(map(value)),
        }
    }
}

impl<'a, T: Key<'a>> Bound<T> {
    /// Serializes the contained value to big-endian bytes.
    pub fn as_ord_bytes(&'a self) -> Result<Bound<Bytes>, T::Error> {
        match self {
            Bound::Unbounded => Ok(Bound::Unbounded),
            Bound::Included(value) => {
                Ok(Bound::Included(Bytes::from(value.as_ord_bytes()?.to_vec())))
            }
            Bound::Excluded(value) => {
                Ok(Bound::Excluded(Bytes::from(value.as_ord_bytes()?.to_vec())))
            }
        }
    }
}

impl<'a, B> Bound<B>
where
    B: AsRef<[u8]>,
{
    /// Deserializes the bound's contained value from big-endian bytes.
    pub fn deserialize<T: for<'k> Key<'k>>(&'a self) -> Result<Bound<T>, <T as Key<'_>>::Error> {
        match self {
            Bound::Unbounded => Ok(Bound::Unbounded),
            Bound::Included(value) => Ok(Bound::Included(T::from_ord_bytes(value.as_ref())?)),
            Bound::Excluded(value) => Ok(Bound::Excluded(T::from_ord_bytes(value.as_ref())?)),
        }
    }
}

impl<T> std::ops::RangeBounds<T> for Range<T> {
    fn start_bound(&self) -> std::ops::Bound<&T> {
        std::ops::Bound::from(&self.start)
    }

    fn end_bound(&self) -> std::ops::Bound<&T> {
        std::ops::Bound::from(&self.end)
    }
}

impl<'a, T> From<&'a Bound<T>> for std::ops::Bound<&'a T> {
    fn from(bound: &'a Bound<T>) -> Self {
        match bound {
            Bound::Unbounded => std::ops::Bound::Unbounded,
            Bound::Included(value) => std::ops::Bound::Included(value),
            Bound::Excluded(value) => std::ops::Bound::Excluded(value),
        }
    }
}

impl<T> From<std::ops::Range<T>> for Range<T> {
    fn from(range: std::ops::Range<T>) -> Self {
        Self {
            start: Bound::Included(range.start),
            end: Bound::Excluded(range.end),
        }
    }
}

impl<T> From<std::ops::RangeFrom<T>> for Range<T> {
    fn from(range: std::ops::RangeFrom<T>) -> Self {
        Self {
            start: Bound::Included(range.start),
            end: Bound::Unbounded,
        }
    }
}

impl<T> From<std::ops::RangeTo<T>> for Range<T> {
    fn from(range: std::ops::RangeTo<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(range.end),
        }
    }
}

impl<T: Clone> From<std::ops::RangeInclusive<T>> for Range<T> {
    fn from(range: std::ops::RangeInclusive<T>) -> Self {
        Self {
            start: Bound::Included(range.start().clone()),
            end: Bound::Included(range.end().clone()),
        }
    }
}

impl<T> From<std::ops::RangeToInclusive<T>> for Range<T> {
    fn from(range: std::ops::RangeToInclusive<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Included(range.end),
        }
    }
}

impl<T> From<std::ops::RangeFull> for Range<T> {
    fn from(_: std::ops::RangeFull) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

/// Changes how the view's outdated data will be treated.
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum AccessPolicy {
    /// Update any changed documents before returning a response.
    UpdateBefore,

    /// Return the results, which may be out-of-date, and start an update job in
    /// the background. This pattern is useful when you want to ensure you
    /// provide consistent response times while ensuring the database is
    /// updating in the background.
    UpdateAfter,

    /// Returns the results, which may be out-of-date, and do not start any
    /// background jobs. This mode is useful if you're using a view as a cache
    /// and have a background process that is responsible for controlling when
    /// data is refreshed and updated. While the default `UpdateBefore`
    /// shouldn't have much overhead, this option removes all overhead related
    /// to view updating from the query.
    NoUpdate,
}

/// Functions for interacting with a multi-database BonsaiDb instance.
#[async_trait]
pub trait StorageConnection: Sized + Send + Sync {
    /// The type that represents a database for this implementation.
    type Database: Connection;
    /// The [`StorageConnection`] type returned from authentication calls.
    type Authenticated: StorageConnection;

    /// Returns the currently authenticated session, if any.
    fn session(&self) -> Option<&Session>;

    /// Returns the administration database.
    fn admin(&self) -> Self::Database;

    /// Creates a database named `name` with the `Schema` provided.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken`]: `name` was already used for a
    ///   previous database name. Database names are case insensitive. Returned
    ///   if `only_if_needed` is false.
    fn create_database<DB: Schema>(
        &self,
        name: &str,
        only_if_needed: bool,
    ) -> Result<Self::Database, crate::Error> {
        self.create_database_with_schema(name, DB::schema_name(), only_if_needed)?;
        self.database::<DB>(name)
    }

    /// Returns a reference to database `name` with schema `DB`.
    fn database<DB: Schema>(&self, name: &str) -> Result<Self::Database, crate::Error>;

    /// Creates a database named `name` using the [`SchemaName`] `schema`.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken`]: `name` was already used for a
    ///   previous database name. Database names are case insensitive. Returned
    ///   if `only_if_needed` is false.
    fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), crate::Error>;

    /// Deletes a database named `name`.
    ///
    /// ## Errors
    ///
    /// * [`Error::DatabaseNotFound`]: database `name` does not exist.
    /// * [`Error::Io`]: an error occurred while deleting files.
    fn delete_database(&self, name: &str) -> Result<(), crate::Error>;

    /// Lists the databases in this storage.
    fn list_databases(&self) -> Result<Vec<Database>, crate::Error>;

    /// Lists the [`SchemaName`]s registered with this storage.
    fn list_available_schemas(&self) -> Result<Vec<SchemaName>, crate::Error>;

    /// Creates a user.
    fn create_user(&self, username: &str) -> Result<u64, crate::Error>;

    /// Deletes a user.
    fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), crate::Error>;

    /// Sets a user's password.
    #[cfg(feature = "password-hashing")]
    fn set_user_password<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: SensitiveString,
    ) -> Result<(), crate::Error>;

    /// Authenticates as a user with a authentication method.
    #[cfg(feature = "password-hashing")]
    fn authenticate<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Self::Authenticated, crate::Error>;

    /// Assumes the `identity`. If successful, the returned instance will have
    /// the merged permissions of the current authentication session and the
    /// permissions from `identity`.
    fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, crate::Error>;

    /// Adds a user to a permission group.
    fn add_permission_group_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    fn remove_permission_group_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Adds a user to a permission group.
    fn add_role_to_user<
        'user,
        'role,
        U: Nameable<'user, u64> + Send + Sync,
        R: Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    fn remove_role_from_user<
        'user,
        'role,
        U: Nameable<'user, u64> + Send + Sync,
        R: Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;
}

/// Functions for interacting with a multi-database BonsaiDb instance.
#[async_trait]
pub trait AsyncStorageConnection: Sized + Send + Sync {
    /// The type that represents a database for this implementation.
    type Database: AsyncConnection;
    /// The [`StorageConnection`] type returned from authentication calls.
    type Authenticated: AsyncStorageConnection;

    /// Returns the currently authenticated session, if any.
    fn session(&self) -> Option<&Session>;

    /// Returns the currently authenticated session, if any.
    async fn admin(&self) -> Self::Database;
    /// Creates a database named `name` with the `Schema` provided.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken`]: `name` was already used for a
    ///   previous database name. Database names are case insensitive. Returned
    ///   if `only_if_needed` is false.
    async fn create_database<DB: Schema>(
        &self,
        name: &str,
        only_if_needed: bool,
    ) -> Result<Self::Database, crate::Error> {
        self.create_database_with_schema(name, DB::schema_name(), only_if_needed)
            .await?;
        self.database::<DB>(name).await
    }

    /// Returns a reference to database `name` with schema `DB`.
    async fn database<DB: Schema>(&self, name: &str) -> Result<Self::Database, crate::Error>;

    /// Creates a database named `name` using the [`SchemaName`] `schema`.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken`]: `name` was already used for a
    ///   previous database name. Database names are case insensitive. Returned
    ///   if `only_if_needed` is false.
    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), crate::Error>;

    /// Deletes a database named `name`.
    ///
    /// ## Errors
    ///
    /// * [`Error::DatabaseNotFound`]: database `name` does not exist.
    /// * [`Error::Io`]: an error occurred while deleting files.
    async fn delete_database(&self, name: &str) -> Result<(), crate::Error>;

    /// Lists the databases in this storage.
    async fn list_databases(&self) -> Result<Vec<Database>, crate::Error>;

    /// Lists the [`SchemaName`]s registered with this storage.
    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, crate::Error>;

    /// Creates a user.
    async fn create_user(&self, username: &str) -> Result<u64, crate::Error>;

    /// Deletes a user.
    async fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), crate::Error>;

    /// Sets a user's password.
    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: SensitiveString,
    ) -> Result<(), crate::Error>;

    /// Authenticates as a user with a authentication method.
    #[cfg(feature = "password-hashing")]
    async fn authenticate<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Self::Authenticated, crate::Error>;

    /// Assumes the `identity`. If successful, the returned instance will have
    /// the merged permissions of the current authentication session and the
    /// permissions from `identity`.
    async fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, crate::Error>;

    /// Adds a user to a permission group.
    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Adds a user to a permission group.
    async fn add_role_to_user<
        'user,
        'role,
        U: Nameable<'user, u64> + Send + Sync,
        R: Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    async fn remove_role_from_user<
        'user,
        'role,
        U: Nameable<'user, u64> + Send + Sync,
        R: Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;
}

/// A database stored in BonsaiDb.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Database {
    /// The name of the database.
    pub name: String,
    /// The schema defining the database.
    pub schema: SchemaName,
}

/// A plain-text password. This struct automatically overwrites the password
/// with zeroes when dropped.
#[derive(Clone, Serialize, Deserialize, Zeroize)]
#[zeroize(drop)]
#[serde(transparent)]
pub struct SensitiveString(pub String);

impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SensitiveString(...)")
    }
}

impl Deref for SensitiveString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// User authentication methods.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Authentication {
    /// Authenticate using a password.
    #[cfg(feature = "password-hashing")]
    Password(crate::connection::SensitiveString),
}

#[doc(hidden)]
#[macro_export]
macro_rules! __doctest_prelude {
    () => {
        use bonsaidb_core::{
            connection::AccessPolicy,
            define_basic_unique_mapped_view,
            document::{CollectionDocument,Emit, Document, OwnedDocument},
            schema::{
                Collection, CollectionName, CollectionViewSchema, DefaultSerialization,
                DefaultViewSerialization, Name, NamedCollection, ReduceResult, Schema, SchemaName,
                Schematic, SerializedCollection, View, ViewMapResult, ViewMappedValue,
            },
            Error,
        };
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Schema)]
        #[schema(name = "MySchema", collections = [MyCollection], core = $crate)]
        pub struct MySchema;

        #[derive( Debug, Serialize, Deserialize, Default, Collection)]
        #[collection(name = "MyCollection", views = [MyCollectionByName], core = $crate)]
        pub struct MyCollection {
            pub name: String,
            pub rank: u32,
            pub score: f32,
        }

        impl MyCollection {
            pub fn named(s: impl Into<String>) -> Self {
                Self::new(s, 0, 0.)
            }

            pub fn new(s: impl Into<String>, rank: u32, score: f32) -> Self {
                Self {
                    name: s.into(),
                    rank,
                    score,
                }
            }
        }

        impl NamedCollection for MyCollection {
            type ByNameView = MyCollectionByName;
        }

        #[derive(Debug, Clone, View)]
        #[view(collection = MyCollection, key = u32, value = f32, name = "scores-by-rank", core = $crate)]
        pub struct ScoresByRank;

        impl CollectionViewSchema for ScoresByRank {
            type View = Self;
            fn map(
                &self,
                document: CollectionDocument<<Self::View as View>::Collection>,
            ) -> ViewMapResult<Self::View> {
                document
                    .header
                    .emit_key_and_value(document.contents.rank, document.contents.score)
            }

            fn reduce(
                &self,
                mappings: &[ViewMappedValue<Self::View>],
                rereduce: bool,
            ) -> ReduceResult<Self::View> {
                if mappings.is_empty() {
                    Ok(0.)
                } else {
                    Ok(mappings.iter().map(|map| map.value).sum::<f32>() / mappings.len() as f32)
                }
            }
        }

        define_basic_unique_mapped_view!(
            MyCollectionByName,
            MyCollection,
            1,
            "by-name",
            String,
            (),
            |document: CollectionDocument<MyCollection>| {
                document.header.emit_key(document.contents.name.clone())
            },
        );
    };
}

/// The authentication state for a [`StorageConnection`].
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[must_use]
pub struct Session {
    /// The session's unique ID.
    pub id: Option<SessionId>,
    /// The authenticated identity, if any.
    pub identity: Option<Arc<Identity>>,
    /// The effective permissions of the session.
    pub permissions: Permissions,
}

/// A unique session ID.
#[derive(Default, Clone, Copy, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(pub u64);

impl Session {
    /// Checks if `action` is permitted against `resource_name`.
    pub fn allowed_to<'a, R: AsRef<[Identifier<'a>]>, P: Action>(
        &self,
        resource_name: R,
        action: &P,
    ) -> bool {
        self.permissions.allowed_to(resource_name, action)
    }

    /// Checks if `action` is permitted against `resource_name`. If permission
    /// is denied, returns a [`PermissionDenied`](Error::PermissionDenied)
    /// error.
    pub fn check_permission<'a, R: AsRef<[Identifier<'a>]>, P: Action>(
        &self,
        resource_name: R,
        action: &P,
    ) -> Result<(), Error> {
        self.permissions
            .check(resource_name, action)
            .map_err(Error::from)
    }
}

impl Eq for Session {}

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl std::hash::Hash for Session {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

/// An identity from the connected BonsaiDb instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Identity {
    /// A [`User`](crate::admin::User).
    User {
        /// The unique ID of the user.
        id: u64,
        /// The username of the user.
        username: String,
    },
    // for role assumption someday: Role{ id: u64, name: String } ,
}

impl Eq for Identity {}

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::User { id: l_id, .. }, Self::User { id: r_id, .. }) => l_id == r_id,
        }
    }
}

impl std::hash::Hash for Identity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Identity::User { id, .. } => {
                0_u8.hash(state); // "Tag" for the variant
                id.hash(state);
            }
        }
    }
}

/// A reference to an identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IdentityReference<'name> {
    /// A reference to a [`User`](crate::admin::User).
    User(NamedReference<'name, u64>),
    // for role assumption someday: Role(NamedReference<'name, u64>),
}

impl<'name> IdentityReference<'name> {
    /// Converts this reference to an owned reference with a `'static` lifetime.
    #[must_use]
    pub fn into_owned(self) -> IdentityReference<'static> {
        match self {
            IdentityReference::User(user) => IdentityReference::User(user.into_owned()),
        }
    }
}
