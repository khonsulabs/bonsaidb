use std::{borrow::Borrow, collections::BTreeMap};

use arc_bytes::serde::Bytes;
use async_trait::async_trait;

use super::GroupedReductions;
use crate::{
    connection::{
        AccessPolicy, HasSession, QueryKey, Range, RangeRef, SerializedQueryKey, Sort, ViewMappings,
    },
    document::{
        CollectionDocument, CollectionHeader, Document, DocumentId, HasHeader, Header,
        OwnedDocument,
    },
    key::{self, Key, KeyEncoding},
    schema::{
        self,
        view::{
            self,
            map::{MappedDocuments, MappedSerializedValue},
        },
        CollectionName, Map, MappedValue, Schematic, SerializedCollection, ViewName,
    },
    transaction::{OperationResult, Transaction},
    Error,
};

/// The low-level interface to a database's [`schema::Schema`], giving access to
/// [`Collection`s](crate::schema::Collection) and
/// [`Views`s](crate::schema::View). This trait is not safe to use within async
/// contexts and will block the current thread. For async access, use
/// [`AsyncLowLevelConnection`].
///
/// This trait's methods are not designed for ergonomics. See
/// [`Connection`](super::Connection) for a higher-level interface.
pub trait LowLevelConnection: HasSchema + HasSession {
    /// Inserts a newly created document into the connected [`schema::Schema`]
    /// for the [`Collection`](schema::Collection) `C`. If `id` is `None` a unique id will be
    /// generated. If an id is provided and a document already exists with that
    /// id, a conflict error will be returned.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::push_into()`]
    /// - [`SerializedCollection::insert_into()`]
    /// - [`self.collection::<Collection>().insert()`](super::Collection::insert)
    /// - [`self.collection::<Collection>().push()`](super::Collection::push)
    fn insert<C, PrimaryKey, B>(
        &self,
        id: Option<&PrimaryKey>,
        contents: B,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error>
    where
        C: schema::Collection,
        B: Into<Bytes> + Send,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + Send + ?Sized,
    {
        let contents = contents.into();
        let results = self.apply_transaction(Transaction::insert(
            C::collection_name(),
            id.map(|id| DocumentId::new(id)).transpose()?,
            contents,
        ))?;
        if let Some(OperationResult::DocumentUpdated { header, .. }) = results.into_iter().next() {
            CollectionHeader::try_from(header)
        } else {
            unreachable!(
                "apply_transaction on a single insert should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Updates an existing document in the connected [`schema::Schema`] for the
    /// [`Collection`](schema::Collection) `C`. Upon success, `doc.revision` will be updated with
    /// the new revision.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`CollectionDocument::update()`]
    /// - [`self.collection::<Collection>().update()`](super::Collection::update)
    fn update<C: schema::Collection, D: Document<C> + Send + Sync>(
        &self,
        doc: &mut D,
    ) -> Result<(), Error> {
        let results = self.apply_transaction(Transaction::update(
            C::collection_name(),
            doc.header().into_header()?,
            doc.bytes()?,
        ))?;
        if let Some(OperationResult::DocumentUpdated { header, .. }) = results.into_iter().next() {
            doc.set_header(header)?;
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Overwrites an existing document, or inserts a new document. Upon success,
    /// `doc.revision` will be updated with the new revision information.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::overwrite()`]
    /// - [`SerializedCollection::overwrite_into()`]
    /// - [`self.collection::<Collection>().overwrite()`](super::Collection::overwrite)
    fn overwrite<C, PrimaryKey>(
        &self,
        id: &PrimaryKey,
        contents: Vec<u8>,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error>
    where
        C: schema::Collection,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey>,
    {
        let results = self.apply_transaction(Transaction::overwrite(
            C::collection_name(),
            DocumentId::new(id)?,
            contents,
        ))?;
        if let Some(OperationResult::DocumentUpdated { header, .. }) = results.into_iter().next() {
            CollectionHeader::try_from(header)
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Retrieves a stored document from [`Collection`](schema::Collection) `C` identified by `id`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::get()`]
    /// - [`self.collection::<Collection>().get()`](super::Collection::get)
    fn get<C, PrimaryKey>(&self, id: &PrimaryKey) -> Result<Option<OwnedDocument>, Error>
    where
        C: schema::Collection,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + ?Sized,
    {
        self.get_from_collection(DocumentId::new(id)?, &C::collection_name())
    }

    /// Retrieves all documents matching `ids`. Documents that are not found are
    /// not returned, but no error will be generated.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::get_multiple()`]
    /// - [`self.collection::<Collection>().get_multiple()`](super::Collection::get_multiple)
    fn get_multiple<'id, C, PrimaryKey, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        DocumentIds: IntoIterator<Item = &'id PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = &'id PrimaryKey> + Send + Sync,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + 'id + ?Sized,
    {
        let ids = ids
            .into_iter()
            .map(|id| DocumentId::new(id))
            .collect::<Result<Vec<_>, _>>()?;
        self.get_multiple_from_collection(&ids, &C::collection_name())
    }

    /// Retrieves all documents within the range of `ids`. To retrieve all
    /// documents, pass in `..` for `ids`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all()`]
    /// - [`self.collection::<Collection>().all()`](super::Collection::all)
    /// - [`SerializedCollection::list()`]
    /// - [`self.collection::<Collection>().list()`](super::Collection::list)
    fn list<'id, C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        R: Into<RangeRef<'id, C::PrimaryKey, PrimaryKey>> + Send,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + PartialEq + 'id + ?Sized,
        C::PrimaryKey: Borrow<PrimaryKey> + PartialEq<PrimaryKey>,
    {
        let ids = ids.into().map_result(|id| DocumentId::new(id))?;
        self.list_from_collection(ids, order, limit, &C::collection_name())
    }

    /// Retrieves all documents within the range of `ids`. To retrieve all
    /// documents, pass in `..` for `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using one
    /// of:
    ///
    /// - [`SerializedCollection::all_async().headers()`](schema::List::headers)
    /// - [`self.collection::<Collection>().all().headers()`](super::List::headers)
    /// - [`SerializedCollection::list_async().headers()`](schema::List::headers)
    /// - [`self.collection::<Collection>().list().headers()`](super::List::headers)
    fn list_headers<'id, C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<Header>, Error>
    where
        C: schema::Collection,
        R: Into<RangeRef<'id, C::PrimaryKey, PrimaryKey>> + Send,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + PartialEq + 'id + ?Sized,
        C::PrimaryKey: Borrow<PrimaryKey> + PartialEq<PrimaryKey>,
    {
        let ids = ids.into().map_result(|id| DocumentId::new(id))?;
        self.list_headers_from_collection(ids, order, limit, &C::collection_name())
    }

    /// Counts the number of documents within the range of `ids`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().all().count()`](super::List::count)
    /// - [`SerializedCollection::list().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().list().count()`](super::List::count)
    fn count<'id, C, R, PrimaryKey>(&self, ids: R) -> Result<u64, Error>
    where
        C: schema::Collection,
        R: Into<RangeRef<'id, C::PrimaryKey, PrimaryKey>> + Send,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + PartialEq + 'id + ?Sized,
        C::PrimaryKey: Borrow<PrimaryKey> + PartialEq<PrimaryKey>,
    {
        self.count_from_collection(
            ids.into().map_result(|id| DocumentId::new(id))?,
            &C::collection_name(),
        )
    }

    /// Removes a `Document` from the database.
    ///
    /// This is a lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`CollectionDocument::delete()`]
    /// - [`self.collection::<Collection>().delete()`](super::Collection::delete)
    fn delete<C: schema::Collection, H: HasHeader + Send + Sync>(
        &self,
        doc: &H,
    ) -> Result<(), Error> {
        let results =
            self.apply_transaction(Transaction::delete(C::collection_name(), doc.header()?))?;
        if let OperationResult::DocumentDeleted { .. } = &results[0] {
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Queries for view entries matching [`View`](schema::View).
    ///
    /// This is a lower-level API. For better ergonomics, consider querying the
    /// view using [`View::entries(self).query()`](super::View::query) instead. The
    /// parameters for the query can be customized on the builder returned from
    /// [`SerializedView::entries()`](schema::SerializedView::entries),
    /// [`SerializedView::entries_async()`](schema::SerializedView::entries_async),
    /// or [`Connection::view()`](super::Connection::view).
    fn query<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<ViewMappings<V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        let mappings = self.query_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            order,
            limit,
            access_policy,
        )?;
        mappings
            .into_iter()
            .map(|mapping| {
                Ok(Map {
                    key: <V::Key as key::Key>::from_ord_bytes(&mapping.key)
                        .map_err(view::Error::key_serialization)
                        .map_err(Error::from)?,
                    value: V::deserialize(&mapping.value)?,
                    source: mapping.source,
                })
            })
            .collect::<Result<Vec<_>, Error>>()
    }

    /// Queries for view entries matching [`View`](schema::View) with their
    /// source documents.
    ///
    /// This is a lower-level API. For better ergonomics, consider querying the
    /// view using
    /// [`View::entries(self).query_with_docs()`](super::View::query_with_docs)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from
    /// [`SerializedView::entries()`](schema::SerializedView::entries),
    /// [`SerializedView::entries_async()`](schema::SerializedView::entries_async),
    /// or [`Connection::view()`](super::Connection::view).
    fn query_with_docs<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        // Query permission is checked by the query call
        let results = self.query::<V, Key>(key, order, limit, access_policy)?;

        // Verify that there is permission to fetch each document
        let documents = self
            .get_multiple::<V::Collection, _, _, _>(results.iter().map(|m| &m.source.id))?
            .into_iter()
            .map(|doc| (doc.header.id.clone(), doc))
            .collect::<BTreeMap<_, _>>();

        Ok(MappedDocuments {
            mappings: results,
            documents,
        })
    }

    /// Queries for view entries matching [`View`](schema::View) with their
    /// source documents, deserialized.
    ///
    /// This is a lower-level API. For better ergonomics, consider querying the
    /// view using
    /// [`View::entries(self).query_with_collection_docs()`](super::View::query_with_collection_docs)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from
    /// [`SerializedView::entries()`](schema::SerializedView::entries),
    /// [`SerializedView::entries_async()`](schema::SerializedView::entries_async),
    /// or [`Connection::view()`](super::Connection::view).
    fn query_with_collection_docs<V, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<CollectionDocument<V::Collection>, V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
        V: schema::SerializedView,
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
    {
        let mapped_docs = self.query_with_docs::<V, Key>(key, order, limit, access_policy)?;
        let mut collection_docs = BTreeMap::new();
        for (id, doc) in mapped_docs.documents {
            collection_docs.insert(id, CollectionDocument::<V::Collection>::try_from(&doc)?);
        }
        Ok(MappedDocuments {
            mappings: mapped_docs.mappings,
            documents: collection_docs,
        })
    }

    /// Reduces the view entries matching [`View`](schema::View).
    ///
    /// This is a lower-level API. For better ergonomics, consider reducing the
    /// view using [`View::entries(self).reduce()`](super::View::reduce)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from
    /// [`SerializedView::entries()`](schema::SerializedView::entries),
    /// [`SerializedView::entries_async()`](schema::SerializedView::entries_async),
    /// or [`Connection::view()`](super::Connection::view).
    fn reduce<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        self.reduce_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
        )
        .and_then(|value| V::deserialize(&value))
    }

    /// Reduces the view entries matching [`View`](schema::View), reducing the
    /// values by each unique key.
    ///
    /// This is a lower-level API. For better ergonomics, consider reducing the
    /// view using
    /// [`View::entries(self).reduce_grouped()`](super::View::reduce_grouped)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from
    /// [`SerializedView::entries()`](schema::SerializedView::entries),
    /// [`SerializedView::entries_async()`](schema::SerializedView::entries_async),
    /// or [`Connection::view()`](super::Connection::view).
    fn reduce_grouped<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        access_policy: AccessPolicy,
    ) -> Result<GroupedReductions<V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        self.reduce_grouped_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
        )?
        .into_iter()
        .map(|map| {
            Ok(MappedValue::new(
                V::Key::from_ord_bytes(&map.key).map_err(view::Error::key_serialization)?,
                V::deserialize(&map.value)?,
            ))
        })
        .collect::<Result<Vec<_>, Error>>()
    }

    /// Deletes all of the documents associated with this view.
    ///
    /// This is a lower-level API. For better ergonomics, consider querying the
    /// view using
    /// [`View::entries(self).delete_docs()`](super::View::delete_docs())
    /// instead. The parameters for the query can be customized on the builder
    /// returned from
    /// [`SerializedView::entries()`](schema::SerializedView::entries),
    /// [`SerializedView::entries_async()`](schema::SerializedView::entries_async),
    /// or [`Connection::view()`](super::Connection::view).
    fn delete_docs<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        self.delete_docs_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
        )
    }

    /// Applies a [`Transaction`] to the [`schema::Schema`]. If any operation in the
    /// [`Transaction`] fails, none of the operations will be applied to the
    /// [`schema::Schema`].
    fn apply_transaction(&self, transaction: Transaction) -> Result<Vec<OperationResult>, Error>;

    /// Retrieves the document with `id` stored within the named `collection`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::get()`]
    /// - [`self.collection::<Collection>().get()`](super::Collection::get)
    fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, Error>;

    /// Retrieves all documents matching `ids` from the named `collection`.
    /// Documents that are not found are not returned, but no error will be
    /// generated.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::get_multiple()`]
    /// - [`self.collection::<Collection>().get_multiple()`](super::Collection::get_multiple)
    fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    /// Retrieves all documents within the range of `ids` from the named
    /// `collection`. To retrieve all documents, pass in `..` for `ids`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all()`]
    /// - [`self.collection::<Collection>().all()`](super::Collection::all)
    /// - [`SerializedCollection::list()`]
    /// - [`self.collection::<Collection>().list()`](super::Collection::list)
    fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    /// Retrieves all headers within the range of `ids` from the named
    /// `collection`. To retrieve all documents, pass in `..` for `ids`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all().headers()`](schema::List::headers)
    /// - [`self.collection::<Collection>().all().headers()`](super::AsyncCollection::all)
    /// - [`SerializedCollection::list().headers()`](schema::List::headers)
    /// - [`self.collection::<Collection>().list()`](super::AsyncCollection::list)
    fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, Error>;

    /// Counts the number of documents within the range of `ids` from the named
    /// `collection`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().all().count()`](super::List::count)
    /// - [`SerializedCollection::list().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().list().count()`](super::List::count)
    fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, Error>;

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
    /// * [`Error::Other`]: an error occurred while compacting the database.
    fn compact_collection_by_name(&self, collection: CollectionName) -> Result<(), Error>;

    /// Queries for view entries from the named `view`.
    ///
    /// This is a lower-level API. For better ergonomics, consider querying the
    /// view using [`View::entries(self).query()`](super::View::query) instead. The
    /// parameters for the query can be customized on the builder returned from
    /// [`Connection::view()`](super::Connection::view).
    fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, Error>;

    /// Queries for view entries from the named `view` with their source
    /// documents.
    ///
    /// This is a lower-level API. For better ergonomics, consider querying the
    /// view using
    /// [`View::entries(self).query_with_docs()`](super::View::query_with_docs)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`Connection::view()`](super::Connection::view).
    fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, Error>;

    /// Reduces the view entries from the named `view`.
    ///
    /// This is a lower-level API. For better ergonomics, consider reducing the
    /// view using [`View::entries(self).reduce()`](super::View::reduce)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`Connection::view()`](super::Connection::view).
    fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, Error>;

    /// Reduces the view entries from the named `view`, reducing the values by each
    /// unique key.
    ///
    /// This is a lower-level API. For better ergonomics, consider reducing
    /// the view using
    /// [`View::entries(self).reduce_grouped()`](super::View::reduce_grouped) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Connection::view()`](super::Connection::view).
    fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, Error>;

    /// Deletes all source documents for entries that match within the named
    /// `view`.
    ///
    /// This is a lower-level API. For better ergonomics, consider querying the
    /// view using
    /// [`View::entries(self).delete_docs()`](super::View::delete_docs())
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`Connection::view()`](super::Connection::view).
    fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>;
}

/// The low-level interface to a database's [`schema::Schema`], giving access to
/// [`Collection`s](crate::schema::Collection) and
/// [`Views`s](crate::schema::View). This trait is for use within async
/// contexts. For access outside of async contexts, use [`LowLevelConnection`].
///
/// This trait's methods are not designed for ergonomics. See
/// [`AsyncConnection`](super::AsyncConnection) for a higher-level interface.
#[async_trait]
pub trait AsyncLowLevelConnection: HasSchema + HasSession + Send + Sync {
    /// Inserts a newly created document into the connected [`schema::Schema`]
    /// for the [`Collection`](schema::Collection) `C`. If `id` is `None` a unique id will be
    /// generated. If an id is provided and a document already exists with that
    /// id, a conflict error will be returned.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::push_into_async()`]
    /// - [`SerializedCollection::insert_into_async()`]
    /// - [`self.collection::<Collection>().insert()`](super::AsyncCollection::insert)
    /// - [`self.collection::<Collection>().push()`](super::AsyncCollection::push)
    async fn insert<C: schema::Collection, PrimaryKey: Send, B: Into<Bytes> + Send>(
        &self,
        id: Option<&PrimaryKey>,
        contents: B,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error>
    where
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + ?Sized,
    {
        let contents = contents.into();
        let results = self
            .apply_transaction(Transaction::insert(
                C::collection_name(),
                id.map(|id| DocumentId::new(id)).transpose()?,
                contents,
            ))
            .await?;
        if let Some(OperationResult::DocumentUpdated { header, .. }) = results.into_iter().next() {
            CollectionHeader::try_from(header)
        } else {
            unreachable!(
                "apply_transaction on a single insert should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Updates an existing document in the connected [`schema::Schema`] for the
    /// [`Collection`](schema::Collection)(schema::Collection) `C`. Upon success, `doc.revision`
    /// will be updated with the new revision.
    ///
    /// This is the lower-level API. For better ergonomics, consider using one
    /// of:
    ///
    /// - [`CollectionDocument::update_async()`]
    /// - [`self.collection::<Collection>().update()`](super::AsyncCollection::update)
    async fn update<C: schema::Collection, D: Document<C> + Send + Sync>(
        &self,
        doc: &mut D,
    ) -> Result<(), Error> {
        let results = self
            .apply_transaction(Transaction::update(
                C::collection_name(),
                doc.header().into_header()?,
                doc.bytes()?,
            ))
            .await?;
        if let Some(OperationResult::DocumentUpdated { header, .. }) = results.into_iter().next() {
            doc.set_header(header)?;
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Overwrites an existing document, or inserts a new document. Upon success,
    /// `doc.revision` will be updated with the new revision information.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::overwrite_async()`]
    /// - [`SerializedCollection::overwrite_into_async()`]
    /// - [`self.collection::<Collection>().overwrite()`](super::AsyncCollection::overwrite)
    async fn overwrite<'a, C, PrimaryKey>(
        &self,
        id: &PrimaryKey,
        contents: Vec<u8>,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error>
    where
        C: schema::Collection,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey>,
    {
        let results = self
            .apply_transaction(Transaction::overwrite(
                C::collection_name(),
                DocumentId::new(id)?,
                contents,
            ))
            .await?;
        if let Some(OperationResult::DocumentUpdated { header, .. }) = results.into_iter().next() {
            CollectionHeader::try_from(header)
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Retrieves a stored document from [`Collection`](schema::Collection) `C` identified by `id`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::get_async()`]
    /// - [`self.collection::<Collection>().get()`](super::AsyncCollection::get)
    async fn get<C, PrimaryKey>(&self, id: &PrimaryKey) -> Result<Option<OwnedDocument>, Error>
    where
        C: schema::Collection,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + ?Sized,
    {
        self.get_from_collection(DocumentId::new(id)?, &C::collection_name())
            .await
    }

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::get_multiple_async()`]
    /// - [`self.collection::<Collection>().get_multiple()`](super::AsyncCollection::get_multiple)
    async fn get_multiple<'id, C, PrimaryKey, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        DocumentIds: IntoIterator<Item = &'id PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = &'id PrimaryKey> + Send + Sync,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + 'id + ?Sized,
    {
        let ids = ids
            .into_iter()
            .map(DocumentId::new)
            .collect::<Result<Vec<_>, _>>()?;
        self.get_multiple_from_collection(&ids, &C::collection_name())
            .await
    }

    /// Retrieves all documents within the range of `ids`. To retrieve all
    /// documents, pass in `..` for `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using one
    /// of:
    ///
    /// - [`SerializedCollection::all_async()`]
    /// - [`self.collection::<Collection>().all()`](super::AsyncCollection::all)
    /// - [`SerializedCollection::list_async()`]
    /// - [`self.collection::<Collection>().list()`](super::AsyncCollection::list)
    async fn list<'id, C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        R: Into<RangeRef<'id, C::PrimaryKey, PrimaryKey>> + Send,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + PartialEq + 'id + ?Sized,
        C::PrimaryKey: Borrow<PrimaryKey> + PartialEq<PrimaryKey>,
    {
        let ids = ids.into().map_result(|id| DocumentId::new(id))?;
        self.list_from_collection(ids, order, limit, &C::collection_name())
            .await
    }

    /// Retrieves all documents within the range of `ids`. To retrieve all
    /// documents, pass in `..` for `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using one
    /// of:
    ///
    /// - [`SerializedCollection::all_async().headers()`](schema::AsyncList::headers)
    /// - [`self.collection::<Collection>().all()`](super::AsyncList::headers)
    /// - [`SerializedCollection::list_async().headers()`](schema::AsyncList::headers)
    /// - [`self.collection::<Collection>().list().headers()`](super::AsyncList::headers)
    async fn list_headers<'id, C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<Header>, Error>
    where
        C: schema::Collection,
        R: Into<RangeRef<'id, C::PrimaryKey, PrimaryKey>> + Send,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + PartialEq + 'id + ?Sized,
        C::PrimaryKey: Borrow<PrimaryKey> + PartialEq<PrimaryKey>,
    {
        let ids = ids.into().map_result(|id| DocumentId::new(id))?;
        self.list_headers_from_collection(ids, order, limit, &C::collection_name())
            .await
    }

    /// Counts the number of documents within the range of `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::all_async().count()`](schema::AsyncList::count)
    /// - [`self.collection::<Collection>().all().count()`](super::AsyncList::count)
    /// - [`SerializedCollection::list_async().count()`](schema::AsyncList::count)
    /// - [`self.collection::<Collection>().list().count()`](super::AsyncList::count)
    async fn count<'id, C, R, PrimaryKey>(&self, ids: R) -> Result<u64, Error>
    where
        C: schema::Collection,
        R: Into<RangeRef<'id, C::PrimaryKey, PrimaryKey>> + Send,
        PrimaryKey: for<'k> KeyEncoding<'k, C::PrimaryKey> + PartialEq + 'id + ?Sized,
        C::PrimaryKey: Borrow<PrimaryKey> + PartialEq<PrimaryKey>,
    {
        self.count_from_collection(
            ids.into().map_result(|id| DocumentId::new(id))?,
            &C::collection_name(),
        )
        .await
    }

    /// Removes a `Document` from the database.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`CollectionDocument::delete_async()`]
    /// - [`self.collection::<Collection>().delete()`](super::AsyncCollection::delete)
    async fn delete<C: schema::Collection, H: HasHeader + Send + Sync>(
        &self,
        doc: &H,
    ) -> Result<(), Error> {
        let results = self
            .apply_transaction(Transaction::delete(C::collection_name(), doc.header()?))
            .await?;
        if let OperationResult::DocumentDeleted { .. } = &results[0] {
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }
    /// Queries for view entries matching [`View`](schema::View)(super::AsyncView).
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`View::entries(self).query()`](super::AsyncView::query)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    async fn query<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<ViewMappings<V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        let mappings = self
            .query_by_name(
                &view.view_name(),
                key.map(|key| key.serialized()).transpose()?,
                order,
                limit,
                access_policy,
            )
            .await?;
        mappings
            .into_iter()
            .map(|mapping| {
                Ok(Map {
                    key: <V::Key as key::Key>::from_ord_bytes(&mapping.key)
                        .map_err(view::Error::key_serialization)
                        .map_err(Error::from)?,
                    value: V::deserialize(&mapping.value)?,
                    source: mapping.source,
                })
            })
            .collect::<Result<Vec<_>, Error>>()
    }

    /// Queries for view entries matching [`View`](schema::View) with their source documents.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`View::entries(self).query_with_docs()`](super::AsyncView::query_with_docs) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`AsyncConnection::view()`](super::AsyncConnection::view).
    #[must_use]
    async fn query_with_docs<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        // Query permission is checked by the query call
        let results = self
            .query::<V, Key>(key, order, limit, access_policy)
            .await?;

        // Verify that there is permission to fetch each document
        let documents = self
            .get_multiple::<V::Collection, _, _, _>(results.iter().map(|m| &m.source.id))
            .await?
            .into_iter()
            .map(|doc| (doc.header.id.clone(), doc))
            .collect::<BTreeMap<_, _>>();

        Ok(MappedDocuments {
            mappings: results,
            documents,
        })
    }

    /// Queries for view entries matching [`View`](schema::View) with their source documents,
    /// deserialized.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using
    /// [`View::entries(self).query_with_collection_docs()`](super::AsyncView::query_with_collection_docs)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    #[must_use]
    async fn query_with_collection_docs<V, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<CollectionDocument<V::Collection>, V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
        V: schema::SerializedView,
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
    {
        let mapped_docs = self
            .query_with_docs::<V, Key>(key, order, limit, access_policy)
            .await?;
        let mut collection_docs = BTreeMap::new();
        for (id, doc) in mapped_docs.documents {
            collection_docs.insert(id, CollectionDocument::<V::Collection>::try_from(&doc)?);
        }
        Ok(MappedDocuments {
            mappings: mapped_docs.mappings,
            documents: collection_docs,
        })
    }

    /// Reduces the view entries matching [`View`](schema::View).
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using
    /// [`View::entries(self).reduce()`](super::AsyncView::reduce)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    #[must_use]
    async fn reduce<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        self.reduce_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
        )
        .await
        .and_then(|value| V::deserialize(&value))
    }

    /// Reduces the view entries matching [`View`](schema::View), reducing the values by each
    /// unique key.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using
    /// [`View::entries(self).reduce_grouped()`](super::AsyncView::reduce_grouped)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    #[must_use]
    async fn reduce_grouped<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        access_policy: AccessPolicy,
    ) -> Result<GroupedReductions<V>, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        self.reduce_grouped_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
        )
        .await?
        .into_iter()
        .map(|map| {
            Ok(MappedValue::new(
                V::Key::from_ord_bytes(&map.key).map_err(view::Error::key_serialization)?,
                V::deserialize(&map.value)?,
            ))
        })
        .collect::<Result<Vec<_>, Error>>()
    }

    /// Deletes all of the documents associated with this view.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using
    /// [`View::entries(self).delete_docs()`](super::AsyncView::delete_docs)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    #[must_use]
    async fn delete_docs<V: schema::SerializedView, Key>(
        &self,
        key: Option<QueryKey<'_, V::Key, Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>
    where
        Key: for<'k> KeyEncoding<'k, V::Key> + PartialEq + ?Sized,
        V::Key: Borrow<Key> + PartialEq<Key>,
    {
        let view = self.schematic().view::<V>()?;
        self.delete_docs_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
        )
        .await
    }

    /// Applies a [`Transaction`] to the [`Schema`](schema::Schema). If any
    /// operation in the [`Transaction`] fails, none of the operations will be
    /// applied to the [`Schema`](schema::Schema).
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, Error>;

    /// Retrieves the document with `id` stored within the named `collection`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::get_async()`]
    /// - [`self.collection::<Collection>().get()`](super::AsyncCollection::get)
    async fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, Error>;

    /// Retrieves all documents matching `ids` from the named `collection`.
    /// Documents that are not found are not returned, but no error will be
    /// generated.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::get_multiple_async()`]
    /// - [`self.collection::<Collection>().get_multiple()`](super::AsyncCollection::get_multiple)
    async fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    /// Retrieves all documents within the range of `ids` from the named
    /// `collection`. To retrieve all documents, pass in `..` for `ids`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all().headers()`](schema::List::headers)
    /// - [`self.collection::<Collection>().all().headers()`](super::List::headers)
    /// - [`SerializedCollection::list().headers()`](schema::List::headers)
    /// - [`self.collection::<Collection>().list().headers()`](super::List::headers)
    async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    /// Retrieves all headers within the range of `ids` from the named
    /// `collection`. To retrieve all documents, pass in `..` for `ids`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all().headers()`](schema::AsyncList::headers)
    /// - [`self.collection::<Collection>().all().headers()`](super::AsyncList::headers)
    /// - [`SerializedCollection::list().headers()`](schema::AsyncList::headers)
    /// - [`self.collection::<Collection>().list().headers()`](super::AsyncList::headers)
    async fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, Error>;

    /// Counts the number of documents within the range of `ids` from the named
    /// `collection`.
    ///
    /// This is a lower-level API. For better ergonomics, consider using one of:
    ///
    /// - [`SerializedCollection::all_async().count()`](schema::AsyncList::count)
    /// - [`self.collection::<Collection>().all().count()`](super::AsyncList::count)
    /// - [`SerializedCollection::list_async().count()`](schema::AsyncList::count)
    /// - [`self.collection::<Collection>().list().count()`](super::AsyncList::count)
    async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, Error>;

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
    /// * [`Error::Other`]: an error occurred while compacting the database.
    async fn compact_collection_by_name(&self, collection: CollectionName) -> Result<(), Error>;

    /// Queries for view entries from the named `view`.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`View::entries(self).query()`](super::AsyncView::query)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, Error>;

    /// Queries for view entries from the named `view` with their source
    /// documents.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`View::entries(self).query_with_docs()`](super::AsyncView::query_with_docs) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`AsyncConnection::view()`](super::AsyncConnection::view).
    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, Error>;

    /// Reduces the view entries from the named `view`.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using
    /// [`View::entries(self).reduce()`](super::AsyncView::reduce)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, Error>;

    /// Reduces the view entries from the named `view`, reducing the values by each
    /// unique key.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using
    /// [`View::entries(self).reduce_grouped()`](super::AsyncView::reduce_grouped)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, Error>;

    /// Deletes all source documents for entries that match within the named
    /// `view`.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using
    /// [`View::entries(self).delete_docs()`](super::AsyncView::delete_docs)
    /// instead. The parameters for the query can be customized on the builder
    /// returned from [`AsyncConnection::view()`](super::AsyncConnection::view).
    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>;
}

/// Access to a connection's schema.
pub trait HasSchema {
    /// Returns the schema for the database.
    fn schematic(&self) -> &Schematic;
}
