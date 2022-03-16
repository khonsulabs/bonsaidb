use std::collections::BTreeMap;

use arc_bytes::serde::Bytes;
use async_trait::async_trait;

use crate::{
    connection::{AccessPolicy, QueryKey, Range, Sort},
    document::{
        AnyDocumentId, CollectionDocument, CollectionHeader, Document, DocumentId, HasHeader,
        OwnedDocument,
    },
    key::Key,
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

pub trait LowLevelConnection {
    fn schematic(&self) -> &Schematic;

    /// Inserts a newly created document into the connected [`schema::Schema`]
    /// for the [`Collection`] `C`. If `id` is `None` a unique id will be
    /// generated. If an id is provided and a document already exists with that
    /// id, a conflict error will be returned.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::push_into()`]
    /// - [`SerializedCollection::insert_into()`]
    /// - [`self.collection::<Collection>().insert()`](Collection::insert)
    /// - [`self.collection::<Collection>().push()`](Collection::push)
    fn insert<
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
        B: Into<Bytes> + Send,
    >(
        &self,
        id: Option<PrimaryKey>,
        contents: B,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error> {
        let contents = contents.into();
        let results = self.apply_transaction(Transaction::insert(
            C::collection_name(),
            id.map(|id| id.into().to_document_id()).transpose()?,
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
    /// [`Collection`] `C`. Upon success, `doc.revision` will be updated with
    /// the new revision.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`CollectionDocument::update()`]
    /// - [`self.collection::<Collection>().update()`](Collection::update)
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
    /// - [`self.collection::<Collection>().overwrite()`](Collection::overwrite)
    fn overwrite<C, PrimaryKey>(
        &self,
        id: PrimaryKey,
        contents: Vec<u8>,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error>
    where
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        let results = self.apply_transaction(Transaction::overwrite(
            C::collection_name(),
            id.into().to_document_id()?,
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

    /// Retrieves a stored document from [`Collection`] `C` identified by `id`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::get()`]
    /// - [`self.collection::<Collection>().get()`](Collection::get)
    fn get<C, PrimaryKey>(&self, id: PrimaryKey) -> Result<Option<OwnedDocument>, Error>
    where
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.get_from_collection(id.into().to_document_id()?, &C::collection_name())
    }

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::get_multiple()`]
    /// - [`self.collection::<Collection>().get_multiple()`](Collection::get_multiple)
    fn get_multiple<C, PrimaryKey, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        DocumentIds: IntoIterator<Item = PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PrimaryKey> + Send + Sync,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send + Sync,
    {
        let ids = ids
            .into_iter()
            .map(|id| id.into().to_document_id())
            .collect::<Result<Vec<_>, _>>()?;
        self.get_multiple_from_collection(&ids, &C::collection_name())
    }

    /// Retrieves all documents within the range of `ids`. To retrieve all
    /// documents, pass in `..` for `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using one
    /// of:
    ///
    /// - [`SerializedCollection::all()`]
    /// - [`self.collection::<Collection>().all()`](Collection::all)
    /// - [`SerializedCollection::list()`]
    /// - [`self.collection::<Collection>().list()`](Collection::list)
    fn list<C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        let ids = ids.into().map_result(|id| id.into().to_document_id())?;
        self.list_from_collection(ids, order, limit, &C::collection_name())
    }

    /// Counts the number of documents within the range of `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::all().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().all().count()`](List::count)
    /// - [`SerializedCollection::list().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().list().count()`](List::count)
    fn count<C, R, PrimaryKey>(&self, ids: R) -> Result<u64, Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.count_from_collection(
            ids.into().map_result(|key| key.into().to_document_id())?,
            &C::collection_name(),
        )
    }

    /// Removes a `Document` from the database.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`CollectionDocument::delete()`]
    /// - [`self.collection::<Collection>().delete()`](Collection::delete)
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

    /// Queries for view entries matching [`View`].
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().query()`](View::query) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    fn query<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, Error> {
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
                    key: <V::Key as Key>::from_ord_bytes(&mapping.key)
                        .map_err(view::Error::key_serialization)
                        .map_err(Error::from)?,
                    value: V::deserialize(&mapping.value)?,
                    source: mapping.source,
                })
            })
            .collect::<Result<Vec<_>, Error>>()
    }

    /// Queries for view entries matching [`View`] with their source documents.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().query_with_docs()`](View::query_with_docs) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    fn query_with_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, Error> {
        // Query permission is checked by the query call
        let results = self.query::<V>(key, order, limit, access_policy)?;

        // Verify that there is permission to fetch each document
        let mut ids = Vec::with_capacity(results.len());
        ids.extend(results.iter().map(|m| m.source.id));

        let documents = self
            .get_multiple::<V::Collection, _, _, _>(ids)?
            .into_iter()
            .map(|doc| (doc.header.id, doc))
            .collect::<BTreeMap<_, _>>();

        Ok(MappedDocuments {
            mappings: results,
            documents,
        })
    }

    /// Queries for view entries matching [`View`] with their source documents, deserialized.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().query_with_collection_docs()`](View::query_with_collection_docs) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    fn query_with_collection_docs<V>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<CollectionDocument<V::Collection>, V>, Error>
    where
        V: schema::SerializedView,
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
    {
        let mapped_docs = self.query_with_docs::<V>(key, order, limit, access_policy)?;
        let mut collection_docs = BTreeMap::new();
        for (id, doc) in mapped_docs.documents {
            collection_docs.insert(id, CollectionDocument::<V::Collection>::try_from(&doc)?);
        }
        Ok(MappedDocuments {
            mappings: mapped_docs.mappings,
            documents: collection_docs,
        })
    }

    /// Reduces the view entries matching [`View`].
    ///
    /// This is the lower-level API. For better ergonomics, consider reducing
    /// the view using [`self.view::<View>().reduce()`](View::reduce) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    fn reduce<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, Error> {
        let view = self.schematic().view::<V>()?;
        self.reduce_by_name(
            &view.view_name(),
            key.map(|key| key.serialized()).transpose()?,
            access_policy,
        )
        .and_then(|value| V::deserialize(&value))
    }

    /// Reduces the view entries matching [`View`], reducing the values by each
    /// unique key.
    ///
    /// This is the lower-level API. For better ergonomics, consider reducing
    /// the view using
    /// [`self.view::<View>().reduce_grouped()`](View::reduce_grouped) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    fn reduce_grouped<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, Error> {
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
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().delete_docs()`](View::delete_docs()) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    fn delete_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error> {
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

    fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, Error>;

    fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, Error>;

    fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    fn compact_collection_by_name(&self, collection: CollectionName) -> Result<(), Error>;

    fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, Error>;

    fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, Error>;

    fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, Error>;

    fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, Error>;

    fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>;
}

#[async_trait]
pub trait AsyncLowLevelConnection {
    /// Inserts a newly created document into the connected [`schema::Schema`]
    /// for the [`Collection`] `C`. If `id` is `None` a unique id will be
    /// generated. If an id is provided and a document already exists with that
    /// id, a conflict error will be returned.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::push_into()`]
    /// - [`SerializedCollection::insert_into()`]
    /// - [`self.collection::<Collection>().insert()`](Collection::insert)
    /// - [`self.collection::<Collection>().push()`](Collection::push)
    async fn insert<
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
        B: Into<Bytes> + Send,
    >(
        &self,
        id: Option<PrimaryKey>,
        contents: B,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error> {
        let contents = contents.into();
        let results = self
            .apply_transaction(Transaction::insert(
                C::collection_name(),
                id.map(|id| id.into().to_document_id()).transpose()?,
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
    /// [`Collection`] `C`. Upon success, `doc.revision` will be updated with
    /// the new revision.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`CollectionDocument::update()`]
    /// - [`self.collection::<Collection>().update()`](Collection::update)
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
    /// - [`SerializedCollection::overwrite()`]
    /// - [`SerializedCollection::overwrite_into()`]
    /// - [`self.collection::<Collection>().overwrite()`](Collection::overwrite)
    async fn overwrite<'a, C, PrimaryKey>(
        &self,
        id: PrimaryKey,
        contents: Vec<u8>,
    ) -> Result<CollectionHeader<C::PrimaryKey>, Error>
    where
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        let results = self
            .apply_transaction(Transaction::overwrite(
                C::collection_name(),
                id.into().to_document_id()?,
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

    /// Retrieves a stored document from [`Collection`] `C` identified by `id`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::get()`]
    /// - [`self.collection::<Collection>().get()`](Collection::get)
    async fn get<C, PrimaryKey>(&self, id: PrimaryKey) -> Result<Option<OwnedDocument>, Error>
    where
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send;

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::get_multiple()`]
    /// - [`self.collection::<Collection>().get_multiple()`](Collection::get_multiple)
    async fn get_multiple<C, PrimaryKey, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        DocumentIds: IntoIterator<Item = PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PrimaryKey> + Send + Sync,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send + Sync;

    /// Retrieves all documents within the range of `ids`. To retrieve all
    /// documents, pass in `..` for `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using one
    /// of:
    ///
    /// - [`SerializedCollection::all()`]
    /// - [`self.collection::<Collection>().all()`](Collection::all)
    /// - [`SerializedCollection::list()`]
    /// - [`self.collection::<Collection>().list()`](Collection::list)
    async fn list<C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<OwnedDocument>, Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send;

    /// Counts the number of documents within the range of `ids`.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`SerializedCollection::all().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().all().count()`](List::count)
    /// - [`SerializedCollection::list().count()`](schema::List::count)
    /// - [`self.collection::<Collection>().list().count()`](List::count)
    async fn count<C, R, PrimaryKey>(&self, ids: R) -> Result<u64, Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send;

    /// Removes a `Document` from the database.
    ///
    /// This is the lower-level API. For better ergonomics, consider using
    /// one of:
    ///
    /// - [`CollectionDocument::delete()`]
    /// - [`self.collection::<Collection>().delete()`](Collection::delete)
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
    /// Queries for view entries matching [`View`].
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().query()`](View::query) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    async fn query<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, Error>;

    /// Queries for view entries matching [`View`] with their source documents.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().query_with_docs()`](View::query_with_docs) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    #[must_use]
    async fn query_with_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, Error>;

    /// Queries for view entries matching [`View`] with their source documents, deserialized.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().query_with_collection_docs()`](View::query_with_collection_docs) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    #[must_use]
    async fn query_with_collection_docs<V>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<CollectionDocument<V::Collection>, V>, Error>
    where
        V: schema::SerializedView,
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
    {
        let mapped_docs = self
            .query_with_docs::<V>(key, order, limit, access_policy)
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

    /// Reduces the view entries matching [`View`].
    ///
    /// This is the lower-level API. For better ergonomics, consider reducing
    /// the view using [`self.view::<View>().reduce()`](View::reduce) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    #[must_use]
    async fn reduce<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, Error>;

    /// Reduces the view entries matching [`View`], reducing the values by each
    /// unique key.
    ///
    /// This is the lower-level API. For better ergonomics, consider reducing
    /// the view using
    /// [`self.view::<View>().reduce_grouped()`](View::reduce_grouped) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    #[must_use]
    async fn reduce_grouped<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, Error>;

    /// Deletes all of the documents associated with this view.
    ///
    /// This is the lower-level API. For better ergonomics, consider querying
    /// the view using [`self.view::<View>().delete_docs()`](View::delete_docs()) instead.
    /// The parameters for the query can be customized on the builder returned
    /// from [`Self::view()`].
    #[must_use]
    async fn delete_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>;

    /// Applies a [`Transaction`] to the [`schema::Schema`]. If any operation in the
    /// [`Transaction`] fails, none of the operations will be applied to the
    /// [`schema::Schema`].
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, Error>;

    async fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, Error>;

    async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, Error>;

    async fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, Error>;

    async fn compact_collection_by_name(&self, collection: CollectionName) -> Result<(), Error>;

    async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, Error>;

    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, Error>;

    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, Error>;

    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, Error>;

    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>;
}
