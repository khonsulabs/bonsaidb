use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    fmt::Debug,
    ops::Deref,
};

use async_trait::async_trait;
use futures::{future::BoxFuture, Future, FutureExt};
use serde::{Deserialize, Serialize};

use super::names::InvalidNameError;
use crate::{
    connection::Connection,
    document::{Document, Header, KeyId},
    schema::{CollectionName, Schematic},
    Error,
};

/// A namespaced collection of `Document<Self>` items and views.
#[async_trait]
pub trait Collection: Debug + Send + Sync {
    /// The `Id` of this collection.
    fn collection_name() -> Result<CollectionName, InvalidNameError>;

    /// Defines all `View`s in this collection in `schema`.
    fn define_views(schema: &mut Schematic) -> Result<(), Error>;

    /// If a [`KeyId`] is returned, this collection will be stored encrypted
    /// at-rest using the key specified.
    #[must_use]
    fn encryption_key() -> Option<KeyId> {
        None
    }

    /// Returns the serializer to use when accessing and storing the document's
    /// contents. If you only interact with a document's bytes directly, this
    /// has no effect.
    #[must_use]
    fn serializer() -> CollectionSerializer {
        CollectionSerializer::default()
    }

    /// Gets a [`CollectionDocument`] with `id` from `connection`.
    async fn get<C: Connection>(
        id: u64,
        connection: &C,
    ) -> Result<Option<CollectionDocument<Self>>, Error>
    where
        Self: Serialize + for<'de> Deserialize<'de>,
    {
        let possible_doc = connection.get::<Self>(id).await?;
        Ok(possible_doc.map(|doc| doc.try_into()).transpose()?)
    }

    /// Inserts this value into the collection, returning the created document.
    async fn insert_into<Cn: Connection>(
        self,
        connection: &Cn,
    ) -> Result<CollectionDocument<Self>, InsertError<Self>>
    where
        Self: Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let header = match connection.collection::<Self>().push(&self).await {
            Ok(header) => header,
            Err(error) => {
                return Err(InsertError {
                    contents: self,
                    error,
                })
            }
        };
        Ok(CollectionDocument {
            header,
            contents: self,
        })
    }
}

/// Serialization format for storing a collection.
#[derive(Debug)]
pub enum CollectionSerializer {
    /// Serialize using the [`Pot`](https://github.com/khonsulabs/pot) format. The default serializer.
    Pot,
    /// Serialize using Json. Requires feature `json`.
    #[cfg(feature = "json")]
    Json,
    /// Serialize using [Cbor](https://github.com/pyfisch/cbor). Requires feature `cbor`.
    #[cfg(feature = "cbor")]
    Cbor,
    /// Serialize using [Bincode](https://github.com/bincode-org/bincode). Requires feature `bincode`.
    #[cfg(feature = "bincode")]
    Bincode,
}

impl Default for CollectionSerializer {
    fn default() -> Self {
        Self::Pot
    }
}

impl CollectionSerializer {
    /// Serializes `contents`.
    pub fn serialize<T: Serialize>(&self, contents: &T) -> Result<Vec<u8>, Error> {
        match self {
            CollectionSerializer::Pot => pot::to_vec(contents).map_err(crate::Error::from),
            #[cfg(feature = "json")]
            CollectionSerializer::Json => serde_json::to_vec(contents).map_err(crate::Error::from),
            #[cfg(feature = "cbor")]
            CollectionSerializer::Cbor => serde_cbor::to_vec(contents).map_err(crate::Error::from),
            #[cfg(feature = "bincode")]
            CollectionSerializer::Bincode => {
                bincode::serialize(contents).map_err(crate::Error::from)
            }
        }
    }
}

/// An error from inserting a [`CollectionDocument`].
#[derive(thiserror::Error, Debug)]
#[error("{error}")]
pub struct InsertError<T> {
    /// The original value being inserted.
    pub contents: T,
    /// The error that occurred while inserting.
    pub error: Error,
}

/// A collection with a unique name column.
#[async_trait]
pub trait NamedCollection: Collection {
    /// The name view defined for the collection.
    type ByNameView: crate::schema::View<Key = String>;

    /// Gets a [`CollectionDocument`] with `id` from `connection`.
    async fn load<'name, N: Into<NamedReference<'name>> + Send + Sync, C: Connection>(
        id: N,
        connection: &C,
    ) -> Result<Option<CollectionDocument<Self>>, Error>
    where
        Self: Serialize + for<'de> Deserialize<'de>,
    {
        let possible_doc = Self::load_document(id, connection).await?;
        Ok(possible_doc.map(|doc| doc.try_into()).transpose()?)
    }

    /// Gets a [`CollectionDocument`] with `id` from `connection`.
    fn entry<'connection, 'name, N: Into<NamedReference<'name>> + Send + Sync, C: Connection>(
        id: N,
        connection: &'connection C,
    ) -> Entry<'connection, 'name, C, Self>
    where
        Self: Serialize + for<'de> Deserialize<'de>,
    {
        let name = id.into();
        Entry {
            state: EntryState::Pending(Some(EntryBuilder {
                name,
                connection,
                insert: None,
                update: None,
                retry_limit: 0,
            })),
        }
    }

    /// Loads a document from this collection by name, if applicable. Return
    /// `Ok(None)` if unsupported.
    #[allow(unused_variables)]
    async fn load_document<'name, N: Into<NamedReference<'name>> + Send + Sync, C: Connection>(
        name: N,
        connection: &C,
    ) -> Result<Option<Document<'static>>, Error>
    where
        Self: Serialize + for<'de> Deserialize<'de>,
    {
        match name.into() {
            NamedReference::Id(id) => connection.get::<Self>(id).await,
            NamedReference::Name(name) => Ok(connection
                .view::<Self::ByNameView>()
                .with_key(name.as_ref().to_owned())
                .query_with_docs()
                .await?
                .into_iter()
                .next()
                .map(|entry| entry.document)),
        }
    }
}

/// A document with serializable contents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDocument<C: Collection + Serialize + for<'de> Deserialize<'de>> {
    /// The header of the document, which contains the id and `Revision`.
    pub header: Header,

    /// The document's contents.
    pub contents: C,
}

impl<'a, C: Collection + Serialize + for<'de> Deserialize<'de>> Deref for CollectionDocument<C> {
    type Target = Header;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl<'b, 'a, C> TryFrom<&'b Document<'a>> for CollectionDocument<C>
where
    C: Collection + Serialize + for<'de> Deserialize<'de>,
{
    type Error = Error;

    fn try_from(value: &'b Document<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            contents: value.contents::<C>()?,
            header: value.header.clone(),
        })
    }
}

impl<C> TryFrom<Document<'static>> for CollectionDocument<C>
where
    C: Collection + Serialize + for<'de> Deserialize<'de>,
{
    type Error = Error;

    fn try_from(value: Document<'static>) -> Result<Self, Self::Error> {
        Ok(Self {
            contents: value.contents::<C>()?,
            header: value.header,
        })
    }
}

impl<'a, C> TryFrom<CollectionDocument<C>> for Document<'a>
where
    C: Collection + Serialize + for<'de> Deserialize<'de>,
{
    type Error = pot::Error;

    fn try_from(value: CollectionDocument<C>) -> Result<Self, Self::Error> {
        Ok(Self {
            contents: Cow::Owned(pot::to_vec(&value.contents)?),
            header: value.header,
        })
    }
}

impl<C> CollectionDocument<C>
where
    C: Collection + Serialize + for<'de> Deserialize<'de>,
{
    /// Stores the new value of `contents` in the document.
    pub async fn update<Cn: Connection>(&mut self, connection: &Cn) -> Result<(), Error> {
        let mut doc = self.to_document()?;

        connection.update::<C>(&mut doc).await?;

        self.header = doc.header;

        Ok(())
    }

    /// Removes the document from the collection.
    pub async fn delete<Cn: Connection>(&self, connection: &Cn) -> Result<(), Error> {
        let doc = self.to_document()?;

        connection.delete::<C>(&doc).await?;

        Ok(())
    }

    /// Converts this value to a serialized `Document`.
    pub fn to_document(&self) -> Result<Document<'static>, Error> {
        Ok(Document {
            contents: Cow::Owned(pot::to_vec(&self.contents)?),
            header: self.header.clone(),
        })
    }
}

/// A reference to a collection that has a unique name view.
#[derive(Clone, PartialEq, Deserialize, Serialize, Debug)]
#[must_use]
pub enum NamedReference<'a> {
    /// An entity's name.
    Name(Cow<'a, str>),
    /// A document id.
    Id(u64),
}

impl<'a> From<&'a str> for NamedReference<'a> {
    fn from(name: &'a str) -> Self {
        Self::Name(Cow::Borrowed(name))
    }
}

impl<'a> From<&'a String> for NamedReference<'a> {
    fn from(name: &'a String) -> Self {
        Self::Name(Cow::Borrowed(name.as_str()))
    }
}

impl<'a, 'b, 'c> From<&'b Document<'c>> for NamedReference<'a> {
    fn from(doc: &'b Document<'c>) -> Self {
        Self::Id(doc.header.id)
    }
}

impl<'a, 'b, C: Collection + Serialize + for<'de> Deserialize<'de>> From<&'b CollectionDocument<C>>
    for NamedReference<'a>
{
    fn from(doc: &'b CollectionDocument<C>) -> Self {
        Self::Id(doc.header.id)
    }
}

impl<'a> From<String> for NamedReference<'a> {
    fn from(name: String) -> Self {
        Self::Name(Cow::Owned(name))
    }
}

impl<'a> From<u64> for NamedReference<'a> {
    fn from(id: u64) -> Self {
        Self::Id(id)
    }
}

impl<'a> NamedReference<'a> {
    /// Converts this reference to an owned reference with a `'static` lifetime.
    pub fn into_owned(self) -> NamedReference<'static> {
        match self {
            Self::Name(name) => NamedReference::Name(match name {
                Cow::Owned(string) => Cow::Owned(string),
                Cow::Borrowed(borrowed) => Cow::Owned(borrowed.to_owned()),
            }),
            Self::Id(id) => NamedReference::Id(id),
        }
    }

    /// Returns this reference's id. If the reference is a name, the
    /// [`NamedCollection::ByNameView`] is queried for the id.
    pub async fn id<Col: NamedCollection, Cn: Connection>(
        &self,
        connection: &Cn,
    ) -> Result<Option<u64>, Error> {
        match self {
            Self::Name(name) => Ok(connection
                .view::<Col::ByNameView>()
                .with_key(name.as_ref().to_owned())
                .query()
                .await?
                .into_iter()
                .next()
                .map(|e| e.source.id)),
            Self::Id(id) => Ok(Some(*id)),
        }
    }
}

/// A future that resolves to an entry in a [`NamedCollection`].
#[must_use]
pub struct Entry<'a, 'name, Connection, Col>
where
    Col: NamedCollection + Serialize + for<'de> Deserialize<'de>,
{
    state: EntryState<'a, 'name, Connection, Col>,
}

struct EntryBuilder<'a, 'name, Connection, Col> {
    name: NamedReference<'name>,
    connection: &'a Connection,
    insert: Option<Box<dyn EntryInsert<Col>>>,
    update: Option<Box<dyn EntryUpdate<Col>>>,
    retry_limit: usize,
}

impl<'a, 'name, Connection, Col> Entry<'a, 'name, Connection, Col>
where
    Col: NamedCollection + Serialize + for<'de> Deserialize<'de> + 'a,
{
    fn pending(&mut self) -> &mut EntryBuilder<'a, 'name, Connection, Col> {
        match &mut self.state {
            EntryState::Pending(pending) => pending.as_mut().unwrap(),
            EntryState::Executing(_) => unreachable!(),
        }
    }

    /// If an entry with the key doesn't exist, `cb` will be executed to provide
    /// an initial document. This document will be saved before being returned.
    pub fn or_insert_with<F: FnOnce() -> Col + Send + 'static>(mut self, cb: F) -> Self {
        self.pending().insert = Some(Box::new(Some(cb)));
        self
    }

    /// If an entry with the keys exists, `cb` will be executed with the stored
    /// value, allowing an opportunity to update the value. This new value will
    /// be saved to the database before returning. If an error occurs during
    /// update, `cb` may be invoked multiple times, up to the
    /// [`retry_limit`](Self::retry_limit()).
    pub fn update_with<F: Fn(&mut Col) + Send + 'static>(mut self, cb: F) -> Self {
        self.pending().update = Some(Box::new(cb));
        self
    }

    /// The number of attempts to attempt updating the document using
    /// `update_with` before returning an error.
    pub fn retry_limit(mut self, attempts: usize) -> Self {
        self.pending().retry_limit = attempts;
        self
    }
}

pub trait EntryInsert<Col>: Send {
    fn call(&mut self) -> Col;
}

impl<F, Col> EntryInsert<Col> for Option<F>
where
    F: FnOnce() -> Col + Send,
{
    fn call(&mut self) -> Col {
        self.take().unwrap()()
    }
}

pub trait EntryUpdate<Col>: Send {
    fn call(&self, doc: &mut Col);
}

impl<'a, F, Col> EntryUpdate<Col> for F
where
    F: Fn(&mut Col) + Send,
    Col: NamedCollection + Serialize + for<'de> Deserialize<'de> + 'a,
{
    fn call(&self, doc: &mut Col) {
        self(doc);
    }
}

impl<'a, 'name, Conn, Col> Future for Entry<'a, 'name, Conn, Col>
where
    Col: NamedCollection + Serialize + for<'de> Deserialize<'de> + 'static,
    Conn: Connection,
    'name: 'a,
{
    type Output = Result<Option<CollectionDocument<Col>>, Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.state {
            EntryState::Executing(future) => future.as_mut().poll(cx),
            EntryState::Pending(builder) => {
                let EntryBuilder {
                    name,
                    connection,
                    insert,
                    update,
                    mut retry_limit,
                } = builder.take().expect("expected builder to have options");
                let future = async move {
                    if let Some(mut existing) = Col::load(name, connection).await? {
                        if let Some(update) = update {
                            loop {
                                update.call(&mut existing.contents);
                                match existing.update(connection).await {
                                    Ok(()) => return Ok(Some(existing)),
                                    Err(Error::DocumentConflict(collection, id)) => {
                                        // Another client has updated the document underneath us.
                                        if retry_limit > 0 {
                                            retry_limit -= 1;
                                            existing = match Col::load(id, connection).await? {
                                                Some(doc) => doc,
                                                // Another client deleted the document before we could reload it.
                                                None => break Ok(None),
                                            }
                                        } else {
                                            break Err(Error::DocumentConflict(collection, id));
                                        }
                                    }
                                    Err(other) => break Err(other),
                                }
                            }
                        } else {
                            Ok(Some(existing))
                        }
                    } else if let Some(mut insert) = insert {
                        let new_document = insert.call();
                        Ok(Some(new_document.insert_into(connection).await?))
                    } else {
                        Ok(None)
                    }
                }
                .boxed();

                self.state = EntryState::Executing(future);
                self.poll(cx)
            }
        }
    }
}

enum EntryState<'a, 'name, Connection, Col>
where
    Col: NamedCollection + Serialize + for<'de> Deserialize<'de> + 'a,
{
    Pending(Option<EntryBuilder<'a, 'name, Connection, Col>>),
    Executing(BoxFuture<'a, Result<Option<CollectionDocument<Col>>, Error>>),
}
