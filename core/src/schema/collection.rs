use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    fmt::Debug,
};

use async_trait::async_trait;
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
    ) -> Result<CollectionDocument<Self>, Error>
    where
        Self: Serialize + for<'de> Deserialize<'de> + 'static,
    {
        let header = connection.collection::<Self>().push(&self).await?;
        Ok(CollectionDocument {
            header: Cow::Owned(header),
            contents: self,
        })
    }
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
#[derive(Clone, Debug)]
pub struct CollectionDocument<C: Collection + Serialize + for<'de> Deserialize<'de>> {
    /// The header of the document, which contains the id and `Revision`.
    pub header: Cow<'static, Header>,

    /// The document's contents.
    pub contents: C,
}

impl<C> TryFrom<Document<'static>> for CollectionDocument<C>
where
    C: Collection + Serialize + for<'de> Deserialize<'de>,
{
    type Error = serde_cbor::Error;

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
    type Error = serde_cbor::Error;

    fn try_from(value: CollectionDocument<C>) -> Result<Self, Self::Error> {
        Ok(Self {
            contents: Cow::Owned(serde_cbor::to_vec(&value.contents)?),
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
            contents: Cow::Owned(serde_cbor::to_vec(&self.contents)?),
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
                .map(|e| e.source)),
            Self::Id(id) => Ok(Some(*id)),
        }
    }
}
