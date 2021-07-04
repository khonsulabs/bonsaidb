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
    document::{Document, Header},
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
