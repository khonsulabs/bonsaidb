use std::borrow::Cow;

use arc_bytes::serde::{Bytes, CowBytes};
use serde::{Deserialize, Serialize};

use crate::schema::{Collection, SerializedCollection};

mod collection;
mod header;
mod id;
mod revision;
pub use self::{
    collection::{CollectionDocument, OwnedDocuments},
    header::{AnyHeader, CollectionHeader, Emit, HasHeader, Header},
    id::{AnyDocumentId, DocumentId, InvalidHexadecimal},
    revision::Revision,
};
/// Contains a serialized document in the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BorrowedDocument<'a> {
    /// The header of the document, which contains the id and `Revision`.
    pub header: Header,

    /// The serialized bytes of the stored item.
    #[serde(borrow)]
    pub contents: CowBytes<'a>,
}

/// Contains a serialized document in the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnedDocument {
    /// The header of the document, which contains the id and `Revision`.
    pub header: Header,

    /// The serialized bytes of the stored item.
    pub contents: Bytes,
}

/// Common interface of a document in BonsaiDb.
pub trait Document<C>: Sized
where
    C: Collection,
{
    /// The bytes type used in the interface.
    type Bytes;

    /// Returns the unique key for this document.
    fn key(&self) -> AnyDocumentId<C::PrimaryKey>;
    /// Returns the header of this document.
    fn header(&self) -> AnyHeader<C::PrimaryKey>;
    /// Sets the header to the new header.
    fn set_header(&mut self, header: Header) -> Result<(), crate::Error>;
    /// Sets the header to the new collection header.
    fn set_collection_header(
        &mut self,
        header: CollectionHeader<C::PrimaryKey>,
    ) -> Result<(), crate::Error> {
        self.set_header(Header::try_from(header)?)
    }
    /// Returns the contents of this document, serialized.
    fn bytes(&self) -> Result<Vec<u8>, crate::Error>;
    /// Retrieves `contents` through deserialization into the type `D`.
    fn contents(&self) -> Result<C::Contents, crate::Error>
    where
        C: SerializedCollection;
    /// Stores `contents` into this document.
    fn set_contents(&mut self, contents: C::Contents) -> Result<(), crate::Error>
    where
        C: SerializedCollection;
}

impl<'a> AsRef<[u8]> for BorrowedDocument<'a> {
    fn as_ref(&self) -> &[u8] {
        &self.contents
    }
}

impl<'a, C> Document<C> for BorrowedDocument<'a>
where
    C: Collection,
{
    type Bytes = CowBytes<'a>;

    fn contents(&self) -> Result<C::Contents, crate::Error>
    where
        C: SerializedCollection,
    {
        <C as SerializedCollection>::deserialize(&self.contents)
    }

    fn set_contents(&mut self, contents: C::Contents) -> Result<(), crate::Error>
    where
        C: SerializedCollection,
    {
        self.contents = CowBytes::from(<C as SerializedCollection>::serialize(&contents)?);
        Ok(())
    }

    fn header(&self) -> AnyHeader<C::PrimaryKey> {
        AnyHeader::Serialized(self.header.clone())
    }

    fn set_header(&mut self, header: Header) -> Result<(), crate::Error> {
        self.header = header;
        Ok(())
    }

    fn bytes(&self) -> Result<Vec<u8>, crate::Error> {
        Ok(self.contents.to_vec())
    }

    fn key(&self) -> AnyDocumentId<C::PrimaryKey> {
        AnyDocumentId::Serialized(self.header.id)
    }
}

impl<'a, C> Document<C> for OwnedDocument
where
    C: Collection,
{
    type Bytes = Vec<u8>;

    fn contents(&self) -> Result<C::Contents, crate::Error>
    where
        C: SerializedCollection,
    {
        <C as SerializedCollection>::deserialize(&self.contents)
    }

    fn set_contents(&mut self, contents: C::Contents) -> Result<(), crate::Error>
    where
        C: SerializedCollection,
    {
        self.contents = Bytes::from(<C as SerializedCollection>::serialize(&contents)?);
        Ok(())
    }

    fn key(&self) -> AnyDocumentId<C::PrimaryKey> {
        AnyDocumentId::Serialized(self.header.id)
    }

    fn header(&self) -> AnyHeader<C::PrimaryKey> {
        AnyHeader::Serialized(self.header.clone())
    }

    fn set_header(&mut self, header: Header) -> Result<(), crate::Error> {
        self.header = header;
        Ok(())
    }

    fn bytes(&self) -> Result<Vec<u8>, crate::Error> {
        Ok(self.contents.to_vec())
    }
}

impl AsRef<Header> for OwnedDocument {
    fn as_ref(&self) -> &Header {
        &self.header
    }
}

impl AsMut<Header> for OwnedDocument {
    fn as_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}

impl AsRef<[u8]> for OwnedDocument {
    fn as_ref(&self) -> &[u8] {
        &self.contents
    }
}

impl<'a> BorrowedDocument<'a> {
    /// Returns a new instance with the id and content bytes.
    pub fn new<Contents: Into<CowBytes<'a>>>(id: DocumentId, contents: Contents) -> Self {
        let contents = contents.into();
        let revision = Revision::new(&contents);
        Self {
            header: Header { id, revision },
            contents,
        }
    }

    /// Returns a new instance with `contents`, after serializing.
    pub fn with_contents<C>(id: C::PrimaryKey, contents: &C::Contents) -> Result<Self, crate::Error>
    where
        C: SerializedCollection,
    {
        let contents = <C as SerializedCollection>::serialize(contents)?;
        Ok(Self::new(DocumentId::new(id)?, contents))
    }

    /// Converts this document to an owned document.
    #[must_use]
    pub fn into_owned(self) -> OwnedDocument {
        OwnedDocument {
            header: self.header,
            contents: Bytes::from(self.contents),
        }
    }
}

impl<'a> AsRef<Header> for BorrowedDocument<'a> {
    fn as_ref(&self) -> &Header {
        &self.header
    }
}

impl<'a> AsMut<Header> for BorrowedDocument<'a> {
    fn as_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}

/// The ID of an encryption key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum KeyId {
    /// A key with no id.
    None,
    /// The master key of the vault.
    Master,
    /// A specific named key in the vault.
    Id(Cow<'static, str>),
}
