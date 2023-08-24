use std::borrow::Cow;
use std::fmt::Debug;

use arc_bytes::serde::{Bytes, CowBytes};
use serde::de::{self, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

use crate::connection::{AsyncConnection, Connection};
use crate::document::{
    BorrowedDocument, CollectionHeader, DocumentId, HasHeader, Header, OwnedDocument,
};
use crate::schema::SerializedCollection;
use crate::transaction::{Operation, Transaction};
use crate::Error;

/// A document with serializable contents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDocument<C>
where
    C: SerializedCollection,
{
    /// The header of the document, which contains the id and `Revision`.
    pub header: CollectionHeader<C::PrimaryKey>,

    /// The document's contents.
    pub contents: C::Contents,
}

impl<'a, C> TryFrom<&'a BorrowedDocument<'a>> for CollectionDocument<C>
where
    C: SerializedCollection,
{
    type Error = Error;

    fn try_from(value: &'a BorrowedDocument<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            contents: C::deserialize(&value.contents)?,
            header: CollectionHeader::try_from(value.header.clone())?,
        })
    }
}

impl<'a, C> TryFrom<&'a OwnedDocument> for CollectionDocument<C>
where
    C: SerializedCollection,
{
    type Error = Error;

    fn try_from(value: &'a OwnedDocument) -> Result<Self, Self::Error> {
        Ok(Self {
            contents: C::deserialize(&value.contents)?,
            header: CollectionHeader::try_from(value.header.clone())?,
        })
    }
}

impl<'a, 'b, C> TryFrom<&'b CollectionDocument<C>> for BorrowedDocument<'a>
where
    C: SerializedCollection,
{
    type Error = crate::Error;

    fn try_from(value: &'b CollectionDocument<C>) -> Result<Self, Self::Error> {
        Ok(Self {
            contents: CowBytes::from(C::serialize(&value.contents)?),
            header: Header::try_from(value.header.clone())?,
        })
    }
}

impl<C> CollectionDocument<C>
where
    C: SerializedCollection,
{
    /// Updates the document stored in the database with the contents of this
    /// collection document.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// if let Some(mut document) = MyCollection::get(&42, &db)? {
    ///     // ... do something `document`
    ///     document.update(&db)?;
    ///     println!(
    ///         "The document has been updated: {:?}",
    ///         document.header.revision
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn update<Cn: Connection>(&mut self, connection: &Cn) -> Result<(), Error> {
        let mut doc = self.to_document()?;

        connection.update::<C, _>(&mut doc)?;

        self.header = CollectionHeader::try_from(doc.header)?;

        Ok(())
    }

    /// Pushes an update [`Operation`] to the transaction for this document.
    ///
    /// The changes will happen once the transaction is applied.
    pub fn update_in_transaction(&self, transaction: &mut Transaction) -> Result<(), Error> {
        transaction.push(Operation::update_serialized::<C>(
            self.header.clone(),
            &self.contents,
        )?);
        Ok(())
    }

    /// Stores the new value of `contents` in the document.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// if let Some(mut document) = MyCollection::get_async(&42, &db).await? {
    ///     // modify the document
    ///     document.update_async(&db).await?;
    ///     println!("Updated revision: {:?}", document.header.revision);
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn update_async<Cn: AsyncConnection>(
        &mut self,
        connection: &Cn,
    ) -> Result<(), Error> {
        let mut doc = self.to_document()?;

        connection.update::<C, _>(&mut doc).await?;

        self.header = CollectionHeader::try_from(doc.header)?;

        Ok(())
    }

    /// Modifies `self`, automatically retrying the modification if the document
    /// has been updated on the server.
    ///
    /// ## Data loss warning
    ///
    /// If you've modified `self` before calling this function and a conflict
    /// occurs, all changes to self will be lost when the current document is
    /// fetched before retrying the process again. When you use this function,
    /// you should limit the edits to the value to within the `modifier`
    /// callback.
    pub fn modify<Cn: Connection, Modifier: FnMut(&mut Self) + Send + Sync>(
        &mut self,
        connection: &Cn,
        mut modifier: Modifier,
    ) -> Result<(), Error>
    where
        C::Contents: Clone,
    {
        let mut is_first_loop = true;
        // TODO this should have a retry-limit.
        loop {
            // On the first attempt, we want to try sending the update to the
            // database without fetching new contents. If we receive a conflict,
            // on future iterations we will first re-load the data.
            if is_first_loop {
                is_first_loop = false;
            } else {
                *self =
                    C::get(&self.header.id, connection)?.ok_or_else(|| {
                        match DocumentId::new(&self.header.id) {
                            Ok(id) => Error::DocumentNotFound(C::collection_name(), Box::new(id)),
                            Err(err) => err,
                        }
                    })?;
            }
            modifier(&mut *self);
            match self.update(connection) {
                Err(Error::DocumentConflict(..)) => {}
                other => return other,
            }
        }
    }

    /// Modifies `self`, automatically retrying the modification if the document
    /// has been updated on the server.
    ///
    /// ## Data loss warning
    ///
    /// If you've modified `self` before calling this function and a conflict
    /// occurs, all changes to self will be lost when the current document is
    /// fetched before retrying the process again. When you use this function,
    /// you should limit the edits to the value to within the `modifier`
    /// callback.
    pub async fn modify_async<Cn: AsyncConnection, Modifier: FnMut(&mut Self) + Send + Sync>(
        &mut self,
        connection: &Cn,
        mut modifier: Modifier,
    ) -> Result<(), Error>
    where
        C::Contents: Clone,
    {
        let mut is_first_loop = true;
        // TODO this should have a retry-limit.
        loop {
            // On the first attempt, we want to try sending the update to the
            // database without fetching new contents. If we receive a conflict,
            // on future iterations we will first re-load the data.
            if is_first_loop {
                is_first_loop = false;
            } else {
                *self = C::get_async(&self.header.id, connection)
                    .await?
                    .ok_or_else(|| match DocumentId::new(&self.header.id) {
                        Ok(id) => Error::DocumentNotFound(C::collection_name(), Box::new(id)),
                        Err(err) => err,
                    })?;
            }
            modifier(&mut *self);
            match self.update_async(connection).await {
                Err(Error::DocumentConflict(..)) => {}
                other => return other,
            }
        }
    }

    /// Removes the document from the collection.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::Connection;
    /// # fn test_fn<C: Connection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// if let Some(document) = MyCollection::get(&42, &db)? {
    ///     document.delete(&db)?;
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub fn delete<Cn: Connection>(&self, connection: &Cn) -> Result<(), Error> {
        connection.collection::<C>().delete(self)?;

        Ok(())
    }

    /// Removes the document from the collection.
    ///
    /// ```rust
    /// # bonsaidb_core::__doctest_prelude!();
    /// # use bonsaidb_core::connection::AsyncConnection;
    /// # fn test_fn<C: AsyncConnection>(db: C) -> Result<(), Error> {
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// if let Some(document) = MyCollection::get_async(&42, &db).await? {
    ///     document.delete_async(&db).await?;
    /// }
    /// # Ok(())
    /// # })
    /// # }
    /// ```
    pub async fn delete_async<Cn: AsyncConnection>(&self, connection: &Cn) -> Result<(), Error> {
        connection.collection::<C>().delete(self).await?;

        Ok(())
    }

    /// Pushes a delete [`Operation`] to the transaction for this document.
    ///
    /// The document will be deleted once the transaction is applied.
    pub fn delete_in_transaction(&self, transaction: &mut Transaction) -> Result<(), Error> {
        transaction.push(Operation::delete(C::collection_name(), self.header()?));
        Ok(())
    }

    /// Converts this value to a serialized `Document`.
    pub fn to_document(&self) -> Result<OwnedDocument, Error> {
        Ok(OwnedDocument {
            contents: Bytes::from(C::serialize(&self.contents)?),
            header: Header::try_from(self.header.clone())?,
        })
    }
}

impl<C> Serialize for CollectionDocument<C>
where
    C: SerializedCollection,
    C::Contents: Serialize,
    C::PrimaryKey: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("CollectionDocument", 2)?;
        s.serialize_field("header", &self.header)?;
        s.serialize_field("contents", &self.contents)?;
        s.end()
    }
}

impl<'de, C> Deserialize<'de> for CollectionDocument<C>
where
    C: SerializedCollection,
    C::PrimaryKey: Deserialize<'de>,
    C::Contents: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CollectionDocumentVisitor<C>
        where
            C: SerializedCollection,
        {
            header: Option<CollectionHeader<C::PrimaryKey>>,
            contents: Option<C::Contents>,
        }

        impl<C> Default for CollectionDocumentVisitor<C>
        where
            C: SerializedCollection,
        {
            fn default() -> Self {
                Self {
                    header: None,
                    contents: None,
                }
            }
        }

        impl<'de, C> Visitor<'de> for CollectionDocumentVisitor<C>
        where
            C: SerializedCollection,
            C::PrimaryKey: Deserialize<'de>,
            C::Contents: Deserialize<'de>,
        {
            type Value = CollectionDocument<C>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a collection document")
            }

            fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                while let Some(key) = map.next_key::<Cow<'_, str>>()? {
                    match key.as_ref() {
                        "header" => {
                            self.header = Some(map.next_value()?);
                        }
                        "contents" => {
                            self.contents = Some(map.next_value()?);
                        }
                        _ => {
                            return Err(<A::Error as de::Error>::custom(format!(
                                "unknown field {key}"
                            )))
                        }
                    }
                }

                Ok(CollectionDocument {
                    header: self
                        .header
                        .ok_or_else(|| <A::Error as de::Error>::custom("`header` missing"))?,
                    contents: self
                        .contents
                        .ok_or_else(|| <A::Error as de::Error>::custom("`contents` missing"))?,
                })
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let header = seq
                    .next_element()?
                    .ok_or_else(|| <A::Error as de::Error>::custom("`header` missing"))?;
                let contents = seq
                    .next_element()?
                    .ok_or_else(|| <A::Error as de::Error>::custom("`contents` missing"))?;
                Ok(CollectionDocument { header, contents })
            }
        }

        deserializer.deserialize_struct(
            "CollectionDocument",
            &["header", "contents"],
            CollectionDocumentVisitor::default(),
        )
    }
}

/// Helper functions for a slice of [`OwnedDocument`]s.
pub trait OwnedDocuments {
    /// Returns a list of deserialized documents.
    fn collection_documents<C: SerializedCollection>(
        &self,
    ) -> Result<Vec<CollectionDocument<C>>, Error>;
}

impl OwnedDocuments for [OwnedDocument] {
    fn collection_documents<C: SerializedCollection>(
        &self,
    ) -> Result<Vec<CollectionDocument<C>>, Error> {
        self.iter().map(CollectionDocument::try_from).collect()
    }
}

#[test]
fn collection_document_serialization() {
    use crate::test_util::Basic;

    let original: CollectionDocument<Basic> = CollectionDocument {
        header: CollectionHeader {
            id: 1,
            revision: super::Revision::new(b"hello world"),
        },
        contents: Basic::new("test"),
    };

    // Pot uses a map to represent a struct
    let pot = pot::to_vec(&original).unwrap();
    assert_eq!(
        pot::from_slice::<CollectionDocument<Basic>>(&pot).unwrap(),
        original
    );
    // Bincode uses a sequence to represent a struct
    let bincode = transmog_bincode::bincode::serialize(&original).unwrap();
    assert_eq!(
        transmog_bincode::bincode::deserialize::<CollectionDocument<Basic>>(&bincode).unwrap(),
        original
    );
}
