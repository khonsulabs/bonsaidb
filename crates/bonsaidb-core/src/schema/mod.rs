mod collection;
mod names;
mod schematic;
/// Types for defining map/reduce-powered `View`s.
pub mod view;
use std::fmt::Debug;

pub use self::{
    collection::{
        Collection, CollectionDocument, CollectionSerializer, InsertError, NamedCollection,
        NamedReference,
    },
    names::{Authority, CollectionName, InvalidNameError, Name, SchemaName, ViewName},
    schematic::Schematic,
    view::{
        map::{Key, Map, MappedDocument, MappedValue},
        MapResult, View,
    },
};
use crate::Error;

/// Defines a group of collections that are stored into a single database.
pub trait Schema: Send + Sync + Debug + 'static {
    /// Returns the unique [`SchemaName`] for this schema.
    fn schema_name() -> Result<SchemaName, InvalidNameError>;

    /// Defines the `Collection`s into `schema`.
    fn define_collections(schema: &mut Schematic) -> Result<(), Error>;

    /// Retrieves the [`Schematic`] for this schema.
    fn schematic() -> Result<Schematic, Error> {
        let mut schematic = Schematic::default();
        Self::define_collections(&mut schematic)?;
        Ok(schematic)
    }
}

/// This implementation is for accessing databases when interacting with
/// collections isn't required. For example, accessing only the key-value store
/// or pubsub.
impl Schema for () {
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        SchemaName::new("", "")
    }

    fn define_collections(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

impl<T> Schema for T
where
    T: Collection + 'static,
{
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        let CollectionName { authority, name } = Self::collection_name()?;
        Ok(SchemaName { authority, name })
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_collection::<Self>()
    }
}
