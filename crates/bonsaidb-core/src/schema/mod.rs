mod collection;
mod names;
mod schematic;
/// Types for defining map/reduce-powered `View`s.
pub mod view;
use std::fmt::Debug;

pub use bonsaidb_macros::{Collection, Schema, View};

pub use self::{
    collection::{
        AsyncEntry, AsyncList, Collection, DefaultSerialization, InsertError, List, Nameable,
        NamedCollection, NamedReference, SerializedCollection,
    },
    names::{
        ApiName, Authority, CollectionName, InvalidNameError, Name, Qualified, SchemaName, ViewName,
    },
    schematic::Schematic,
    view::{
        map::{Map, MappedValue, ViewMappedValue},
        CollectionViewSchema, DefaultViewSerialization, ReduceResult, SerializedView, View,
        ViewMapResult, ViewSchema,
    },
};
use crate::Error;

/// Defines a group of collections that are stored into a single database.
///
/// ## Deriving this trait
///
/// This trait can be derived rather than manually implemented:
///
/// ```rust
/// use bonsaidb_core::schema::{Collection, Schema};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Schema)]
/// #[schema(name = "MySchema", collections = [MyCollection])]
/// # #[schema(core = bonsaidb_core)]
/// pub struct MySchema;
///
/// #[derive(Debug, Serialize, Deserialize, Default, Collection)]
/// #[collection(name = "MyCollection")]
/// # #[collection(core = bonsaidb_core)]
/// pub struct MyCollection {
///     pub rank: u32,
///     pub score: f32,
/// }
/// ```
///
/// If you're publishing a schema for use in multiple projects, consider giving
/// the schema an `authority`, which gives your schema a namespace:
///
/// ```rust
/// use bonsaidb_core::schema::Schema;
///
/// #[derive(Debug, Schema)]
/// #[schema(name = "MySchema", authority = "khonsulabs", collections = [MyCollection])]
/// # #[schema(core = bonsaidb_core)]
/// pub struct MySchema;
///
/// # use serde::{Deserialize, Serialize};
/// # use bonsaidb_core::schema::Collection;
/// # #[derive(Debug, Serialize, Deserialize, Default, Collection)]
/// # #[collection(name = "MyCollection")]
/// # #[collection(core = bonsaidb_core)]
/// # pub struct MyCollection {
/// #    pub rank: u32,
/// #    pub score: f32,
/// # }
/// ```
pub trait Schema: Send + Sync + Debug + 'static {
    /// Returns the unique [`SchemaName`] for this schema.
    fn schema_name() -> SchemaName;

    /// Defines the `Collection`s into `schema`.
    fn define_collections(schema: &mut Schematic) -> Result<(), Error>;

    /// Retrieves the [`Schematic`] for this schema.
    fn schematic() -> Result<Schematic, Error> {
        Schematic::from_schema::<Self>()
    }
}

/// This implementation is for accessing databases when interacting with
/// collections isn't required. For example, accessing only the key-value store
/// or pubsub.
impl Schema for () {
    fn schema_name() -> SchemaName {
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
    fn schema_name() -> SchemaName {
        let CollectionName(qualified) = Self::collection_name();
        SchemaName(qualified)
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_collection::<Self>()
    }
}
