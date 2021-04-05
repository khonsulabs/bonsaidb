/// Types for defining a `Collection`.
pub mod collection;
mod schematic;
/// Types for defining map/reduce-powered `View`s.
pub mod view;

use std::{
    borrow::Cow,
    fmt::{Debug, Display},
};

use serde::{Deserialize, Serialize};

pub use self::{collection::*, schematic::*, view::*};

#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone)]
#[serde(transparent)]
/// The unique Id of a [`Schema`]. Primarily used to try to protect against
/// using the incorrect data types across a remote connection.
pub struct Id(Cow<'static, str>);

impl AsRef<str> for Id {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Id {
    /// Creates a new id.
    pub fn new<S: Into<String>>(id: S) -> Self {
        Self(Cow::Owned(id.into()))
    }
}

impl From<&'static str> for Id {
    fn from(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }
}

/// Defines a group of collections that are stored into a single database.
pub trait Schema: Send + Sync + Debug + 'static {
    /// Returns the unique [`Id`] for this schema.
    fn schema_id() -> Id;

    /// Defines the `Collection`s into `schema`.
    fn define_collections(schema: &mut Schematic);

    /// Retrieves the [`Schematic`] for this schema.
    #[must_use]
    fn schematic() -> Schematic {
        let mut schematic = Schematic::default();
        Self::define_collections(&mut schematic);
        schematic
    }
}

/// This trait is only useful for tools like `pliantdb local-backup`. There is no
/// real-world use case of connecting to a Database with no schema.
impl Schema for () {
    fn schema_id() -> Id {
        Id::from("")
    }

    fn define_collections(_schema: &mut Schematic) {}
}

impl<T> Schema for T
where
    T: Collection + 'static,
{
    fn schema_id() -> Id {
        Id(Self::collection_id().0)
    }

    fn define_collections(schema: &mut Schematic) {
        schema.define_collection::<Self>();
    }
}
