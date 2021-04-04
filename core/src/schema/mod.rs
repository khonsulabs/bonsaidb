/// Types for defining a `Collection`.
pub mod collection;
mod schematic;
/// Types for defining map/reduce-powered `View`s.
pub mod view;

use std::fmt::Debug;

pub use self::{collection::*, schematic::*, view::*};

/// Defines a group of collections that are stored into a single database.
pub trait Schema: Send + Sync + Debug + 'static {
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
    fn define_collections(_schema: &mut Schematic) {}
}
