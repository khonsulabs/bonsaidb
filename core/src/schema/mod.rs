/// Types for defining a `Collection`.
pub mod collection;
mod database;
/// Types for defining map/reduce-powered `View`s.
pub mod view;

pub use self::{collection::*, database::*, view::*};
