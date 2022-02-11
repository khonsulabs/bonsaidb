//! Aysnc jobs management for BonsaiDb.

/// Types related to the job [`Manager`](manager::Manager).
pub mod manager;
/// Types related to defining [`Job`]s.
pub mod task;
mod traits;

pub use flume;

pub use self::traits::{Job, Keyed};
