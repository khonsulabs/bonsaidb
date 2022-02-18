use crate::schema::Schema;

#[doc(hidden)]
pub mod database;
#[cfg(feature = "multiuser")]
#[doc(hidden)]
pub mod group;
#[doc(hidden)]
#[cfg(feature = "multiuser")]
pub mod role;
#[cfg(feature = "multiuser")]
#[doc(hidden)]
pub mod user;

pub use self::database::Database;
#[cfg(feature = "multiuser")]
pub use self::{group::PermissionGroup, role::Role, user::User};

/// The BonsaiDb administration schema.
#[derive(Debug, Schema)]
#[schema(name = "bonsaidb-admin", authority = "khonsulabs", collections = [Database], core = crate)]
#[cfg_attr(feature = "multiuser", schema(collections = [PermissionGroup, Role, User]))]
pub struct Admin;

/// The name of the admin database.
pub const ADMIN_DATABASE_NAME: &str = "_admin";
