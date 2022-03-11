use crate::schema::Schema;

#[doc(hidden)]
pub mod database;
#[doc(hidden)]
pub mod group;
#[doc(hidden)]
pub mod role;
#[doc(hidden)]
pub mod user;

pub use self::{database::Database, group::PermissionGroup, role::Role, user::User};

/// The BonsaiDb administration schema.
#[derive(Debug, Schema)]
#[schema(name = "bonsaidb-admin", authority = "khonsulabs", collections = [Database, PermissionGroup, Role, User], core = crate)]
pub struct Admin;

/// The name of the admin database.
pub const ADMIN_DATABASE_NAME: &str = "_admin";
