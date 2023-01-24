use crate::schema::Schema;

#[doc(hidden)]
pub mod authentication_token;
#[doc(hidden)]
pub mod database;
#[doc(hidden)]
pub mod group;
#[doc(hidden)]
pub mod role;
#[doc(hidden)]
pub mod user;

pub use self::authentication_token::AuthenticationToken;
pub use self::database::Database;
pub use self::group::PermissionGroup;
pub use self::role::Role;
pub use self::user::User;

/// The BonsaiDb administration schema.
#[derive(Debug, Schema)]
#[schema(name = "bonsaidb-admin", authority = "khonsulabs", collections = [Database, PermissionGroup, Role, User, AuthenticationToken], core = crate)]
pub struct Admin;

/// The name of the admin database.
pub const ADMIN_DATABASE_NAME: &str = "_admin";
