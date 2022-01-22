use crate::{
    schema::{Schema, SchemaName, Schematic},
    Error,
};

#[doc(hidden)]
pub mod database;
#[cfg(feature = "multiuser")]
#[doc(hidden)]
pub mod group;
#[cfg(feature = "multiuser")]
#[doc(hidden)]
pub mod password_config;
#[doc(hidden)]
#[cfg(feature = "multiuser")]
pub mod role;
#[cfg(feature = "multiuser")]
#[doc(hidden)]
pub mod user;

pub use self::database::Database;
#[cfg(feature = "multiuser")]
pub use self::{group::PermissionGroup, role::Role, user::User};

/// The `BonsaiDb` administration schema.
#[derive(Debug)]
pub struct Admin;

impl Schema for Admin {
    fn schema_name() -> SchemaName {
        SchemaName::new("khonsulabs", "bonsaidb-admin")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_collection::<database::Database>()?;

        #[cfg(feature = "multiuser")]
        {
            schema.define_collection::<group::PermissionGroup>()?;
            schema.define_collection::<role::Role>()?;
            schema.define_collection::<user::User>()?;
            schema.define_collection::<password_config::PasswordConfig>()?;
        }

        Ok(())
    }
}

/// The name of the admin database.
pub const ADMIN_DATABASE_NAME: &str = "_admin";
