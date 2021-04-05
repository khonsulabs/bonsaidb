use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    path::{Path, PathBuf},
    sync::Arc,
};

use admin::database::{self, Database};
use pliantdb_core::schema;
use pliantdb_local::{
    core::{
        connection::Connection,
        schema::{Schema, Schematic},
    },
    Configuration, Storage,
};
use tokio::sync::RwLock;

use crate::{
    admin::{self, database::ByName, Admin},
    error::Error,
    hosted,
};

/// A `PliantDB` server.
#[derive(Clone, Debug)]
pub struct Server {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    directory: PathBuf,
    admin: Storage<Admin>,
    schemas: HashMap<schema::Id, Schematic>,
    open_databases: RwLock<HashMap<String, Box<dyn OpenDatabase>>>,
    available_databases: RwLock<HashMap<String, schema::Id>>,
}

impl Server {
    /// Creates or opens a [`Server`] with its data stored in `directory`.
    /// `schemas` is a collection of [`schema::Id`] to [`Schematic`] pairs. [`schema::Id`]s are used as an identifier of a specific `Schema`, which the Server uses to
    pub async fn open(
        directory: &Path,
        schemas: HashMap<schema::Id, Schematic>,
    ) -> Result<Self, Error> {
        let admin =
            Storage::open_local(directory.join("admin.pliantdb"), &Configuration::default())
                .await?;

        let available_databases = admin
            .view::<ByName>()
            .query()
            .await?
            .into_iter()
            .map(|map| (map.key, map.value))
            .collect();

        Ok(Self {
            data: Arc::new(Data {
                admin,
                directory: directory.to_owned(),
                schemas,
                available_databases: RwLock::new(available_databases),
                open_databases: RwLock::default(),
            }),
        })
    }

    /// Creates a database named `name` using the [`schema::Id`] `schema`.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken]: `name` was already used for a
    ///   previous database name. Database names are case insensitive.
    pub async fn create_database(&self, name: &str, schema: schema::Id) -> Result<(), Error> {
        Self::validate_name(name)?;

        let mut available_databases = self.data.available_databases.write().await;
        if !self
            .data
            .admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()
            .await?
            .is_empty()
        {
            return Err(Error::DatabaseNameAlreadyTaken(name.to_string()));
        }
        self.data
            .admin
            .collection::<Database>()
            .push(&pliantdb_networking::Database {
                name: Cow::Borrowed(name),
                schema: schema.clone(),
            })
            .await?;
        available_databases.insert(name.to_owned(), schema);

        Ok(())
    }

    /// Retrieves a database. This function only verifies that the database exists
    pub async fn database<'a, DB: Schema>(
        &self,
        name: &'a str,
    ) -> Result<hosted::Database<'_, 'a, DB>, Error> {
        let available_databases = self.data.available_databases.read().await;

        if let Some(stored_schema) = available_databases.get(name) {
            if stored_schema == &DB::schema_id() {
                Ok(hosted::Database::new(self, name))
            } else {
                Err(Error::SchemaMismatch {
                    database_name: name.to_owned(),
                    schema: DB::schema_id(),
                    stored_schema: stored_schema.clone(),
                })
            }
        } else {
            Err(Error::DatabaseNotFound(name.to_owned()))
        }
    }

    pub(crate) async fn open_database<DB: Schema>(&self, name: &str) -> Result<Storage<DB>, Error> {
        // If we have an open database return it
        {
            let open_databases = self.data.open_databases.read().await;
            if let Some(db) = open_databases.get(name) {
                let storage = db
                    .as_any()
                    .downcast_ref::<Storage<DB>>()
                    .expect("schema did not match");
                return Ok(storage.clone());
            }
        }

        // Open the database.
        let mut open_databases = self.data.open_databases.write().await;
        if self
            .data
            .admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()
            .await?
            .is_empty()
        {
            return Err(Error::DatabaseNotFound(name.to_string()));
        }

        let db = Storage::<DB>::open_local(
            self.data.directory.join(format!("{}.pliantdb", name)),
            &Configuration::default(),
        )
        .await?;
        open_databases.insert(name.to_owned(), Box::new(db.clone()));
        Ok(db)
    }

    fn validate_name(name: &str) -> Result<(), Error> {
        if name
            .chars()
            .enumerate()
            .all(|(index, c)| c.is_ascii_alphanumeric() || (index > 0 && (c == '.' || c == '-')))
        {
            Ok(())
        } else {
            Err(Error::InvalidDatabaseName(name.to_owned()))
        }
    }
}

trait OpenDatabase: Send + Sync + Debug + 'static {
    fn as_any(&self) -> &'_ dyn Any;
}

impl<DB> OpenDatabase for Storage<DB>
where
    DB: Schema,
{
    fn as_any(&self) -> &'_ dyn Any {
        self
    }
}

#[test]
fn name_validation_tests() {
    assert!(matches!(Server::validate_name("azAZ09.-"), Ok(())));
    assert!(matches!(
        Server::validate_name(".alphaunmericfirstrequired"),
        Err(Error::InvalidDatabaseName(_))
    ));
    assert!(matches!(
        Server::validate_name("-alphaunmericfirstrequired"),
        Err(Error::InvalidDatabaseName(_))
    ));
    assert!(matches!(
        Server::validate_name("\u{2661}"),
        Err(Error::InvalidDatabaseName(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn opening_databases_test() -> Result<(), Error> {
    Ok(())
}
