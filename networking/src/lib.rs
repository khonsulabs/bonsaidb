pub use cosmicverge_networking;
use pliantdb_core::{document::Document, schema::collection};

use std::{borrow::Cow, sync::Arc};

use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum Request<'a> {
    Server {
        request: ServerRequest<'a>,
    },
    Database {
        database: Cow<'a, str>,
        request: DatabaseRequest,
    },
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum ServerRequest<'a> {
    CreateDatabase(Database<'a>),
    DeleteDatabase { name: Cow<'a, str> },
    ListDatabases,
    ListAvailableSchemas,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum DatabaseRequest {
    Get { collection: collection::Id, id: u64 },
}
#[derive(Serialize, Deserialize, Debug)]
pub enum Response<'a> {
    Server(ServerResponse<'a>),
    Database(DatabaseResponse<'a>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerResponse<'a> {
    DatabaseCreated { name: Cow<'a, str> },
    DatabaseDeleted { name: Cow<'a, str> },
    Databases(Vec<Database<'a>>),
    AvailableSchemas(Vec<SchemaId>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DatabaseResponse<'a> {
    Documents(Vec<Document<'a>>),
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Database<'a> {
    pub name: Cow<'a, str>,
    pub schema: SchemaId,
}

#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone)]
#[serde(transparent)]
pub struct SchemaId(Arc<String>);

impl AsRef<str> for SchemaId {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl SchemaId {
    pub fn new<S: Into<String>>(id: S) -> Self {
        Self(Arc::new(id.into()))
    }
}

impl From<&'_ str> for SchemaId {
    fn from(id: &'_ str) -> Self {
        Self::new(id)
    }
}
