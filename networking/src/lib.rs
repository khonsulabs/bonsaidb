use std::borrow::Cow;

pub use cosmicverge_networking as fabruic;
use pliantdb_core::{
    document::Document,
    schema::{self, collection},
    transaction::{OperationResult, Transaction},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Payload<'a> {
    pub id: u64,
    pub api: Api<'a>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum Api<'a> {
    Request(Request<'a>),
    Response(Response<'a>),
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum Request<'a> {
    Server(ServerRequest<'a>),
    Database {
        database: Cow<'a, str>,
        request: DatabaseRequest<'a>,
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
pub enum DatabaseRequest<'a> {
    Get { collection: collection::Id, id: u64 },
    ApplyTransaction { transaction: Transaction<'a> },
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Response<'a> {
    Server(ServerResponse<'a>),
    Database(DatabaseResponse<'a>),
    Error(String),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ServerResponse<'a> {
    DatabaseCreated { name: Cow<'a, str> },
    DatabaseDeleted { name: Cow<'a, str> },
    Databases(Vec<Database<'a>>),
    AvailableSchemas(Vec<schema::Id>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DatabaseResponse<'a> {
    Documents(Vec<Document<'a>>),
    TransactionResults(Vec<OperationResult>),
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Database<'a> {
    pub name: Cow<'a, str>,
    pub schema: schema::Id,
}
