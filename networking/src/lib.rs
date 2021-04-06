use std::borrow::Cow;

pub use cosmicverge_networking as fabruic;
use pliantdb_core::{
    document::Document,
    schema::{self, collection},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum Payload<'a> {
    Request(Request<'a>),
    Response(Response<'a>),
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum Request<'a> {
    Server { request: ServerRequest<'a> },
    // Database {
    //     database: Cow<'a, str>,
    //     request: DatabaseRequest,
    // },
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum ServerRequest<'a> {
    CreateDatabase(Database<'a>),
    // DeleteDatabase { name: Cow<'a, str> },
    // ListDatabases,
    // ListAvailableSchemas,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum DatabaseRequest {
    Get { collection: collection::Id, id: u64 },
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
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Database<'a> {
    pub name: Cow<'a, str>,
    pub schema: schema::Id,
}
