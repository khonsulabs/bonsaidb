use custodian_password::{LoginRequest, LoginResponse, RegistrationRequest, RegistrationResponse};
use schema::SchemaName;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    connection::{AccessPolicy, Database, QueryKey},
    document::Document,
    kv::{KeyOperation, Output},
    schema::{
        self,
        view::{self, map},
        CollectionName, Key, MappedValue, ViewName,
    },
    transaction::{Executed, OperationResult, Transaction},
};

/// A payload with an associated id.
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Payload<T> {
    /// The unique id for this payload.
    pub id: Option<u32>,
    /// The wrapped payload.
    pub wrapped: T,
}

/// A request made to a server.
#[derive(Clone, Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "actionable-traits", derive(actionable::Actionable))]
pub enum Request<T> {
    /// A server-related request.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "none"))]
    Server(ServerRequest),

    /// A database-related request.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "none"))]
    Database {
        /// The name of the database.
        database: String,
        /// The request made to the database.
        request: DatabaseRequest,
    },

    /// A database-related request.
    #[cfg_attr(
        feature = "actionable-traits",
        actionable(protection = "none", subaction)
    )]
    Api(T),
}

/// A server-related request.
#[derive(Clone, Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "actionable-traits", derive(actionable::Actionable))]
pub enum ServerRequest {
    /// Creates a database.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    CreateDatabase(Database),
    /// Deletes the database named `name`
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    DeleteDatabase {
        /// The name of the database to delete.
        name: String,
    },
    /// Lists all databases.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    ListDatabases,
    /// Lists available schemas.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    ListAvailableSchemas,
    /// Creates a user.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    CreateUser {
        /// The unique username of the user to create.
        username: String,
        password_request: RegistrationRequest,
    },
    /// Authenticates the current session as `username`.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    Login {
        /// The username of the user to authenticate as.
        username: String,
        password_request: LoginRequest,
    },
}

/// A database-related request.
#[derive(Clone, Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "actionable-traits", derive(actionable::Actionable))]
pub enum DatabaseRequest {
    /// Retrieve a single document.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    Get {
        /// The collection of the document.
        collection: CollectionName,
        /// The id of the document.
        id: u64,
    },
    /// Retrieve multiple documents.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "custom"))]
    GetMultiple {
        /// The collection of the documents.
        collection: CollectionName,
        /// The ids of the documents.
        ids: Vec<u64>,
    },
    /// Queries a view.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    Query {
        /// The name of the view.
        view: ViewName,
        /// The filter for the view.
        key: Option<QueryKey<Vec<u8>>>,
        /// The access policy for the query.
        access_policy: AccessPolicy,
        /// If true, [`DatabaseResponse::ViewMappingsWithDocs`] will be
        /// returned. If false, [`DatabaseResponse::ViewMappings`] will be
        /// returned.
        with_docs: bool,
    },
    /// Reduces a view.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    Reduce {
        /// The name of the view.
        view: ViewName,
        /// The filter for the view.
        key: Option<QueryKey<Vec<u8>>>,
        /// The access policy for the query.
        access_policy: AccessPolicy,
        /// Whether to return a single value or values grouped by unique key. If
        /// true, [`DatabaseResponse::ViewGroupedReduction`] will be returned.
        /// If false, [`DatabaseResponse::ViewReduction`] is returned.
        grouped: bool,
    },
    /// Applies a transaction.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "custom"))]
    ApplyTransaction {
        /// The trasnaction to apply.
        transaction: Transaction<'static>,
    },
    /// Lists executed transactions.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    ListExecutedTransactions {
        /// The starting transaction id.
        starting_id: Option<u64>,
        /// The maximum number of results.
        result_limit: Option<usize>,
    },
    /// Queries the last transaction id.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    LastTransactionId,
    /// Creates a `PubSub` [`Subscriber`](crate::pubsub::Subscriber)
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    CreateSubscriber,
    /// Publishes `payload` to all subscribers of `topic`.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    Publish {
        /// The topics to publish to.
        topic: String,
        /// The payload to publish.
        payload: Vec<u8>,
    },
    /// Publishes `payload` to all subscribers of all `topics`.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "custom"))]
    PublishToAll {
        /// The topics to publish to.
        topics: Vec<String>,
        /// The payload to publish.
        payload: Vec<u8>,
    },
    /// Subscribes `subscriber_id` to messages for `topic`.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    SubscribeTo {
        /// The id of the [`Subscriber`](crate::pubsub::Subscriber).
        subscriber_id: u64,
        /// The topic to subscribe to.
        topic: String,
    },
    /// Unsubscribes `subscriber_id` from messages for `topic`.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    UnsubscribeFrom {
        /// The id of the [`Subscriber`](crate::pubsub::Subscriber).
        subscriber_id: u64,
        /// The topic to unsubscribe from.
        topic: String,
    },
    /// Unregisters the subscriber.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "none"))]
    UnregisterSubscriber {
        /// The id of the [`Subscriber`](crate::pubsub::Subscriber).
        subscriber_id: u64,
    },
    /// Excutes a key-value store operation.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    ExecuteKeyOperation(KeyOperation),
}

/// A response from a server.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Response<T> {
    /// A request succeded but provided no output.
    Ok,
    /// A response to a [`ServerRequest`].
    Server(ServerResponse),
    /// A response to a [`DatabaseRequest`].
    Database(DatabaseResponse),
    /// A response to an Api request.
    Api(T),
    /// An error occurred processing a request.
    Error(crate::Error),
}

/// A response to a [`ServerRequest`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ServerResponse {
    /// A database with `name` was successfully created.
    DatabaseCreated {
        /// The name of the database to create.
        name: String,
    },
    /// A database with `name` was successfully removed.
    DatabaseDeleted {
        /// The name of the database to remove.
        name: String,
    },
    /// A list of available databases.
    Databases(Vec<Database>),
    /// A list of availble schemas.
    AvailableSchemas(Vec<SchemaName>),
    /// A user was created.
    UserCreated {
        /// The id of the user created.
        id: u64,
        password_reponse: Box<RegistrationResponse>,
    },
    /// Successfully authenticated.
    LoggedIn {
        // TODO actionable needs to make `Permissions` serialize/deserialize: permissions: actionable::Permissions,
        response: Box<LoginResponse>,
    },
}

/// A response to a [`DatabaseRequest`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DatabaseResponse {
    /// One or more documents.
    Documents(Vec<Document<'static>>),
    /// Results of [`DatabaseRequest::ApplyTransaction`].
    TransactionResults(Vec<OperationResult>),
    /// Results of [`DatabaseRequest::Query`] when `with_docs` is false.
    ViewMappings(Vec<map::Serialized>),
    /// Results of [`DatabaseRequest::Query`] when `with_docs` is true.
    ViewMappingsWithDocs(Vec<MappedDocument>),
    /// Result of [`DatabaseRequest::Reduce`] when `grouped` is false.
    ViewReduction(Vec<u8>),
    /// Result of [`DatabaseRequest::Reduce`] when `grouped` is true.
    ViewGroupedReduction(Vec<MappedValue<Vec<u8>, Vec<u8>>>),
    /// Results of [`DatabaseRequest::ListExecutedTransactions`].
    ExecutedTransactions(Vec<Executed<'static>>),
    /// Result of [`DatabaseRequest::LastTransactionId`].
    LastTransactionId(Option<u64>),
    /// A new `PubSub` subscriber was created.
    SubscriberCreated {
        /// The unique ID of the subscriber.
        subscriber_id: u64,
    },
    /// A PubSub message was received.
    MessageReceived {
        /// The ID of the subscriber receiving the message.
        subscriber_id: u64,
        /// The topic the payload was received on.
        topic: String,
        /// The message payload.
        payload: Vec<u8>,
    },
    /// Output from a [`KeyOperation`] being executed.
    KvOutput(Output),
}

/// A serialized [`MappedDocument`](map::MappedDocument).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedDocument {
    /// The serialized key.
    pub key: Vec<u8>,
    /// The serialized value.
    pub value: Vec<u8>,
    /// The source document.
    pub source: Document<'static>,
}

impl MappedDocument {
    /// Deserialize into a [`MappedDocument`](map::MappedDocument).
    pub fn deserialized<K: Key, V: Serialize + DeserializeOwned>(
        self,
    ) -> Result<map::MappedDocument<K, V>, crate::Error> {
        let key = Key::from_big_endian_bytes(&self.key).map_err(|err| {
            crate::Error::Database(view::Error::KeySerialization(err).to_string())
        })?;
        let value = serde_cbor::from_slice(&self.value)
            .map_err(|err| crate::Error::Database(view::Error::from(err).to_string()))?;

        Ok(map::MappedDocument {
            document: self.source,
            key,
            value,
        })
    }
}

/// A networking error.
#[derive(Clone, thiserror::Error, Debug, Serialize, Deserialize)]
pub enum Error {
    /// The server responded with a message that wasn't expected for the request
    /// sent.
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),

    /// The connection was interrupted.
    #[error("unexpected disconnection")]
    Disconnected,
}
