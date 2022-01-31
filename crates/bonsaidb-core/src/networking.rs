use arc_bytes::serde::Bytes;
use derive_where::derive_where;
use schema::SchemaName;
use serde::{Deserialize, Serialize};

use crate::{
    connection::{AccessPolicy, Authenticated, Database, QueryKey, Range, Sort},
    document::OwnedDocument,
    keyvalue::{KeyOperation, Output},
    schema::{
        self,
        view::map::{self},
        CollectionName, NamedReference, ViewName,
    },
    transaction::{Executed, OperationResult, Transaction},
};

/// The current protocol version.
pub const CURRENT_PROTOCOL_VERSION: &str = "bonsai/pre/0";

/// A payload with an associated id.
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Payload<T> {
    /// The unique id for this payload.
    pub id: Option<u32>,
    /// The wrapped payload.
    pub wrapped: T,
}

/// A request made to a server.
#[derive(Clone, Deserialize, Serialize)]
#[derive_where(Debug)]
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
    #[derive_where(skip_inner)]
    Api(T),
}

/// A server-related request.
#[derive(Clone, Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "actionable-traits", derive(actionable::Actionable))]
pub enum ServerRequest {
    /// Creates a database.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    CreateDatabase {
        /// The database to create.
        database: Database,
        /// Only attempts to create the database if it doesn't already exist.
        only_if_needed: bool,
    },
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
    },
    /// Set's a user's password.
    #[cfg(feature = "password-hashing")]
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    SetUserPassword {
        /// The username or id of the user.
        user: NamedReference<'static>,
        /// The user's new password.
        password: crate::connection::SensitiveString,
    },
    /// Authenticate as a user.
    #[cfg(feature = "password-hashing")]
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "custom"))]
    Authenticate {
        /// The username or id of the user.
        user: NamedReference<'static>,
        /// The method of authentication.
        authentication: crate::connection::Authentication,
    },

    /// Alter's a user's membership in a permission group.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    AlterUserPermissionGroupMembership {
        /// The username or id of the user.
        user: NamedReference<'static>,

        /// The name or id of the group.
        group: NamedReference<'static>,

        /// Whether the user should be in the group.
        should_be_member: bool,
    },

    /// Alter's a user's role
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    AlterUserRoleMembership {
        /// The username or id of the user.
        user: NamedReference<'static>,

        /// The name or id of the role.
        role: NamedReference<'static>,

        /// Whether the user should have the role.
        should_be_member: bool,
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
    /// Retrieve multiple documents.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    List {
        /// The collection of the documents.
        collection: CollectionName,
        /// The range of ids to list.
        ids: Range<u64>,
        /// The order for the query into the collection.
        order: Sort,
        /// The maximum number of results to return.
        limit: Option<usize>,
    },
    /// Queries a view.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    Query {
        /// The name of the view.
        view: ViewName,
        /// The filter for the view.
        key: Option<QueryKey<Bytes>>,
        /// The order for the query into the view.
        order: Sort,
        /// The maximum number of results to return.
        limit: Option<usize>,
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
        key: Option<QueryKey<Bytes>>,
        /// The access policy for the query.
        access_policy: AccessPolicy,
        /// Whether to return a single value or values grouped by unique key. If
        /// true, [`DatabaseResponse::ViewGroupedReduction`] will be returned.
        /// If false, [`DatabaseResponse::ViewReduction`] is returned.
        grouped: bool,
    },
    /// Deletes the associated documents resulting from the view query.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    DeleteDocs {
        /// The name of the view.
        view: ViewName,
        /// The filter for the view.
        key: Option<QueryKey<Bytes>>,
        /// The access policy for the query.
        access_policy: AccessPolicy,
    },
    /// Applies a transaction.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "custom"))]
    ApplyTransaction {
        /// The trasnaction to apply.
        transaction: Transaction,
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
        payload: Bytes,
    },
    /// Publishes `payload` to all subscribers of all `topics`.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "custom"))]
    PublishToAll {
        /// The topics to publish to.
        topics: Vec<String>,
        /// The payload to publish.
        payload: Bytes,
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
    /// Compacts the collection.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    CompactCollection {
        /// The name of the collection to compact.
        name: CollectionName,
    },
    /// Compacts the key-value store.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    CompactKeyValueStore,
    /// Compacts the entire database.
    #[cfg_attr(feature = "actionable-traits", actionable(protection = "simple"))]
    Compact,
}

/// A response from a server.
#[derive(Clone, Serialize, Deserialize)]
#[derive_where(Debug)]
pub enum Response<T> {
    /// A request succeded but provided no output.
    Ok,
    /// A response to a [`ServerRequest`].
    Server(ServerResponse),
    /// A response to a [`DatabaseRequest`].
    Database(DatabaseResponse),
    /// A response to an Api request.
    #[derive_where(skip_inner)]
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
    },
    /// Successfully authenticated.
    Authenticated(Authenticated),
}

/// A response to a [`DatabaseRequest`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DatabaseResponse {
    /// One or more documents.
    Documents(Vec<OwnedDocument>),
    /// A number of documents were deleted.
    DocumentsDeleted(u64),
    /// Results of [`DatabaseRequest::ApplyTransaction`].
    TransactionResults(Vec<OperationResult>),
    /// Results of [`DatabaseRequest::Query`] when `with_docs` is false.
    ViewMappings(Vec<map::Serialized>),
    /// Results of [`DatabaseRequest::Query`] when `with_docs` is true.
    ViewMappingsWithDocs(Vec<map::MappedSerialized>),
    /// Result of [`DatabaseRequest::Reduce`] when `grouped` is false.
    ViewReduction(Bytes),
    /// Result of [`DatabaseRequest::Reduce`] when `grouped` is true.
    ViewGroupedReduction(Vec<map::MappedSerializedValue>),
    /// Results of [`DatabaseRequest::ListExecutedTransactions`].
    ExecutedTransactions(Vec<Executed>),
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
        payload: Bytes,
    },
    /// Output from a [`KeyOperation`] being executed.
    KvOutput(Output),
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
