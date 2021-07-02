// When generating docs with either keyvalue or pubsub features disabled, there
// will be broken links in these docs. Instead of failing to generate docs with
// these feaures disabled, we downgrade the `broken_intra_doc_links` lint to a
// warning.
#![cfg_attr(
    all(doc, not(all(feature = "keyvalue", feature = "pubsub"))),
    warn(broken_intra_doc_links)
)]
use actionable::{Action, ResourceName};
use serde::{Deserialize, Serialize};

use crate::{
    document::KeyId,
    schema::{CollectionName, ViewName},
};

/// The base `PliantDb` resource namespace. All database objects have this as
/// their first name segment.
#[must_use]
pub fn pliantdb_resource_name<'a>() -> ResourceName<'a> {
    ResourceName::named("pliantdb")
}

/// Creates a resource name with the database `name`.
#[must_use]
pub fn database_resource_name(name: &'_ str) -> ResourceName<'_> {
    pliantdb_resource_name().and(name)
}

/// Creates a resource name for a `collection` within a `database`.
#[must_use]
pub fn collection_resource_name<'a>(
    database: &'a str,
    collection: &'a CollectionName,
) -> ResourceName<'a> {
    database_resource_name(database).and(collection.to_string())
}

/// Creates a resource name for a document `id` within `collection` within `database`.
#[must_use]
pub fn document_resource_name<'a>(
    database: &'a str,
    collection: &'a CollectionName,
    id: u64,
) -> ResourceName<'a> {
    collection_resource_name(database, collection)
        .and("document")
        .and(id)
}

/// Creaets a resource name for a `view` within `database`.
#[must_use]
pub fn view_resource_name<'a>(database: &'a str, view: &'a ViewName) -> ResourceName<'a> {
    database_resource_name(database)
        .and(view.collection.to_string())
        .and("view")
        .and(view.name.as_ref())
}

/// Creates a resource name for `PubSub` `topic` within `database`.
#[must_use]
pub fn pubsub_topic_resource_name<'a>(database: &'a str, topic: &'a str) -> ResourceName<'a> {
    database_resource_name(database).and("pubsub").and(topic)
}

/// Creates a resource name for `key` within `namespace` within the key-value store of `database`.
#[must_use]
pub fn kv_key_resource_name<'a>(
    database: &'a str,
    namespace: Option<&'a str>,
    key: &'a str,
) -> ResourceName<'a> {
    database_resource_name(database)
        .and("pubsub")
        .and(namespace.unwrap_or(""))
        .and(key)
}

/// Creates a resource name for encryption key `key_id`.
#[must_use]
pub fn vault_key_resource_name(key_id: &KeyId) -> ResourceName<'_> {
    pliantdb_resource_name()
        .and("vault")
        .and("key")
        .and(match key_id {
            KeyId::Master => "_master",
            KeyId::Id(id) => id.as_ref(),
            KeyId::None => unreachable!(),
        })
}

/// Actions that can be permitted within `PliantDb`.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum PliantAction {
    /// Actions that operate on a server
    Server(ServerAction),
    /// Actions that operate on a specific database.
    Database(DatabaseAction),
}

/// Actions that operate on a server.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum ServerAction {
    /// Permits connecting to the server. Upon negotiating authentication, the
    /// effective permissions of the connected party will be checked for
    /// permissions to `Connect`. If not allowed, the connection will be
    /// terminated. (Not implemented)
    // TODO actually implement Connect checking
    Connect,
    /// Permits [`ServerConnection::list_available_schemas`](crate::connection::ServerConnection::list_available_schemas).
    ListAvailableSchemas,
    /// Permits [`ServerConnection::list_databases`](crate::connection::ServerConnection::list_databases).
    ListDatabases,
    /// Permits [`ServerConnection::create_database`](crate::connection::ServerConnection::create_database).
    CreateDatabase,
    /// Permits [`ServerConnection::delete_database`](crate::connection::ServerConnection::delete_database).
    DeleteDatabase,
    CreateUser,
    Login,
}

/// Actions that operate on a specific database.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum DatabaseAction {
    /// Actions that operate on a document.
    Document(DocumentAction),
    /// Actions that operate on a view.
    View(ViewAction),
    /// Actions that operate on transactions.
    Transaction(TransactionAction),
    /// Actions that operate on the `PubSub` system.
    PubSub(PubSubAction),
    /// Actions that operate on the key-value store.
    Kv(KvAction),
}

/// Actions that operate on a document.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum DocumentAction {
    /// Allows document retrieval through
    /// [`Connection::get()`](crate::connection::Connection::get) and
    /// [`Connection::get_multiple()`](crate::connection::Connection::get_multiple).
    /// See [`document_resource_name()`] for the format of document resource
    /// names.
    Get,
    /// Allows inserting a document through
    /// [`Connection::apply_transaction()`](crate::connection::Connection::apply_transaction).
    /// See [`collection_resource_name()`] for the format of collection resource
    /// names.
    Insert,
    /// Allows updating a document through
    /// [`Connection::apply_transaction()`](crate::connection::Connection::apply_transaction).
    /// See [`document_resource_name()`] for the format of document resource
    /// names.
    Update,
    /// Allows deleting a document through
    /// [`Connection::apply_transaction()`](crate::connection::Connection::apply_transaction).
    /// See [`document_resource_name()`] for the format of document resource
    /// names.
    Delete,
}

/// Actions that operate on a view.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum ViewAction {
    /// Allows querying a view with
    /// [`Connection::query()`](crate::connection::Connection::query). See
    /// [`view_resource_name`] for the format of view resource names.
    Query,
    /// Allows reducing a view with
    /// [`Connection::reduce()`](crate::connection::Connection::reduce). See
    /// [`view_resource_name`] for the format of view resource names.
    Reduce,
}

/// Actions that operate on transactions.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum TransactionAction {
    /// Allows listing executed transactions with
    /// [`Connection::list_executed_transactions()`](crate::connection::Connection::list_executed_transactions).
    /// This action is checked against the database's resource name. See
    /// [`database_resource_name()`] for the format of database resource names.
    ListExecuted,
    /// Allows retrieving the last executed transaction id with
    /// [`Connection::last_transaction_id()`](crate::connection::Connection::last_transaction_id).
    /// This action is checked against the database's resource name. See
    /// [`database_resource_name()`] for the format of database resource names.
    GetLastId,
}

/// Actions that operate on the `PubSub` system.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum PubSubAction {
    /// Allows creating a subscriber with
    /// [`PubSub::create_subscriber()`](crate::pubsub::PubSub::create_subscriber).
    /// This action is checked against the database's resource name. See
    /// [`database_resource_name()`] for the format of database resource names.
    CreateSuscriber,
    /// Allows publishing a payload to a `PubSub` topic with
    /// [`PubSub::publish()`](crate::pubsub::PubSub::publish). See
    /// [`pubsub_topic_resource_name()`] for the format of `PubSub` topic
    /// resource names.
    Publish,
    /// Allows subscribing to a `PubSub` topic with
    /// [`PubSub::subscribe_to()`](crate::pubsub::Subscriber::subscribe_to). See
    /// [`pubsub_topic_resource_name()`] for the format of `PubSub` topic
    /// resource names.
    SubscribeTo,
    /// Allows unsubscribing from a `PubSub` topic with
    /// [`PubSub::unsubscribe_from()`](crate::pubsub::Subscriber::unsubscribe_from). See
    /// [`pubsub_topic_resource_name()`] for the format of `PubSub` topic
    /// resource names.
    UnsubscribeFrom,
}

/// Actions that operate on the key-value store.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum KvAction {
    /// Allows executing a key-value store operation with
    /// [`Kv::execute_key_operation()`](crate::kv::Kv::execute_key_operation).
    /// See [`kv_key_resource_name()`] for the format of key resource names.
    ExecuteOperation,
}

/// Actions that use encryption keys.
#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum EncryptionKeyAction {
    /// Uses a key to encrypt data.
    Encrypt,
    /// Uses a key to decrypt data.
    Decrypt,
}
