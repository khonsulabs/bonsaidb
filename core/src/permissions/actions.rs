#![allow(missing_docs)] // TODO actions documentation

use actionable::Action;
use serde::{Deserialize, Serialize};

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum PliantAction {
    Server(ServerAction),
    Database(DatabaseAction),
}

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum ServerAction {
    Connect,
    ListAvailableSchemas,
    ListDatabases,
    CreateDatabase,
    DeleteDatabase,
}

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum DatabaseAction {
    Document(DocumentAction),
    View(ViewAction),
    Transaction(TransactionAction),
    PubSub(PubSubAction),
    Kv(KvAction),
}

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum DocumentAction {
    Get,
    GetMultiple,
    Insert,
    Update,
    Delete,
}

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum ViewAction {
    Query,
    Reduce,
}

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum TransactionAction {
    Apply,
    ListExecuted,
    GetLastId,
}

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum PubSubAction {
    CreateSuscriber,
    Publish,
    SubscribeTo,
    UnsubscribeFrom,
}

#[derive(Action, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum KvAction {
    ExecuteOperation,
}
