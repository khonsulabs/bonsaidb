use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::{AccessPolicy, AsyncConnection, AsyncLowLevelConnection, QueryKey, Range, Sort},
    document::{DocumentId, OwnedDocument},
    networking::{DatabaseRequest, DatabaseResponse, Request, Response},
    schema::{self, view::map::MappedSerializedValue, CollectionName, Schematic, ViewName},
    transaction::{Executed, OperationResult, Transaction},
};

use crate::Client;

mod pubsub;
pub use pubsub::*;

mod keyvalue;

/// A database on a remote server.
#[derive(Debug, Clone)]
pub struct RemoteDatabase {
    pub(crate) client: Client,
    name: Arc<String>,
    pub(crate) schema: Arc<Schematic>,
}
impl RemoteDatabase {
    /// Returns the name of the database.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn tokio(&self) -> &tokio::runtime::Handle {
        self.client.tokio()
    }
}

impl Deref for RemoteDatabase {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl RemoteDatabase {
    pub(crate) fn new(client: Client, name: String, schema: Arc<Schematic>) -> Self {
        Self {
            client,
            name: Arc::new(name),
            schema,
        }
    }
}

#[async_trait]
impl AsyncConnection for RemoteDatabase {
    type Storage = Client;

    fn storage(&self) -> Self::Storage {
        self.client.clone()
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<Executed>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::ListExecutedTransactions {
                    starting_id,
                    result_limit,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ExecutedTransactions(results)) => Ok(results),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::LastTransactionId,
            })
            .await?
        {
            Response::Database(DatabaseResponse::LastTransactionId(result)) => Ok(result),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Compact,
            })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::CompactKeyValueStore,
            })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}

#[async_trait]
impl AsyncLowLevelConnection for RemoteDatabase {
    fn schematic(&self) -> &Schematic {
        &self.schema
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::ApplyTransaction { transaction },
            })
            .await?
        {
            Response::Database(DatabaseResponse::TransactionResults(results)) => Ok(results),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Get {
                    collection: collection.clone(),
                    id,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::Documents(documents)) => {
                Ok(documents.into_iter().next())
            }
            Response::Error(bonsaidb_core::Error::DocumentNotFound(_, _)) => Ok(None),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::List {
                    collection: collection.clone(),
                    ids,
                    order,
                    limit,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::Documents(documents)) => Ok(documents),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Count {
                    collection: collection.clone(),
                    ids,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::Count(count)) => Ok(count),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::GetMultiple {
                    collection: collection.clone(),
                    ids: ids.to_vec(),
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::Documents(documents)) => Ok(documents),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::CompactCollection {
                    name: collection.clone(),
                },
            })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Query {
                    view: view.clone(),
                    key,
                    order,
                    limit,
                    access_policy,
                    with_docs: false,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewMappings(mappings)) => Ok(mappings),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Query {
                    view: view.clone(),
                    key,
                    order,
                    limit,
                    access_policy,
                    with_docs: true,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewMappingsWithDocs(mappings)) => Ok(mappings),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Reduce {
                    view: view.clone(),
                    key,
                    access_policy,
                    grouped: false,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewReduction(value)) => Ok(value.0),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Reduce {
                    view: view.clone(),
                    key,
                    access_policy,
                    grouped: true,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewGroupedReduction(values)) => Ok(values),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::DeleteDocs {
                    view: view.clone(),
                    key,
                    access_policy,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::Count(count)) => Ok(count),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}
