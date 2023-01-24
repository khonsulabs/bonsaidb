use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use bonsaidb_core::connection::{
    AccessPolicy, AsyncConnection, AsyncLowLevelConnection, HasSchema, HasSession, Range,
    SerializedQueryKey, Session, Sort,
};
use bonsaidb_core::document::{DocumentId, Header, OwnedDocument};
use bonsaidb_core::networking::{
    ApplyTransaction, Compact, CompactCollection, CompactKeyValueStore, Count, DeleteDocs, Get,
    GetMultiple, LastTransactionId, List, ListExecutedTransactions, ListHeaders, Query,
    QueryWithDocs, Reduce, ReduceGrouped,
};
use bonsaidb_core::schema::view::map::MappedSerializedValue;
use bonsaidb_core::schema::{self, CollectionName, Schematic, ViewName};
use bonsaidb_core::transaction::{Executed, OperationResult, Transaction};

use crate::Client;

mod pubsub;
pub use pubsub::*;

mod keyvalue;

/// A database on a remote server.
#[derive(Debug, Clone)]
pub struct RemoteDatabase {
    pub(crate) client: Client,
    pub(crate) name: Arc<String>,
    pub(crate) schema: Arc<Schematic>,
}
impl RemoteDatabase {
    /// Returns the name of the database.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_ref()
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

impl HasSession for RemoteDatabase {
    fn session(&self) -> Option<&Session> {
        Some(&self.session)
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
        Ok(self
            .client
            .send_api_request_async(&ListExecutedTransactions {
                database: self.name.to_string(),
                starting_id,
                result_limit,
            })
            .await?)
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&LastTransactionId {
                database: self.name.to_string(),
            })
            .await?)
    }

    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request_async(&Compact {
            database: self.name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request_async(&CompactKeyValueStore {
            database: self.name.to_string(),
        })
        .await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncLowLevelConnection for RemoteDatabase {
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&ApplyTransaction {
                database: self.name.to_string(),
                transaction,
            })
            .await?)
    }

    async fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&Get {
                database: self.name.to_string(),
                collection: collection.clone(),
                id,
            })
            .await?)
    }

    async fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&GetMultiple {
                database: self.name.to_string(),
                collection: collection.clone(),
                ids: ids.to_vec(),
            })
            .await?)
    }

    async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&List {
                database: self.name.to_string(),
                collection: collection.clone(),
                ids,
                order,
                limit,
            })
            .await?)
    }

    async fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&ListHeaders(List {
                database: self.name.to_string(),
                collection: collection.clone(),
                ids,
                order,
                limit,
            }))
            .await?)
    }

    async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&Count {
                database: self.name.to_string(),
                collection: collection.clone(),
                ids,
            })
            .await?)
    }

    async fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request_async(&CompactCollection {
            database: self.name.to_string(),
            name: collection,
        })
        .await?;
        Ok(())
    }

    async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&Query {
                database: self.name.to_string(),
                view: view.clone(),
                key,
                order,
                limit,
                access_policy,
            })
            .await?)
    }

    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&QueryWithDocs(Query {
                database: self.name.to_string(),
                view: view.clone(),
                key,
                order,
                limit,
                access_policy,
            }))
            .await?)
    }

    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&Reduce {
                database: self.name.to_string(),
                view: view.clone(),
                key,
                access_policy,
            })
            .await?
            .into_vec())
    }

    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&ReduceGrouped(Reduce {
                database: self.name.to_string(),
                view: view.clone(),
                key,
                access_policy,
            }))
            .await?)
    }

    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&DeleteDocs {
                database: self.name.to_string(),
                view: view.clone(),
                key,
                access_policy,
            })
            .await?)
    }
}

impl HasSchema for RemoteDatabase {
    fn schematic(&self) -> &Schematic {
        &self.schema
    }
}
