use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use bonsaidb_core::{
    connection::{AccessPolicy, Connection, QueryKey, Range, Sort},
    custom_api::CustomApi,
    document::Document,
    networking::{DatabaseRequest, DatabaseResponse, Request, Response},
    schema::{
        view::{self, map, SerializedView},
        Collection, Key, Map, MappedDocument, MappedValue, Schematic,
    },
    transaction::{Executed, OperationResult, Transaction},
};
use derive_where::DeriveWhere;

use crate::Client;

mod pubsub;
pub use pubsub::*;

mod keyvalue;

/// A database on a remote server.
#[derive(Debug, DeriveWhere)]
#[derive_where(Clone)]
pub struct RemoteDatabase<A: CustomApi = ()> {
    client: Client<A>,
    name: Arc<String>,
    schema: Arc<Schematic>,
}
impl<A: CustomApi> RemoteDatabase<A> {
    /// Returns the name of the database.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

impl<A: CustomApi> Deref for RemoteDatabase<A> {
    type Target = Client<A>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<A: CustomApi> RemoteDatabase<A> {
    pub(crate) fn new(client: Client<A>, name: String, schema: Arc<Schematic>) -> Self {
        Self {
            client,
            name: Arc::new(name),
            schema,
        }
    }
}

#[async_trait]
impl<A: CustomApi> Connection for RemoteDatabase<A> {
    async fn get<C: Collection>(
        &self,
        id: u64,
    ) -> Result<Option<Document<'static>>, bonsaidb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Get {
                    collection: C::collection_name()?,
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

    async fn get_multiple<C: Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Document<'static>>, bonsaidb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::GetMultiple {
                    collection: C::collection_name()?,
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

    async fn list<C: Collection, R: Into<Range<u64>> + Send>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Vec<Document<'static>>, bonsaidb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::List {
                    collection: C::collection_name()?,
                    ids: ids.into(),
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

    async fn query<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Query {
                    view: self
                        .schema
                        .view::<V>()
                        .ok_or(bonsaidb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    order,
                    limit,
                    access_policy,
                    with_docs: false,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewMappings(mappings)) => Ok(mappings
                .iter()
                .map(map::Serialized::deserialized::<V>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| bonsaidb_core::Error::Database(err.to_string()))?),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn query_with_docs<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedDocument<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Query {
                    view: self
                        .schema
                        .view::<V>()
                        .ok_or(bonsaidb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    order,
                    limit,
                    access_policy,
                    with_docs: true,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewMappingsWithDocs(mappings)) => Ok(mappings
                .into_iter()
                .map(map::MappedSerialized::deserialized::<V>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| bonsaidb_core::Error::Database(err.to_string()))?),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn reduce<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Reduce {
                    view: self
                        .schema
                        .view::<V>()
                        .ok_or(bonsaidb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    access_policy,
                    grouped: false,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewReduction(value)) => {
                let value = V::deserialize(&value)?;
                Ok(value)
            }
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn reduce_grouped<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Reduce {
                    view: self
                        .schema
                        .view::<V>()
                        .ok_or(bonsaidb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    access_policy,
                    grouped: true,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewGroupedReduction(values)) => values
                .into_iter()
                .map(|map| {
                    Ok(MappedValue {
                        key: V::Key::from_big_endian_bytes(&map.key).map_err(|err| {
                            bonsaidb_core::Error::Database(
                                view::Error::key_serialization(err).to_string(),
                            )
                        })?,
                        value: V::deserialize(&map.value)?,
                    })
                })
                .collect::<Result<Vec<_>, bonsaidb_core::Error>>(),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn delete_docs<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::DeleteDocs {
                    view: self
                        .schema
                        .view::<V>()
                        .ok_or(bonsaidb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    access_policy,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::DocumentsDeleted(count)) => Ok(count),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
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

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed>, bonsaidb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
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
            .send_request(Request::Database {
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

    async fn compact_collection<C: Collection>(&self) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::CompactCollection {
                    name: C::collection_name()?,
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

    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Database {
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
            .send_request(Request::Database {
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
