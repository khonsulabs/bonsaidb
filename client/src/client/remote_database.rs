use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use pliantdb_core::{
    connection::{AccessPolicy, Connection, QueryKey},
    document::Document,
    networking::{self, DatabaseRequest, DatabaseResponse, Request, Response},
    schema::{
        view, view::map, Collection, Key, Map, MappedDocument, MappedValue, Schema, Schematic, View,
    },
    transaction::{Executed, OperationResult, Transaction},
};

use crate::Client;

#[cfg(feature = "pubsub")]
mod pubsub;
#[cfg(feature = "pubsub")]
pub use pubsub::*;

#[cfg(feature = "keyvalue")]
mod kv;

/// A database on a remote server.
#[derive(Debug)]
pub struct RemoteDatabase<DB: Schema> {
    client: Client,
    name: Arc<String>,
    schema: Arc<Schematic>,
    _phantom: PhantomData<DB>,
}

impl<DB: Schema> Clone for RemoteDatabase<DB> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
            _phantom: PhantomData::default(),
        }
    }
}

impl<DB: Schema> RemoteDatabase<DB> {
    pub(crate) fn new(client: Client, name: String, schema: Arc<Schematic>) -> Self {
        Self {
            client,
            name: Arc::new(name),
            schema,
            _phantom: PhantomData::default(),
        }
    }
}

#[async_trait]
impl<DB: Schema> Connection for RemoteDatabase<DB> {
    async fn get<C: Collection>(
        &self,
        id: u64,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error> {
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
            Response::Error(pliantdb_core::Error::DocumentNotFound(_, _)) => Ok(None),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn get_multiple<C: Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error> {
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
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn query<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, pliantdb_core::Error>
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
                        .ok_or(pliantdb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    access_policy,
                    with_docs: false,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewMappings(mappings)) => Ok(mappings
                .iter()
                .map(map::Serialized::deserialized::<V::Key, V::Value>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| pliantdb_core::Error::Database(err.to_string()))?),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn query_with_docs<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedDocument<V::Key, V::Value>>, pliantdb_core::Error>
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
                        .ok_or(pliantdb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    access_policy,
                    with_docs: true,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewMappingsWithDocs(mappings)) => Ok(mappings
                .into_iter()
                .map(networking::MappedDocument::deserialized::<V::Key, V::Value>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| pliantdb_core::Error::Database(err.to_string()))?),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn reduce<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, pliantdb_core::Error>
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
                        .ok_or(pliantdb_core::Error::CollectionNotFound)?
                        .view_name()?,
                    key: key.map(|key| key.serialized()).transpose()?,
                    access_policy,
                    grouped: false,
                },
            })
            .await?
        {
            Response::Database(DatabaseResponse::ViewReduction(value)) => {
                let value = serde_cbor::from_slice::<V::Value>(&value)?;
                Ok(value)
            }
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn reduce_grouped<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, pliantdb_core::Error>
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
                        .ok_or(pliantdb_core::Error::CollectionNotFound)?
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
                            pliantdb_core::Error::Database(
                                view::Error::KeySerialization(err).to_string(),
                            )
                        })?,
                        value: serde_cbor::from_slice(&map.value)?,
                    })
                })
                .collect::<Result<Vec<_>, pliantdb_core::Error>>(),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, pliantdb_core::Error> {
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
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed<'static>>, pliantdb_core::Error> {
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
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error> {
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
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}
