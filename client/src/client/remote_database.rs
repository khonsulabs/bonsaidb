use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use pliantdb_core::{
    circulate::Message,
    connection::{AccessPolicy, Connection, QueryKey},
    document::Document,
    kv::Kv,
    networking::{self, DatabaseRequest, DatabaseResponse, Request, Response},
    pubsub::{database_topic, PubSub, Subscriber},
    schema::{
        view, view::map, Collection, Key, Map, MappedDocument, MappedValue, Schema, Schematic, View,
    },
    transaction::{Executed, OperationResult, Transaction},
};
use serde::Serialize;

use crate::Client;

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
                .map_err(|err| pliantdb_core::Error::Storage(err.to_string()))?),
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
                .map_err(|err| pliantdb_core::Error::Storage(err.to_string()))?),
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
                            pliantdb_core::Error::Storage(
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

#[async_trait]
impl<DB> PubSub for RemoteDatabase<DB>
where
    DB: Schema,
{
    type Subscriber = RemoteSubscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, pliantdb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::CreateSubscriber,
            })
            .await?
        {
            Response::Database(DatabaseResponse::SubscriberCreated { subscriber_id }) => {
                let (sender, receiver) = flume::unbounded();
                self.client.register_subscriber(subscriber_id, sender).await;
                Ok(RemoteSubscriber {
                    client: self.client.clone(),
                    database: self.name.clone(),
                    id: subscriber_id,
                    receiver,
                })
            }
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn publish<S: Into<String> + Send, P: Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), pliantdb_core::Error> {
        let payload = serde_cbor::to_vec(&payload)?;
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Publish {
                    topic: topic.into(),
                    payload,
                },
            })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn publish_to_all<P: Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), pliantdb_core::Error> {
        let payload = serde_cbor::to_vec(&payload)?;
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::PublishToAll { topics, payload },
            })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}

#[derive(Debug)]
pub struct RemoteSubscriber {
    client: Client,
    database: Arc<String>,
    id: u64,
    receiver: flume::Receiver<Arc<Message>>,
}

#[async_trait]
impl Subscriber for RemoteSubscriber {
    async fn subscribe_to<S: Into<String> + Send>(
        &self,
        topic: S,
    ) -> Result<(), pliantdb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.database.to_string(),
                request: DatabaseRequest::SubscribeTo {
                    subscriber_id: self.id,
                    topic: database_topic(&self.database, &topic.into()),
                },
            })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn unsubscribe_from(&self, topic: &str) -> Result<(), pliantdb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.database.to_string(),
                request: DatabaseRequest::UnsubscribeFrom {
                    subscriber_id: self.id,
                    topic: database_topic(&self.database, topic),
                },
            })
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    fn receiver(&self) -> &'_ flume::Receiver<Arc<pliantdb_core::circulate::Message>> {
        &self.receiver
    }
}

impl Drop for RemoteSubscriber {
    fn drop(&mut self) {
        let client = self.client.clone();
        let database = self.database.to_string();
        let subscriber_id = self.id;
        tokio::spawn(async move {
            client.unregister_subscriber(database, subscriber_id).await;
        });
    }
}

#[async_trait]
impl<DB> Kv for RemoteDatabase<DB>
where
    DB: Schema,
{
    async fn execute_key_operation(
        &self,
        op: pliantdb_core::kv::KeyOperation,
    ) -> Result<pliantdb_core::kv::Output, pliantdb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::ExecuteKeyOperation(op),
            })
            .await?
        {
            Response::Database(DatabaseResponse::KvOutput(output)) => Ok(output),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}
