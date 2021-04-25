use std::sync::Arc;

use async_trait::async_trait;
use pliantdb_core::{
    circulate::Message,
    networking::{DatabaseRequest, DatabaseResponse, Request, Response},
    pubsub::{database_topic, PubSub, Subscriber},
    schema::Schema,
};
use serde::Serialize;

use crate::Client;

#[async_trait]
impl<DB> PubSub for super::RemoteDatabase<DB>
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
