use std::sync::Arc;

use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    circulate::Message,
    networking::{DatabaseRequest, DatabaseResponse, Request, Response},
    pubsub::{AsyncPubSub, AsyncSubscriber},
};
use serde::Serialize;

use crate::Client;

#[async_trait]
impl AsyncPubSub for super::RemoteDatabase {
    type Subscriber = RemoteSubscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
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
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn publish<S: Into<String> + Send, P: Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        let payload = pot::to_vec(&payload)?;
        self.publish_bytes(topic, payload).await
    }

    async fn publish_bytes<S: Into<String> + Send>(
        &self,
        topic: S,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::Publish {
                    topic: topic.into(),
                    payload: Bytes::from(payload),
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

    async fn publish_to_all<P: Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        let payload = pot::to_vec(&payload)?;
        self.publish_bytes_to_all(topics, payload).await
    }

    async fn publish_bytes_to_all(
        &self,
        topics: Vec<String>,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::PublishToAll {
                    topics,
                    payload: Bytes::from(payload),
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
}

/// A `PubSub` subscriber from a remote server.
#[derive(Debug)]
pub struct RemoteSubscriber {
    client: Client,
    database: Arc<String>,
    id: u64,
    receiver: flume::Receiver<Arc<Message>>,
}

impl RemoteSubscriber {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn tokio(&self) -> &tokio::runtime::Handle {
        self.client.tokio()
    }
}

#[async_trait]
impl AsyncSubscriber for RemoteSubscriber {
    async fn subscribe_to<S: Into<String> + Send>(
        &self,
        topic: S,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.database.to_string(),
                request: DatabaseRequest::SubscribeTo {
                    subscriber_id: self.id,
                    topic: topic.into(),
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

    async fn unsubscribe_from(&self, topic: &str) -> Result<(), bonsaidb_core::Error> {
        match self
            .client
            .send_request_async(Request::Database {
                database: self.database.to_string(),
                request: DatabaseRequest::UnsubscribeFrom {
                    subscriber_id: self.id,
                    topic: topic.to_string(),
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

    fn receiver(&self) -> &'_ flume::Receiver<Arc<bonsaidb_core::circulate::Message>> {
        &self.receiver
    }
}

impl Drop for RemoteSubscriber {
    fn drop(&mut self) {
        let client = self.client.clone();
        let database = self.database.to_string();
        let subscriber_id = self.id;
        let drop_future = async move {
            client.unregister_subscriber(database, subscriber_id).await;
        };
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(drop_future);
        #[cfg(not(target_arch = "wasm32"))]
        tokio::spawn(drop_future);
    }
}
