use std::sync::Arc;

use async_trait::async_trait;
use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::networking::{
    CreateSubscriber, Publish, PublishToAll, SubscribeTo, UnsubscribeFrom,
};
use bonsaidb_core::pubsub::{AsyncPubSub, AsyncSubscriber, Receiver};

use crate::Client;

#[async_trait]
impl AsyncPubSub for super::RemoteDatabase {
    type Subscriber = RemoteSubscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        let subscriber_id = self
            .client
            .send_api_request_async(&CreateSubscriber {
                database: self.name.to_string(),
            })
            .await?;

        let (sender, receiver) = flume::unbounded();
        self.client.register_subscriber(subscriber_id, sender);
        Ok(RemoteSubscriber {
            client: self.client.clone(),
            database: self.name.clone(),
            id: subscriber_id,
            receiver: Receiver::new(receiver),
            #[cfg(not(target_arch = "wasm32"))]
            tokio: tokio::runtime::Handle::try_current().ok().map(Arc::new),
        })
    }

    async fn publish_bytes(
        &self,
        topic: Vec<u8>,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.client
            .send_api_request_async(&Publish {
                database: self.name.to_string(),
                topic: Bytes::from(topic),
                payload: Bytes::from(payload),
            })
            .await?;
        Ok(())
    }

    async fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send + 'async_trait,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        let topics = topics.into_iter().map(Bytes::from).collect();
        self.client
            .send_api_request_async(&PublishToAll {
                database: self.name.to_string(),
                topics,
                payload: Bytes::from(payload),
            })
            .await?;
        Ok(())
    }
}

/// A `PubSub` subscriber from a remote server.
#[derive(Debug)]
pub struct RemoteSubscriber {
    pub(crate) client: Client,
    pub(crate) database: Arc<String>,
    pub(crate) id: u64,
    pub(crate) receiver: Receiver,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) tokio: Option<Arc<tokio::runtime::Handle>>,
}

#[async_trait]
impl AsyncSubscriber for RemoteSubscriber {
    async fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.client
            .send_api_request_async(&SubscribeTo {
                database: self.database.to_string(),
                subscriber_id: self.id,
                topic: Bytes::from(topic),
            })
            .await?;
        Ok(())
    }

    async fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), bonsaidb_core::Error> {
        self.client
            .send_api_request_async(&UnsubscribeFrom {
                database: self.database.to_string(),
                subscriber_id: self.id,
                topic: Bytes::from(topic),
            })
            .await?;
        Ok(())
    }

    fn receiver(&self) -> &Receiver {
        &self.receiver
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for RemoteSubscriber {
    fn drop(&mut self) {
        let client = self.client.clone();
        let database = self.database.to_string();
        let subscriber_id = self.id;
        let drop_future = async move {
            client
                .unregister_subscriber_async(database, subscriber_id)
                .await;
        };
        wasm_bindgen_futures::spawn_local(drop_future);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for RemoteSubscriber {
    fn drop(&mut self) {
        if let Some(tokio) = &self.tokio {
            let client = self.client.clone();
            let database = self.database.to_string();
            let subscriber_id = self.id;
            tokio.spawn(async move {
                client
                    .unregister_subscriber_async(database, subscriber_id)
                    .await;
            });
        } else {
            self.client
                .unregister_subscriber(self.database.to_string(), self.id);
        }
    }
}
