use async_trait::async_trait;
pub use pliantdb_core::circulate::Relay;
use pliantdb_core::{circulate::Subscriber, pubsub::PubSub, schema::Schema};

#[async_trait]
impl<DB> PubSub for super::Storage<DB>
where
    DB: Schema,
{
    type Subscriber = Subscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, pliantdb_core::Error> {
        Ok(self.data.relay.create_subscriber().await)
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), pliantdb_core::Error> {
        self.data.relay.publish(topic, payload).await?;
        Ok(())
    }

    async fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), pliantdb_core::Error> {
        self.data.relay.publish_to_all(topics, payload).await?;
        Ok(())
    }
}
