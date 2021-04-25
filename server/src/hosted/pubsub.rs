use async_trait::async_trait;
use pliantdb_local::core::{
    self, circulate,
    pubsub::{self, database_topic, PubSub},
    schema::Schema,
};

#[async_trait]
impl<'a, 'b, DB> PubSub for super::Database<'a, 'b, DB>
where
    DB: Schema,
{
    type Subscriber = Subscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, core::Error> {
        let subscriber = self.server.relay().create_subscriber().await;

        Ok(Subscriber {
            database_name: self.name.to_string(),
            subscriber,
        })
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), core::Error> {
        self.server
            .relay()
            .publish(database_topic(self.name, &topic.into()), payload)
            .await?;

        Ok(())
    }

    async fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), core::Error> {
        self.server
            .relay()
            .publish_to_all(
                topics
                    .iter()
                    .map(|topic| database_topic(self.name, topic))
                    .collect(),
                payload,
            )
            .await?;

        Ok(())
    }
}

pub struct Subscriber {
    database_name: String,
    subscriber: circulate::Subscriber,
}

#[async_trait]
impl pubsub::Subscriber for Subscriber {
    async fn subscribe_to<S: Into<String> + Send>(&self, topic: S) -> Result<(), core::Error> {
        self.subscriber
            .subscribe_to(database_topic(&self.database_name, &topic.into()))
            .await;
        Ok(())
    }

    async fn unsubscribe_from(&self, topic: &str) -> Result<(), core::Error> {
        let topic = format!("{}\u{0}{}", self.database_name, topic);
        self.subscriber.unsubscribe_from(&topic).await;
        Ok(())
    }

    fn receiver(&self) -> &'_ flume::Receiver<std::sync::Arc<circulate::Message>> {
        self.subscriber.receiver()
    }
}
