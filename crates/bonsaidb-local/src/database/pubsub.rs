use std::sync::Arc;

use async_trait::async_trait;
pub use bonsaidb_core::circulate::Relay;
use bonsaidb_core::{
    circulate,
    pubsub::{self, database_topic, PubSub},
    Error,
};

impl PubSub for super::Database {
    type Subscriber = Subscriber;

    fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        Ok(Subscriber {
            database_name: self.data.name.to_string(),
            subscriber: self.data.storage.relay().create_subscriber(),
        })
    }

    fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .relay()
            .publish(database_topic(&self.data.name, &topic.into()), payload)?;
        Ok(())
    }

    fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data.storage.relay().publish_to_all(
            topics
                .iter()
                .map(|topic| database_topic(&self.data.name, topic))
                .collect(),
            payload,
        )?;
        Ok(())
    }
}

/// A subscriber for `PubSub` messages.
pub struct Subscriber {
    database_name: String,
    subscriber: circulate::Subscriber,
}

#[async_trait]
impl pubsub::Subscriber for Subscriber {
    fn subscribe_to<S: Into<String> + Send>(&self, topic: S) -> Result<(), Error> {
        self.subscriber
            .subscribe_to(database_topic(&self.database_name, &topic.into()));
        Ok(())
    }

    fn unsubscribe_from(&self, topic: &str) -> Result<(), Error> {
        self.subscriber
            .unsubscribe_from(&database_topic(&self.database_name, topic));
        Ok(())
    }

    fn receiver(&self) -> &'_ flume::Receiver<Arc<circulate::Message>> {
        self.subscriber.receiver()
    }
}
