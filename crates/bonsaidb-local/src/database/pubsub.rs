use async_trait::async_trait;
pub use bonsaidb_core::circulate::Relay;
use bonsaidb_core::{
    arc_bytes::OwnedBytes,
    circulate,
    connection::Connection,
    permissions::bonsai::{
        database_resource_name, pubsub_topic_resource_name, BonsaiAction, DatabaseAction,
        PubSubAction,
    },
    pubsub::{self, database_topic, PubSub, Receiver},
    Error,
};

use crate::{Database, DatabaseNonBlocking};

impl PubSub for super::Database {
    type Subscriber = Subscriber;

    fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        self.check_permission(
            database_resource_name(self.name()),
            &BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::CreateSuscriber)),
        )?;
        Ok(self
            .storage()
            .instance
            .register_subscriber(self.session().and_then(|session| session.id), self.clone()))
    }

    fn publish_bytes(&self, topic: Vec<u8>, payload: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.check_permission(
            pubsub_topic_resource_name(self.name(), &topic),
            &BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::Publish)),
        )?;
        self.storage
            .instance
            .relay()
            .publish_raw(database_topic(&self.data.name, &topic), payload);
        Ok(())
    }

    fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage.instance.relay().publish_raw_to_all(
            topics
                .into_iter()
                .map(|topic| {
                    self.check_permission(
                        pubsub_topic_resource_name(self.name(), &topic),
                        &BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::Publish)),
                    )
                    .map(|_| OwnedBytes::from(database_topic(&self.data.name, &topic)))
                })
                .collect::<Result<Vec<_>, _>>()?,
            payload,
        );
        Ok(())
    }
}

/// A subscriber for `PubSub` messages.
#[derive(Debug, Clone)]
pub struct Subscriber {
    pub(crate) id: u64,
    pub(crate) database: Database,
    pub(crate) subscriber: circulate::Subscriber,
    pub(crate) receiver: Receiver,
}

impl Subscriber {
    /// Returns the unique id of the subscriber.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.database.storage().instance.unregister_subscriber(self);
    }
}

#[async_trait]
impl pubsub::Subscriber for Subscriber {
    fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), Error> {
        self.database.check_permission(
            pubsub_topic_resource_name(self.database.name(), &topic),
            &BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::SubscribeTo)),
        )?;
        self.subscriber
            .subscribe_to_raw(database_topic(self.database.name(), &topic));
        Ok(())
    }

    fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), Error> {
        self.database.check_permission(
            pubsub_topic_resource_name(self.database.name(), topic),
            &BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::UnsubscribeFrom)),
        )?;
        self.subscriber
            .unsubscribe_from_raw(&database_topic(self.database.name(), topic));
        Ok(())
    }

    fn receiver(&self) -> &Receiver {
        &self.receiver
    }
}
