//! Lightweight async `PubSub` framework.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(clippy::option_if_let_else)]

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_lock::RwLock;
pub use flume;
use serde::{Deserialize, Serialize};

/// A `PubSub` message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// The topic of the message.
    pub topic: String,
    /// The payload of the message.
    pub payload: Vec<u8>,
}

impl Message {
    /// Creates a new message.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to serialize with `pot`.
    pub fn new<S: Into<String>, P: Serialize>(topic: S, payload: &P) -> Result<Self, pot::Error> {
        Ok(Self {
            topic: topic.into(),
            payload: pot::to_vec(payload)?,
        })
    }

    /// Deserialize the payload as `P` using `pot`.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to deserialize with `pot`.
    pub fn payload<P: for<'de> Deserialize<'de>>(&self) -> Result<P, pot::Error> {
        pot::from_slice(&self.payload).map_err(pot::Error::from)
    }
}

type TopicId = u64;
type SubscriberId = u64;

#[derive(Default, Debug, Clone)]
/// Manages subscriptions and notifications for `PubSub`.
pub struct Relay {
    data: Arc<Data>,
}

#[derive(Debug, Default)]
struct Data {
    subscribers: RwLock<HashMap<SubscriberId, SubscriberInfo>>,
    topics: RwLock<HashMap<String, TopicId>>,
    subscriptions: RwLock<HashMap<TopicId, HashSet<SubscriberId>>>,
    last_topic_id: AtomicU64,
    last_subscriber_id: AtomicU64,
}

impl Relay {
    /// Create a new [`Subscriber`] for this relay.
    pub async fn create_subscriber(&self) -> Subscriber {
        let mut subscribers = self.data.subscribers.write().await;
        let id = self.data.last_subscriber_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = flume::unbounded();
        subscribers.insert(
            id,
            SubscriberInfo {
                sender,
                topics: HashSet::default(),
            },
        );
        Subscriber {
            data: Arc::new(SubscriberData {
                id,
                receiver,
                relay: self.clone(),
                #[cfg(not(target_arch = "wasm32"))]
                tokio: tokio::runtime::Handle::current(),
            }),
        }
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to serialize with `pot`.
    pub async fn publish<S: Into<String> + Send, P: Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), pot::Error> {
        let message = Message::new(topic, payload)?;
        self.publish_message(message).await;
        Ok(())
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to serialize with `pot`.
    pub async fn publish_to_all<P: Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), pot::Error> {
        let tasks = topics
            .into_iter()
            .map(|topic| Message::new(topic, payload).map(|message| self.publish_message(message)))
            .collect::<Result<Vec<_>, _>>()?;
        futures::future::join_all(tasks).await;

        Ok(())
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    pub async fn publish_serialized_to_all(&self, topics: Vec<String>, payload: Vec<u8>) {
        let tasks = topics.into_iter().map(|topic| {
            self.publish_message(Message {
                topic,
                payload: payload.clone(),
            })
        });
        futures::future::join_all(tasks).await;
    }

    /// Publishes a message to all subscribers of its topic.
    pub async fn publish_message(&self, message: Message) {
        if let Some(topic_id) = self.topic_id(&message.topic).await {
            self.post_message_to_topic(message, topic_id).await;
        }
    }

    async fn add_subscriber_to_topic(&self, subscriber_id: u64, topic: String) {
        let mut subscribers = self.data.subscribers.write().await;
        let mut topics = self.data.topics.write().await;
        let mut subscriptions = self.data.subscriptions.write().await;

        // Lookup or create a topic id
        let topic_id = *topics
            .entry(topic)
            .or_insert_with(|| self.data.last_topic_id.fetch_add(1, Ordering::SeqCst));
        if let Some(subscriber) = subscribers.get_mut(&subscriber_id) {
            subscriber.topics.insert(topic_id);
        }
        let subscribers = subscriptions
            .entry(topic_id)
            .or_insert_with(HashSet::default);
        subscribers.insert(subscriber_id);
    }

    async fn remove_subscriber_from_topic(&self, subscriber_id: u64, topic: &str) {
        let mut subscribers = self.data.subscribers.write().await;
        let mut topics = self.data.topics.write().await;

        let remove_topic = if let Some(topic_id) = topics.get(topic) {
            if let Some(subscriber) = subscribers.get_mut(&subscriber_id) {
                if !subscriber.topics.remove(topic_id) {
                    // subscriber isn't subscribed to this topic
                    return;
                }
            } else {
                // subscriber_id is not subscribed anymore
                return;
            }

            let mut subscriptions = self.data.subscriptions.write().await;
            let remove_topic = if let Some(subscriptions) = subscriptions.get_mut(topic_id) {
                subscriptions.remove(&subscriber_id);
                subscriptions.is_empty()
            } else {
                // Shouldn't be reachable, but this allows cleanup to proceed if it does.
                true
            };

            if remove_topic {
                subscriptions.remove(topic_id);
                true
            } else {
                false
            }
        } else {
            false
        };
        if remove_topic {
            topics.remove(topic);
        }
    }

    async fn topic_id(&self, topic: &str) -> Option<TopicId> {
        let topics = self.data.topics.read().await;
        topics.get(topic).copied()
    }

    async fn post_message_to_topic(&self, message: Message, topic: TopicId) {
        let failures = {
            // For an optimal-flow case, we're going to acquire read permissions
            // only, allowing messages to be posted in parallel. This block is
            // responsible for returning `SubscriberId`s that failed to send.
            let subscribers = self.data.subscribers.read().await;
            let subscriptions = self.data.subscriptions.read().await;
            if let Some(registered) = subscriptions.get(&topic) {
                let message = Arc::new(message);
                let failures = futures::future::join_all(registered.iter().filter_map(|id| {
                    subscribers.get(id).map(|subscriber| {
                        let message = message.clone();
                        async move { (*id, subscriber.sender.send_async(message).await.is_ok()) }
                    })
                }))
                .await
                .into_iter()
                .filter_map(|(id, sent_message)| if sent_message { None } else { Some(id) })
                .collect::<Vec<SubscriberId>>();

                failures
            } else {
                return;
            }
        };

        if !failures.is_empty() {
            for failed in failures {
                self.unsubscribe_all(failed).await;
            }
        }
    }

    async fn unsubscribe_all(&self, subscriber_id: SubscriberId) {
        let mut subscribers = self.data.subscribers.write().await;
        let mut topics = self.data.topics.write().await;
        let mut subscriptions = self.data.subscriptions.write().await;
        if let Some(subscriber) = subscribers.remove(&subscriber_id) {
            for topic in &subscriber.topics {
                let remove = if let Some(subscriptions) = subscriptions.get_mut(topic) {
                    subscriptions.remove(&subscriber_id);
                    subscriptions.is_empty()
                } else {
                    false
                };

                if remove {
                    subscriptions.remove(topic);
                    topics.retain(|_name, id| id != topic);
                }
            }
        }
    }
}

#[derive(Debug)]
struct SubscriberInfo {
    sender: flume::Sender<Arc<Message>>,
    topics: HashSet<u64>,
}

/// A subscriber for [`Message`]s published to subscribed topics.
#[derive(Debug, Clone)]
pub struct Subscriber {
    data: Arc<SubscriberData>,
}

impl Subscriber {
    /// Subscribe to [`Message`]s published to `topic`.
    pub async fn subscribe_to<S: Into<String> + Send>(&self, topic: S) {
        self.data
            .relay
            .add_subscriber_to_topic(self.data.id, topic.into())
            .await;
    }

    /// Unsubscribe from [`Message`]s published to `topic`.
    pub async fn unsubscribe_from(&self, topic: &str) {
        self.data
            .relay
            .remove_subscriber_from_topic(self.data.id, topic)
            .await;
    }

    /// Returns the receiver to receive [`Message`]s.
    #[must_use]
    pub fn receiver(&self) -> &'_ flume::Receiver<Arc<Message>> {
        &self.data.receiver
    }

    #[must_use]
    /// Returns the unique ID of the subscriber.
    pub fn id(&self) -> u64 {
        self.data.id
    }
}

#[derive(Debug)]
struct SubscriberData {
    id: SubscriberId,
    relay: Relay,
    receiver: flume::Receiver<Arc<Message>>,
    #[cfg(not(target_arch = "wasm32"))]
    tokio: tokio::runtime::Handle,
}

impl Drop for SubscriberData {
    fn drop(&mut self) {
        let id = self.id;
        let relay = self.relay.clone();
        let drop_task = async move {
            relay.unsubscribe_all(id).await;
        };
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(drop_task);
        #[cfg(not(target_arch = "wasm32"))]
        self.tokio.spawn(drop_task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simple_pubsub_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber = pubsub.create_subscriber().await;
        subscriber.subscribe_to("mytopic").await;
        pubsub.publish("mytopic", &String::from("test")).await?;
        let receiver = subscriber.receiver().clone();
        let message = receiver.recv_async().await.expect("No message received");
        assert_eq!(message.topic, "mytopic");
        assert_eq!(message.payload::<String>()?, "test");
        // The message should only be received once.
        assert!(matches!(
            tokio::task::spawn_blocking(
                move || receiver.recv_timeout(std::time::Duration::from_millis(100))
            )
            .await,
            Ok(Err(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn multiple_subscribers_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber_a = pubsub.create_subscriber().await;
        let subscriber_ab = pubsub.create_subscriber().await;
        subscriber_a.subscribe_to("a").await;
        subscriber_ab.subscribe_to("a").await;
        subscriber_ab.subscribe_to("b").await;

        pubsub.publish("a", &String::from("a1")).await?;
        pubsub.publish("b", &String::from("b1")).await?;
        pubsub.publish("a", &String::from("a2")).await?;

        // Check subscriber_a for a1 and a2.
        let message = subscriber_a.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber_a.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a2");

        let message = subscriber_ab.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber_ab.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "b1");
        let message = subscriber_ab.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a2");

        Ok(())
    }

    #[tokio::test]
    async fn unsubscribe_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber = pubsub.create_subscriber().await;
        subscriber.subscribe_to("a").await;

        pubsub.publish("a", &String::from("a1")).await?;
        subscriber.unsubscribe_from("a").await;
        pubsub.publish("a", &String::from("a2")).await?;
        subscriber.subscribe_to("a").await;
        pubsub.publish("a", &String::from("a3")).await?;

        // Check subscriber_a for a1 and a2.
        let message = subscriber.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a3");

        Ok(())
    }

    #[tokio::test]
    async fn drop_and_send_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber_a = pubsub.create_subscriber().await;
        let subscriber_to_drop = pubsub.create_subscriber().await;
        subscriber_a.subscribe_to("a").await;
        subscriber_to_drop.subscribe_to("a").await;

        pubsub.publish("a", &String::from("a1")).await?;
        drop(subscriber_to_drop);
        pubsub.publish("a", &String::from("a2")).await?;

        // Check subscriber_a for a1 and a2.
        let message = subscriber_a.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber_a.receiver().recv_async().await?;
        assert_eq!(message.payload::<String>()?, "a2");

        let subscribers = pubsub.data.subscribers.read().await;
        assert_eq!(subscribers.len(), 1);
        let topics = pubsub.data.topics.read().await;
        let topic_id = topics.get("a").expect("topic not found");
        let subscriptions = pubsub.data.subscriptions.read().await;
        assert_eq!(
            subscriptions
                .get(topic_id)
                .expect("subscriptions not found")
                .len(),
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn drop_cleanup_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber = pubsub.create_subscriber().await;
        subscriber.subscribe_to("a").await;
        drop(subscriber);
        // Drop spawns a task that cleans up. Give it a moment to execute.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let subscribers = pubsub.data.subscribers.read().await;
        assert_eq!(subscribers.len(), 0);
        let subscriptions = pubsub.data.subscriptions.read().await;
        assert_eq!(subscriptions.len(), 0);
        let topics = pubsub.data.topics.read().await;
        assert_eq!(topics.len(), 0);

        Ok(())
    }
}
