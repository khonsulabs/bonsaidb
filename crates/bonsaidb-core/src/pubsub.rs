use async_trait::async_trait;
use circulate::{flume, Message};
use serde::Serialize;

use crate::Error;

/// Publishes and Subscribes to messages on topics.
pub trait PubSub {
    /// The Subscriber type for this `PubSub` connection.
    type Subscriber: Subscriber;

    /// Create a new [`Subscriber`] for this relay.
    fn create_subscriber(&self) -> Result<Self::Subscriber, Error>;

    /// Publishes a `payload` to all subscribers of `topic`.
    fn publish<Topic: Serialize, Payload: Serialize>(
        &self,
        topic: &Topic,
        payload: &Payload,
    ) -> Result<(), Error> {
        self.publish_bytes(pot::to_vec(topic)?, pot::to_vec(payload)?)
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    fn publish_bytes(&self, topic: Vec<u8>, payload: Vec<u8>) -> Result<(), Error>;

    /// Publishes a `payload` to all subscribers of all `topics`.
    fn publish_to_all<
        'topics,
        Topics: IntoIterator<Item = &'topics Topic> + 'topics,
        Topic: Serialize + 'topics,
        Payload: Serialize,
    >(
        &self,
        topics: Topics,
        payload: &Payload,
    ) -> Result<(), Error> {
        let topics = topics
            .into_iter()
            .map(pot::to_vec)
            .collect::<Result<Vec<_>, _>>()?;
        self.publish_bytes_to_all(topics, pot::to_vec(payload)?)
    }

    /// Publishes a `payload` to all subscribers of all `topics`.
    fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send,
        payload: Vec<u8>,
    ) -> Result<(), Error>;
}

/// A subscriber to one or more topics.
pub trait Subscriber {
    /// Subscribe to [`Message`]s published to `topic`.
    fn subscribe_to<Topic: Serialize>(&self, topic: &Topic) -> Result<(), Error> {
        self.subscribe_to_bytes(pot::to_vec(topic)?)
    }

    /// Subscribe to [`Message`]s published to `topic`.
    fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), Error>;

    /// Unsubscribe from [`Message`]s published to `topic`.
    fn unsubscribe_from<Topic: Serialize>(&self, topic: &Topic) -> Result<(), Error> {
        self.unsubscribe_from_bytes(&pot::to_vec(topic)?)
    }

    /// Unsubscribe from [`Message`]s published to `topic`.
    fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), Error>;

    /// Returns the receiver to receive [`Message`]s.
    fn receiver(&self) -> &Receiver;
}

/// Publishes and Subscribes to messages on topics.
#[async_trait]
pub trait AsyncPubSub: Send + Sync {
    /// The Subscriber type for this `PubSub` connection.
    type Subscriber: AsyncSubscriber;

    /// Create a new [`Subscriber`] for this relay.
    async fn create_subscriber(&self) -> Result<Self::Subscriber, Error>;

    /// Publishes a `payload` to all subscribers of `topic`.
    async fn publish<Topic: Serialize + Send + Sync, Payload: Serialize + Send + Sync>(
        &self,
        topic: &Topic,
        payload: &Payload,
    ) -> Result<(), Error> {
        let topic = pot::to_vec(topic)?;
        let payload = pot::to_vec(payload)?;
        self.publish_bytes(topic, payload).await
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    async fn publish_bytes(&self, topic: Vec<u8>, payload: Vec<u8>) -> Result<(), Error>;

    /// Publishes a `payload` to all subscribers of all `topics`.
    async fn publish_to_all<
        'topics,
        Topics: IntoIterator<Item = &'topics Topic> + Send + 'topics,
        Topic: Serialize + Send + 'topics,
        Payload: Serialize + Send + Sync,
    >(
        &self,
        topics: Topics,
        payload: &Payload,
    ) -> Result<(), Error> {
        let topics = topics
            .into_iter()
            .map(|topic| pot::to_vec(topic))
            .collect::<Result<Vec<_>, _>>()?;
        self.publish_bytes_to_all(topics, pot::to_vec(payload)?)
            .await
    }

    /// Publishes a `payload` to all subscribers of all `topics`.
    async fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send + 'async_trait,
        payload: Vec<u8>,
    ) -> Result<(), Error>;
}

/// A subscriber to one or more topics.
#[async_trait]
pub trait AsyncSubscriber: Send + Sync {
    /// Subscribe to [`Message`]s published to `topic`.
    async fn subscribe_to<Topic: Serialize + Send + Sync>(
        &self,
        topic: &Topic,
    ) -> Result<(), Error> {
        self.subscribe_to_bytes(pot::to_vec(topic)?).await
    }

    /// Subscribe to [`Message`]s published to `topic`.
    async fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), Error>;

    /// Unsubscribe from [`Message`]s published to `topic`.
    async fn unsubscribe_from<Topic: Serialize + Send + Sync>(
        &self,
        topic: &Topic,
    ) -> Result<(), Error> {
        self.unsubscribe_from_bytes(&pot::to_vec(topic)?).await
    }

    /// Unsubscribe from [`Message`]s published to `topic`.
    async fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), Error>;

    /// Returns the receiver to receive [`Message`]s.
    fn receiver(&self) -> &Receiver;
}

/// Receiver of PubSub [`Message`]s.
#[derive(Clone, Debug)]
#[must_use]
pub struct Receiver {
    receiver: flume::Receiver<Message>,
    strip_database: bool,
}

impl Receiver {
    #[doc(hidden)]
    pub fn new_stripping_prefixes(receiver: flume::Receiver<Message>) -> Self {
        Self {
            receiver,
            strip_database: true,
        }
    }

    #[doc(hidden)]
    pub fn new(receiver: flume::Receiver<Message>) -> Self {
        Self {
            receiver,
            strip_database: false,
        }
    }

    /// Receive the next [`Message`]. Blocks the current thread until a message
    /// is available. If the receiver becomes disconnected, an error will be
    /// returned.
    pub fn receive(&self) -> Result<Message, Disconnected> {
        self.receiver
            .recv()
            .map(|message| self.remove_database_prefix(message))
            .map_err(|_| Disconnected)
    }

    /// Receive the next [`Message`]. Blocks the current task until a new
    /// message is available. If the receiver becomes disconnected, an error
    /// will be returned.
    pub async fn receive_async(&self) -> Result<Message, Disconnected> {
        self.receiver
            .recv_async()
            .await
            .map(|message| self.remove_database_prefix(message))
            .map_err(|_| Disconnected)
    }

    /// Try to receive the next [`Message`]. This function will not block, and
    /// only returns a message if one is already available.
    pub fn try_receive(&self) -> Result<Message, TryReceiveError> {
        self.receiver
            .try_recv()
            .map(|message| self.remove_database_prefix(message))
            .map_err(TryReceiveError::from)
    }

    fn remove_database_prefix(&self, mut message: Message) -> Message {
        if self.strip_database {
            if let Some(database_length) = message.topic.iter().position(|b| b == 0) {
                message.topic.0.read_bytes(database_length + 1).unwrap();
            }
        }

        message
    }
}

impl Iterator for Receiver {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.receive().ok()
    }
}

/// The [`Receiver`] was disconnected
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq)]
#[error("the receiver is disconnected")]
pub struct Disconnected;

/// An error occurred trying to receive a message.
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq)]
pub enum TryReceiveError {
    /// The receiver was disconnected
    #[error("the receiver is disconnected")]
    Disconnected,
    /// No message was avaiable
    #[error("the receiver was empty")]
    Empty,
}

impl From<flume::TryRecvError> for TryReceiveError {
    fn from(err: flume::TryRecvError) -> Self {
        match err {
            flume::TryRecvError::Empty => Self::Empty,
            flume::TryRecvError::Disconnected => Self::Disconnected,
        }
    }
}

/// Creates a topic for use in a server. This is an internal API, which is why
/// the documentation is hidden. This is an implementation detail, but both
/// Client and Server must agree on this format, which is why it lives in core.
#[doc(hidden)]
#[must_use]
pub fn database_topic(database: &str, topic: &[u8]) -> Vec<u8> {
    let mut namespaced_topic = Vec::with_capacity(database.len() + topic.len() + 1);

    namespaced_topic.extend(database.bytes());
    namespaced_topic.push(b'\0');
    namespaced_topic.extend(topic);

    namespaced_topic
}

/// Expands into a suite of pubsub unit tests using the passed type as the test harness.
#[cfg(feature = "test-util")]
#[macro_export]
macro_rules! define_async_pubsub_test_suite {
    ($harness:ident) => {
        #[cfg(test)]
        mod r#async_pubsub {
            use $crate::pubsub::{AsyncPubSub, AsyncSubscriber};

            use super::$harness;
            #[tokio::test]
            async fn simple_pubsub_test() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::PubSubSimple).await?;
                let pubsub = harness.connect().await?;
                let subscriber = AsyncPubSub::create_subscriber(&pubsub).await?;
                AsyncSubscriber::subscribe_to(&subscriber, &"mytopic").await?;
                AsyncPubSub::publish(&pubsub, &"mytopic", &String::from("test")).await?;
                AsyncPubSub::publish(&pubsub, &"othertopic", &String::from("test")).await?;
                let receiver = subscriber.receiver().clone();
                let message = receiver.receive_async().await.expect("No message received");
                assert_eq!(message.topic::<String>()?, "mytopic");
                assert_eq!(message.payload::<String>()?, "test");
                // The message should only be received once.
                assert!(matches!(
                    receiver.try_receive(),
                    Err($crate::pubsub::TryReceiveError::Empty)
                ));
                Ok(())
            }

            #[tokio::test]
            async fn multiple_subscribers_test() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::PubSubMultipleSubscribers)
                        .await?;
                let pubsub = harness.connect().await?;
                let subscriber_a = AsyncPubSub::create_subscriber(&pubsub).await?;
                let subscriber_ab = AsyncPubSub::create_subscriber(&pubsub).await?;
                AsyncSubscriber::subscribe_to(&subscriber_a, &"a").await?;
                AsyncSubscriber::subscribe_to(&subscriber_ab, &"a").await?;
                AsyncSubscriber::subscribe_to(&subscriber_ab, &"b").await?;

                let mut messages_a = Vec::new();
                let mut messages_ab = Vec::new();
                AsyncPubSub::publish(&pubsub, &"a", &String::from("a1")).await?;
                messages_a.push(
                    subscriber_a
                        .receiver()
                        .receive_async()
                        .await?
                        .payload::<String>()?,
                );
                messages_ab.push(
                    subscriber_ab
                        .receiver()
                        .receive_async()
                        .await?
                        .payload::<String>()?,
                );

                AsyncPubSub::publish(&pubsub, &"b", &String::from("b1")).await?;
                messages_ab.push(
                    subscriber_ab
                        .receiver()
                        .receive_async()
                        .await?
                        .payload::<String>()?,
                );

                AsyncPubSub::publish(&pubsub, &"a", &String::from("a2")).await?;
                messages_a.push(
                    subscriber_a
                        .receiver()
                        .receive_async()
                        .await?
                        .payload::<String>()?,
                );
                messages_ab.push(
                    subscriber_ab
                        .receiver()
                        .receive_async()
                        .await?
                        .payload::<String>()?,
                );

                assert_eq!(&messages_a[0], "a1");
                assert_eq!(&messages_a[1], "a2");

                assert_eq!(&messages_ab[0], "a1");
                assert_eq!(&messages_ab[1], "b1");
                assert_eq!(&messages_ab[2], "a2");

                Ok(())
            }

            #[tokio::test]
            async fn unsubscribe_test() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::PubSubUnsubscribe).await?;
                let pubsub = harness.connect().await?;
                let subscriber = AsyncPubSub::create_subscriber(&pubsub).await?;
                AsyncSubscriber::subscribe_to(&subscriber, &"a").await?;

                AsyncPubSub::publish(&pubsub, &"a", &String::from("a1")).await?;
                AsyncSubscriber::unsubscribe_from(&subscriber, &"a").await?;
                AsyncPubSub::publish(&pubsub, &"a", &String::from("a2")).await?;
                AsyncSubscriber::subscribe_to(&subscriber, &"a").await?;
                AsyncPubSub::publish(&pubsub, &"a", &String::from("a3")).await?;

                // Check subscriber_a for a1 and a2.
                let message = subscriber.receiver().receive_async().await?;
                assert_eq!(message.payload::<String>()?, "a1");
                let message = subscriber.receiver().receive_async().await?;
                assert_eq!(message.payload::<String>()?, "a3");

                Ok(())
            }

            #[tokio::test]
            async fn pubsub_drop_cleanup_test() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::PubSubDropCleanup).await?;
                let pubsub = harness.connect().await?;
                let subscriber = AsyncPubSub::create_subscriber(&pubsub).await?;
                AsyncSubscriber::subscribe_to(&subscriber, &"a").await?;

                AsyncPubSub::publish(&pubsub, &"a", &String::from("a1")).await?;
                let receiver = subscriber.receiver().clone();
                drop(subscriber);

                // The receiver should now be disconnected, but after receiving the
                // first message. For when we're testing network connections, we
                // need to insert a little delay here to allow the server to process
                // the drop.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                AsyncPubSub::publish(&pubsub, &"a", &String::from("a1")).await?;

                let message = receiver.receive_async().await?;
                assert_eq!(message.payload::<String>()?, "a1");
                let $crate::pubsub::Disconnected = receiver.receive_async().await.unwrap_err();

                Ok(())
            }

            #[tokio::test]
            async fn publish_to_all_test() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::PubSubPublishAll).await?;
                let pubsub = harness.connect().await?;
                let subscriber_a = AsyncPubSub::create_subscriber(&pubsub).await?;
                let subscriber_b = AsyncPubSub::create_subscriber(&pubsub).await?;
                let subscriber_c = AsyncPubSub::create_subscriber(&pubsub).await?;
                AsyncSubscriber::subscribe_to(&subscriber_a, &"1").await?;
                AsyncSubscriber::subscribe_to(&subscriber_b, &"1").await?;
                AsyncSubscriber::subscribe_to(&subscriber_b, &"2").await?;
                AsyncSubscriber::subscribe_to(&subscriber_c, &"2").await?;
                AsyncSubscriber::subscribe_to(&subscriber_a, &"3").await?;
                AsyncSubscriber::subscribe_to(&subscriber_c, &"3").await?;

                AsyncPubSub::publish_to_all(&pubsub, [&"1", &"2", &"3"], &String::from("1"))
                    .await?;

                // Each subscriber should get "1" twice on separate topics
                for subscriber in &[subscriber_a, subscriber_b, subscriber_c] {
                    let mut message_topics = Vec::new();
                    for _ in 0..2_u8 {
                        let message = subscriber.receiver().receive_async().await?;
                        assert_eq!(message.payload::<String>()?, "1");
                        message_topics.push(message.topic.clone());
                    }
                    assert!(matches!(
                        subscriber.receiver().try_receive(),
                        Err($crate::pubsub::TryReceiveError::Empty)
                    ));
                    assert!(message_topics[0] != message_topics[1]);
                }

                Ok(())
            }
        }
    };
}

/// Expands into a suite of pubsub unit tests using the passed type as the test harness.
#[cfg(feature = "test-util")]
#[macro_export]
macro_rules! define_blocking_pubsub_test_suite {
    ($harness:ident) => {
        #[cfg(test)]
        mod blocking_pubsub {
            use $crate::pubsub::{PubSub, Subscriber};

            use super::$harness;
            #[test]
            fn simple_pubsub_test() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::PubSubSimple)?;
                let pubsub = harness.connect()?;
                let subscriber = PubSub::create_subscriber(&pubsub)?;
                Subscriber::subscribe_to(&subscriber, &"mytopic")?;
                PubSub::publish(&pubsub, &"mytopic", &String::from("test"))?;
                PubSub::publish(&pubsub, &"othertopic", &String::from("test"))?;
                let receiver = subscriber.receiver().clone();
                let message = receiver.receive().expect("No message received");
                assert_eq!(message.topic::<String>()?, "mytopic");
                assert_eq!(message.payload::<String>()?, "test");
                // The message should only be received once.
                assert!(matches!(
                    receiver.try_receive(),
                    Err($crate::pubsub::TryReceiveError::Empty)
                ));
                Ok(())
            }

            #[test]
            fn multiple_subscribers_test() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::PubSubMultipleSubscribers)?;
                let pubsub = harness.connect()?;
                let subscriber_a = PubSub::create_subscriber(&pubsub)?;
                let subscriber_ab = PubSub::create_subscriber(&pubsub)?;
                Subscriber::subscribe_to(&subscriber_a, &"a")?;
                Subscriber::subscribe_to(&subscriber_ab, &"a")?;
                Subscriber::subscribe_to(&subscriber_ab, &"b")?;

                let mut messages_a = Vec::new();
                let mut messages_ab = Vec::new();
                PubSub::publish(&pubsub, &"a", &String::from("a1"))?;
                messages_a.push(subscriber_a.receiver().receive()?.payload::<String>()?);
                messages_ab.push(subscriber_ab.receiver().receive()?.payload::<String>()?);

                PubSub::publish(&pubsub, &"b", &String::from("b1"))?;
                messages_ab.push(subscriber_ab.receiver().receive()?.payload::<String>()?);

                PubSub::publish(&pubsub, &"a", &String::from("a2"))?;
                messages_a.push(subscriber_a.receiver().receive()?.payload::<String>()?);
                messages_ab.push(subscriber_ab.receiver().receive()?.payload::<String>()?);

                assert_eq!(&messages_a[0], "a1");
                assert_eq!(&messages_a[1], "a2");

                assert_eq!(&messages_ab[0], "a1");
                assert_eq!(&messages_ab[1], "b1");
                assert_eq!(&messages_ab[2], "a2");

                Ok(())
            }

            #[test]
            fn unsubscribe_test() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::PubSubUnsubscribe)?;
                let pubsub = harness.connect()?;
                let subscriber = PubSub::create_subscriber(&pubsub)?;
                Subscriber::subscribe_to(&subscriber, &"a")?;

                PubSub::publish(&pubsub, &"a", &String::from("a1"))?;
                Subscriber::unsubscribe_from(&subscriber, &"a")?;
                PubSub::publish(&pubsub, &"a", &String::from("a2"))?;
                Subscriber::subscribe_to(&subscriber, &"a")?;
                PubSub::publish(&pubsub, &"a", &String::from("a3"))?;

                // Check subscriber_a for a1 and a2.
                let message = subscriber.receiver().receive()?;
                assert_eq!(message.payload::<String>()?, "a1");
                let message = subscriber.receiver().receive()?;
                assert_eq!(message.payload::<String>()?, "a3");

                Ok(())
            }

            #[test]
            fn pubsub_drop_cleanup_test() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::PubSubDropCleanup)?;
                let pubsub = harness.connect()?;
                let subscriber = PubSub::create_subscriber(&pubsub)?;
                Subscriber::subscribe_to(&subscriber, &"a")?;

                PubSub::publish(&pubsub, &"a", &String::from("a1"))?;
                let receiver = subscriber.receiver().clone();
                drop(subscriber);

                // The receiver should now be disconnected, but after receiving the
                // first message. For when we're testing network connections, we
                // need to insert a little delay here to allow the server to process
                // the drop.
                std::thread::sleep(std::time::Duration::from_millis(100));

                PubSub::publish(&pubsub, &"a", &String::from("a1"))?;

                let message = receiver.receive()?;
                assert_eq!(message.payload::<String>()?, "a1");
                let $crate::pubsub::Disconnected = receiver.receive().unwrap_err();

                Ok(())
            }

            #[test]
            fn publish_to_all_test() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::PubSubPublishAll)?;
                let pubsub = harness.connect()?;
                let subscriber_a = PubSub::create_subscriber(&pubsub)?;
                let subscriber_b = PubSub::create_subscriber(&pubsub)?;
                let subscriber_c = PubSub::create_subscriber(&pubsub)?;
                Subscriber::subscribe_to(&subscriber_a, &"1")?;
                Subscriber::subscribe_to(&subscriber_b, &"1")?;
                Subscriber::subscribe_to(&subscriber_b, &"2")?;
                Subscriber::subscribe_to(&subscriber_c, &"2")?;
                Subscriber::subscribe_to(&subscriber_a, &"3")?;
                Subscriber::subscribe_to(&subscriber_c, &"3")?;

                PubSub::publish_to_all(&pubsub, [&"1", &"2", &"3"], &String::from("1"))?;

                // Each subscriber should get "1" twice on separate topics
                for subscriber in &[subscriber_a, subscriber_b, subscriber_c] {
                    let mut message_topics = Vec::new();
                    for _ in 0..2_u8 {
                        let message = subscriber.receiver().receive()?;
                        assert_eq!(message.payload::<String>()?, "1");
                        message_topics.push(message.topic.clone());
                    }
                    assert!(matches!(
                        subscriber.receiver().try_receive(),
                        Err($crate::pubsub::TryReceiveError::Empty)
                    ));
                    assert!(message_topics[0] != message_topics[1]);
                }

                Ok(())
            }
        }
    };
}
