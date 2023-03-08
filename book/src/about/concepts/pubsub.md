# PubSub

The [Publish/Subscribe pattern](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) enables developers to design systems that produce and receive messages. It is implemented for BonsaiDb through the [`PubSub` and `Subscriber`](../../traits/pubsub.md) traits.

A common example of what PubSub enables is implementing a simple chat system. Each chat participant can subscribe to messages on the `chat` topic, and when any participant publishes a `chat` message, all subscribers will receive a copy of that message.

A working example of PubSub is available at [`examples/basic-local/examples/pubsub.rs`]({{REPO_BASE_URL}}/examples/basic-local/examples/pubsub.rs).
