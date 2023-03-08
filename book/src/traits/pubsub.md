# PubSub Trait

The [`PubSub`][pubsub]/[`AsyncPubSub`][asyncpubsub] traits contain functions for using [PubSub](../about/concepts/pubsub.md) in BonsaiDb. The traits are implemented by the [`Database`](../about/concepts/database.md) types in each crate:

- For bonsaidb-local: [`Database`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Database.html) / [`AsyncDatabase`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.AsyncDatabase.html)
- For bonsaidb-server: [`ServerDatabase`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerDatabase.html), and `Database` via [`ServerDatabase::as_blocking()`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerDatabase.html#method.as_blocking)
- For bonsaidb-client: [`BlockingRemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.BlockingRemoteDatabase.html) / [`AsyncRemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.AsyncRemoteDatabase.html)

Using these traits, you can write code that generically can work regardless of whether BonsaiDb is operationg locally with no network connection or across the globe.

The only differences between `PubSub` and `AsyncPubSub` is that
`AsyncPubSub` is able to be used in async code and the `PubSub` trait is
designed to block the current thread. BonsaiDb is designed to try to make it
hard to accidentally call a blocking function from async code accidentally,
while still supporting both async and blocking access patterns.

[pubsub]: {{DOCS_BASE_URL}}/bonsaidb/core/pubsub/trait.PubSub.html
[asyncpubsub]: {{DOCS_BASE_URL}}/bonsaidb/core/pubsub/trait.AsyncPubSub.html
