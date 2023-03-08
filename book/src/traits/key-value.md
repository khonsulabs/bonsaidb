# Key-Value Trait

The [`KeyValue`][keyvalue]/[`AsyncKeyValue`][asynckeyvalue] traits contain functions for interacting the atomic key-value store. The key-value store provides high-performance atomic operations without ACID compliance. Once the data is persisted to disk, it holds the same guarantees as all of BonsaiDb, but this feature is designed for high throughput and does not wait to persist to disk before reporting success to the client. This trait is implemented by the [`Database`](../about/concepts/database.md) types in each crate:

- For bonsaidb-local: [`Database`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Database.html) / [`AsyncDatabase`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.AsyncDatabase.html)
- For bonsaidb-server: [`ServerDatabase`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerDatabase.html), and `Database` via [`ServerDatabase::as_blocking()`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerDatabase.html#method.as_blocking)
- For bonsaidb-client: [`BlockingRemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.BlockingRemoteDatabase.html) / [`AsyncRemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.AsyncRemoteDatabase.html)

Using these traits, you can write code that generically can work regardless of whether BonsaiDb is operationg locally with no network connection or across the globe.

The only differences between `KeyValue` and `AsyncKeyValue` is that
`AsyncKeyValue` is able to be used in async code and the `KeyValue` trait is
designed to block the current thread. BonsaiDb is designed to try to make it
hard to accidentally call a blocking function from async code accidentally,
while still supporting both async and blocking access patterns.

[keyvalue]: {{DOCS_BASE_URL}}/bonsaidb/core/keyvalue/trait.KeyValue.html
[asynckeyvalue]: {{DOCS_BASE_URL}}/bonsaidb/core/keyvalue/trait.AsyncKeyValue.html
