# Key-Value Trait

The [`KeyValue` trait][keyvalue] contains functions for interacting the atomic key-value store. The key-value store provides high-performance atomic operations without ACID compliance. Once the data is persisted to disk, it holds the same guarantees as all of BonsaiDb, but this feature is designed for high throughput and does not wait to persist to disk before reporting success to the client. This trait is implemented by the [`Database`](../about/concepts/database.md) types in each crate:

- For bonsaidb-local: [`Database`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Database.html)
- For bonsaidb-server: [`ServerDatabase`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerDatabase.html)
- For bonsaidb-client: [`BlockingRemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.BlockingRemoteDatabase.html)

Using this trait, you can write code that generically can work regardless of whether BonsaiDb is operationg locally with no network connection or across the globe.

This is an [async trait](https://crates.io/crates/async-trait), which unfortunately yields [messy documentation][keyvalue].

[keyvalue]: {{DOCS_BASE_URL}}/bonsaidb/core/keyvalue/trait.KeyValue.html
