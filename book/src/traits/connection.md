# Connection

The [`Connection` trait][connection] contains functions for interacting with collections in a database. This trait is implemented by the [`Database`](../about/concepts/database.md) types in each crate:

- For bonsaidb-local: [`Database`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Database.html)
- For bonsaidb-server: [`ServerDatabase`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerDatabase.html)
- For bonsaidb-client: [`RemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.RemoteDatabase.html)

Using this trait, you can write code that generically can work regardless of whether BonsaiDb is operationg locally with no network connection or across the globe.

This is an [async trait](https://crates.io/crates/async-trait), which unfortunately yields [messy documentation][connection] due to the lifetimes.

[connection]: {{DOCS_BASE_URL}}/bonsaidb/core/connection/trait.Connection.html
