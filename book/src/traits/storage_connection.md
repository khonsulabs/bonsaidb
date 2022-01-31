# StorageConnection

The [`StorageConnection` trait][storageconnection] contains functions for interacting with BonsaiDb's multi-database storage. This trait is implemented by these types:

- For bonsaidb-local: [`Storage`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Storage.html)
- For bonsaidb-server: [`CustomServer<Backend>`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.CustomServer.html) / [`Server`]({{DOCS_BASE_URL}}/bonsaidb/server/type.Server.html)
- For bonsaidb-client: [`Client`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.Client.html)

Using this trait, you can write code that generically can work regardless of whether BonsaiDb is operationg locally with no network connection or across the globe.

This is an [async trait](https://crates.io/crates/async-trait), which unfortunately yields [messy documentation][storageconnection].

[storageconnection]: {{DOCS_BASE_URL}}/bonsaidb/core/connection/trait.StorageConnection.html
