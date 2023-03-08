# StorageConnection

The [`StorageConnection`][storageconnection]/[`AsyncStorageConnection`][asyncstorageconnection] traits contain functions for interacting with BonsaiDb's multi-database storage. These traits are implemented by the [`Storage`](../about/concepts/storage.md) types.

Using these trait, you can write code that generically works with BonsaiDb's multi-database storage types regardless of whether BonsaiDb is operationg locally with no network connection or across the globe.

The only differences between `StorageConnection` and `AsyncStorageConnection` is that
`AsyncStorageConnection` is able to be used in async code and the `StorageConnection` trait is
designed to block the current thread. BonsaiDb is designed to try to make it
hard to accidentally call a blocking function from async code accidentally,
while still supporting both async and blocking access patterns.

[storageconnection]: {{DOCS_BASE_URL}}/bonsaidb/core/connection/trait.StorageConnection.html
[asyncstorageconnection]: {{DOCS_BASE_URL}}/bonsaidb/core/connection/trait.AsyncStorageConnection.html
