# Connection

The [`Connection`][connection]/[`AsyncConnection`][asyncconnection] traits
contain functions for interacting with collections in a database. These traits
are implemented by the [`Database`](../about/concepts/database.md) types in each
crate.

Using these trait, you can write code that generically can work regardless of
whether BonsaiDb is operationg locally with no network connection or across the
globe.

The only differences between `Connection` and `AsyncConnection` is that
`AsyncConnection` is able to be used in async code and the `Connection` trait is
designed to block the current thread. BonsaiDb is designed to try to make it
hard to accidentally call a blocking function from async code accidentally,
while still supporting both async and blocking access patterns.

[connection]: {{DOCS_BASE_URL}}/bonsaidb/core/connection/trait.Connection.html
[asyncconnection]: {{DOCS_BASE_URL}}/bonsaidb/core/connection/trait.AsyncConnection.html
