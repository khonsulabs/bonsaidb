BonsaiDb's networked database implementation.

This crate implements BonsaiDb's networked database implementation. The
[`Server`]($server-alias$) and [`CustomServer<Backend>`]($customserver-type$)
types provide their most common functionality by implementing the
[`StorageConnection`]($storage-connection-trait$).

This crate supports two methods for exposing a BonsaiDb server: QUIC and
WebSockets.

QUIC is a new protocol built atop UDP. It is designed to operate more
reliably than TCP, and features TLS built-in at the protocol level.
WebSockets are an established protocol built atop TCP and HTTP.

Our user's guide has a section covering [setting up and accessing a BonsaiDb
server]($pages-base$/guide/integration/server.html).

## Minimum Supported Rust Version (MSRV)

While this project is alpha, we are actively adopting the current version of
Rust. The current minimum version is `1.60`.
