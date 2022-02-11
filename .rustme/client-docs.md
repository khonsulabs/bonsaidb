Networked client for `bonsaidb-server`.

This crate supports two methods for accessing a BonsaiDb server: QUIC and
WebSockets.

QUIC is a new protocol built atop UDP. It is designed to operate more
reliably than TCP, and features TLS built-in at the protocol level.
WebSockets are an established protocol built atop TCP and HTTP.

[`Client`]($client-type$) provides access to BonsaiDb by implementing the
[`StorageConnection`]($storage-connection-trait$) trait.

## WASM Support

This crate supports compiling to WebAssembly. When using WebAssembly, the
only protocol available is WebSockets.
