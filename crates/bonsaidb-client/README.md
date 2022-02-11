# BonsaiDb Client

Networked client for `bonsaidb-server`.

This crate supports two methods for accessing a BonsaiDb server: QUIC and
WebSockets.

QUIC is a new protocol built atop UDP. It is designed to operate more
reliably than TCP, and features TLS built-in at the protocol level.
WebSockets are an established protocol built atop TCP and HTTP.

[`Client`](https://dev.bonsaidb.io/main/docs/bonsaidb_client/struct.Client.html) provides access to BonsaiDb by implementing the
[`StorageConnection`](https://dev.bonsaidb.io/main/docs/bonsaidb/core/connection/trait.StorageConnection.html) trait.

## WASM Support

This crate supports compiling to WebAssembly. When using WebAssembly, the
only protocol available is WebSockets.

## Feature Flags

By default, the `full` feature is enabled. These features are prefixed by
`client-` when being enabled from the omnibus `bonsaidb` crate.

- `full`: Enables `trusted-dns` and `websockets`
- `trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
- `tracing`: Enables `tracing` annotations on some functions and dependencies.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
