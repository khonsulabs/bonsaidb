# BonsaiDb Server

BonsaiDb's networked database implementation.

This crate implements BonsaiDb's networked database implementation. The
[`Server`](https://docs.rs/bonsaidb-server/*/bonsaidb_server/type.Server.html) and [`CustomServer<Backend>`](https://docs.rs/bonsaidb-server/*/bonsaidb_server/struct.CustomServer.html)
types provide their most common functionality by implementing the
[`StorageConnection`](https://docs.rs/bonsaidb-core/*/bonsaidb_core/connection/trait.StorageConnection.html).

This crate supports two methods for exposing a BonsaiDb server: QUIC and
WebSockets.

QUIC is a new protocol built atop UDP. It is designed to operate more
reliably than TCP, and features TLS built-in at the protocol level.
WebSockets are an established protocol built atop TCP and HTTP.

Our user's guide has a section covering [setting up and accessing a BonsaiDb
server](https://dev.bonsaidb.io/v0.4.0/guide/integration/server.html).

## Minimum Supported Rust Version (MSRV)

While this project is alpha, we are actively adopting the current version of
Rust. The current minimum version is `1.58`, and we plan on updating the MSRV to
implement [namespaced
Features](https://github.com/khonsulabs/bonsaidb/issues/178) as soon as the
feature is released.

## Feature Flags

By default, the `full` feature is enabled. These features are prefixed by
`server-` when being enabled from the omnibus `bonsaidb` crate.

- `full`: Enables all the flags below,
- `acme`: Enables automtic certificate acquisition through ACME/LetsEncrypt.
- `cli`: Enables the `cli` module.
- `encryption`: Enables at-rest encryption.
- `hyper`: Enables convenience functions for upgrading websockets using `hyper`.
- `instrument`: Enables instrumenting with `tracing`.
- `pem`: Enables the ability to install a certificate using the PEM format.
- `websockets`: Enables `WebSocket` support.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
