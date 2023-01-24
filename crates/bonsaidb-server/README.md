# BonsaiDb Server

BonsaiDb's networked database implementation.

This crate implements BonsaiDb's networked database implementation. The
[`Server`](https://dev.bonsaidb.io/main/docs/bonsaidb_server/type.Server.html) and [`CustomServer<Backend>`](https://dev.bonsaidb.io/main/docs/bonsaidb_server/struct.CustomServer.html)
types provide their most common functionality by implementing the
[`StorageConnection`](https://dev.bonsaidb.io/main/docs/bonsaidb/core/connection/trait.StorageConnection.html).

This crate supports two methods for exposing a BonsaiDb server: QUIC and
WebSockets.

QUIC is a new protocol built atop UDP. It is designed to operate more
reliably than TCP, and features TLS built-in at the protocol level.
WebSockets are an established protocol built atop TCP and HTTP.

Our user's guide has a section covering [setting up and accessing a BonsaiDb
server](https://dev.bonsaidb.io/main/guide/integration/server.html).

## Minimum Supported Rust Version (MSRV)

While this project is alpha, we are actively adopting the current version of
Rust. The current minimum version is `1.64`.

## Feature Flags

By default, the `full` feature is enabled.

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
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
