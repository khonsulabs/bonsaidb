# BonsaiDb Client

Networked client for `bonsaidb-server`.

This crate supports two methods for accessing a BonsaiDb server: QUIC and
WebSockets.

QUIC is a new protocol built atop UDP. It is designed to operate more
reliably than TCP, and features TLS built-in at the protocol level.
WebSockets are an established protocol built atop TCP and HTTP.

[`Client`](https://dev.bonsaidb.io/main/docs/bonsaidb_client/struct.Client.html) provides access to BonsaiDb by implementing the
[`StorageConnection`](https://dev.bonsaidb.io/main/docs/bonsaidb/core/connection/trait.StorageConnection.html) trait.

## Minimum Supported Rust Version (MSRV)

While this project is alpha, we are actively adopting the current version of
Rust. The current minimum version is `1.64`.

## WASM Support

This crate supports compiling to WebAssembly. When using WebAssembly, the
only protocol available is WebSockets.

### Testing Websockets in a Browser

We have a fully deployed [example
application](https://github.com/khonsulabs/minority-game) available that can be
downloaded and run locally. This example is confirmed to work using Firefox on a
local machine, or any modern browser when accessing
[https://minority-game.gooey.rs/](https://minority-game.gooey.rs/).

- Ensure that the browser is able to talk to the IP/port that you're bound to.
  Browsers try to prevent malicious scripts. You may need to bind `0.0.0.0`
  instead of `localhost`, for example, to circumvent these protections.
- All Chrome-based browsers [require secure
  websockets](https://stackoverflow.com/a/50861413/457) over localhost. This
  makes Chrome not a great candidate for testing WASM applications locally, as
  installing a valid certificate in a test/dev environment can be tricky and/or
  annoying.

## Feature Flags

By default, the `full` feature is enabled.

- `full`: Enables `trusted-dns` and `websockets`
- `trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.
- `tracing`: Enables `tracing` annotations on some functions and dependencies.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
