# BonsaiDb Client

Networked client for `bonsaidb-server`.

This crate supports two methods for accessing a BonsaiDb server: QUIC and
WebSockets.

QUIC is a new protocol built atop UDP. It is designed to operate more
reliably than TCP, and features TLS built-in at the protocol level.
WebSockets are an established protocol built atop TCP and HTTP.

[`Client`] provides access to BonsaiDb by implementing the
[`StorageConnection`][::bonsaidb_core::connection::StorageConnection] trait.

## Connecting via QUIC

The URL scheme to connect via QUIC is `bonsaidb`. If no port is specified,
port 5645 is assumed.

### With a valid TLS certificate

```rust
let client = Client::build(Url::parse("bonsaidb://my-server.com")?)
    .finish()
    .await?;
```

### With a Self-Signed Pinned Certificate

When using `install_self_signed_certificate()`, clients will need the
contents of the `pinned-certificate.der` file within the database. It can be
specified when building the client:

```rust
let certificate =
    Certificate::from_der(std::fs::read("mydb.bonsaidb/pinned-certificate.der")?)?;
let client = Client::build(Url::parse("bonsaidb://localhost")?)
    .with_certificate(certificate)
    .finish()
    .await?;
```

## Connecting via WebSockets

WebSockets are built atop the HTTP protocol. There are two URL schemes for
WebSockets:

- `ws`: Insecure WebSockets. Port 80 is assumed if no port is specified.
- `wss`: Secure WebSockets. Port 443 is assumed if no port is specified.

### Without TLS

```rust
let client = Client::build(Url::parse("ws://localhost")?)
    .finish()
    .await?;
```

### With TLS

```rust
let client = Client::build(Url::parse("wss://my-server.com")?)
    .finish()
    .await?;
```

## Using a `CustomApi`

```rust
// `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_local::core`.
use bonsaidb_core::custom_api::{CustomApi, Infallible};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Ping,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Response {
    Pong,
}

#[derive(Debug)]
pub enum MyApi {}

impl CustomApi for MyApi {
    type Request = Request;
    type Response = Response;
    type Error = Infallible;
}

let client = Client::build(Url::parse("bonsaidb://localhost")?)
    .with_custom_api::<MyApi>()
    .finish()
    .await?;
let Response::Pong = client.send_api_request(Request::Ping).await?;
```

### Receiving out-of-band messages from the server

If the server sends a message that isn't in response to a request, the
client will invoke it's [custom api
callback](Builder::with_custom_api_callback):

```rust
let client = Client::build(Url::parse("bonsaidb://localhost")?)
    .with_custom_api_callback::<MyApi,_>(|result: Result<Response, Infallible>| {
        let Response::Pong = result.unwrap();
    })
    .finish()
    .await?;
```

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
