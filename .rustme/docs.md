BonsaiDb aims to be a [Rust](https://rust-lang.org)-written, ACID-compliant, document-database inspired by [CouchDB](https://couchdb.apache.org/). While it is inspired by CouchDB, this project will not aim to be compatible with existing CouchDB servers, and it will be implementing its own replication, clustering, and sharding strategies.

## Project Goals

The high-level goals for this project are:

- ☑️ Be able to build a document-based database's schema using Rust types.
- ☑️ Run within your Rust binary, simplifying basic deployments.
- ☑️ Run as a local-only file-based database with no networking involved.
- ☑️ Run as a networked server using QUIC with TLS enabled by default.
- ☑️ Expose a Publish/Subscribe eventing system.
- ☐ Expose a Job queue and scheduling system -- a la [Sidekiq](https://sidekiq.org/) or [SQS](https://aws.amazon.com/sqs/) (tracking issue [#78](https://github.com/khonsulabs/bonsaidb/issues/78))
- ☐ Easily set up read-replicas between multiple servers (tracking issue [#90](https://github.com/khonsulabs/bonsaidb/issues/90))
- ☐ Easily run a highly-available quorum-based cluster across at least 3 servers (tracking issue [#104](https://github.com/khonsulabs/bonsaidb/issues/104))

## ⚠️ Status of this project

**You should not attempt to use this software in anything except for experiments.** This project is under active development (![GitHub commit activity](https://img.shields.io/github/commit-activity/m/khonsulabs/bonsaidb)), but at the point of writing this README, the project is too early to be used.

If you're interested in chatting about this project or potentially wanting to contribute, come chat with us on Discord: [![Discord](https://img.shields.io/discord/578968877866811403)](https://discord.khonsulabs.com/).

## Example

Check out [./examples](https://github.com/khonsulabs/bonsaidb/tree/main/examples) for examples. To get an idea of how it works, this is a simple schema:

```rust
$../examples/basic-local/examples/view-examples.rs:snippet-a$
```

After you have your collection(s) defined, you can open up a database and insert documents:

```rust
$../examples/basic-local/examples/view-examples.rs:snippet-b$
```

And query data using the Map-Reduce-powered view:

```rust
$../examples/basic-local/examples/view-examples.rs:snippet-c$
```

See the [examples README](https://github.com/khonsulabs/bonsaidb/blob/main/examples/README.md) for a list of all available examples.

## Why write another database?

- Deploying highly-available databases is hard (and often expensive). It doesn't need to be.
- We are passionate Rustaceans and are striving for an ideal of supporting a 100% Rust-based deployment ecosystem for newly written software.
- Specifically for the founding author [@ecton](https://github.com/ecton), the idea for this design dates back to thoughts of fun side-projects while running my last business which was built atop CouchDB. Working on this project is fulfilling a long-time desire of his.

## Feature Flags

No feature flags are enabled by default in the `bonsaidb` crate. This is
because in most Rust executables, you will only need a subset of the
functionality. If you'd prefer to enable everything, you can use the `full`
feature:

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "full" }
```

- `full`: Enables `local-full`, `server-full`, and `client-full`.
- `cli`: Enables the `bonsaidb` executable.

### Local databases only

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "local-full" }
```

- `local-full`: Enables all the flags below
- `local`: Enables the [`local`](https://dev.bonsaidb.io/main/bonsaidb/local/) module, which re-exports the crate
  `bonsaidb-local`.
- `local-cli`: Enables the `StructOpt` structures for embedding database
  management commands into your own command-line interface.
- `local-encryption`: Enables at-rest encryption.
- `local-instrument`: Enables instrumenting with `tracing`.
- `local-multiuser`: Enables multi-user support.

### `BonsaiDb` server

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "server-full" }
```

- `server-full`: Enables all the flags below,
- `server`: Enables the [`server`](https://dev.bonsaidb.io/main/bonsaidb/server/) module, which re-exports the crate
  `bonsaidb-server`.
- `server-acme`: Enables automtic certificate acquisition through ACME/LetsEncrypt.
- `server-cli`: Enables the `cli` module.
- `server-encryption`: Enables at-rest encryption.
- `server-hyper`: Enables convenience functions for upgrading websockets using `hyper`.
- `server-instrument`: Enables instrumenting with `tracing`.
- `server-pem`: Enables the ability to install a certificate using the PEM format.
- `server-websockets`: Enables `WebSocket` support.

### Client for accessing a `BonsaiDb` server

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "client-full" }
```

- `client-full`: Enables `client`, `client-trusted-dns` and `client-websockets`
- `client`: Enables the [`client`](https://dev.bonsaidb.io/main/bonsaidb/client/) module, which re-exports the crate
  `bonsaidb-client`.
- `client-trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `client-websockets`: Enables `WebSocket` support for `bonsaidb-client`.
