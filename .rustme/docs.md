BonsaiDb is a developer-friendly document database for [Rust](https://rust-lang.org) that grows with you. Visit [BonsaiDb.io](https://bonsaidb.io/about) to learn more about the features of BonsaiDb.

## ⚠️ Status of this project

**You should not attempt to use this software in anything except for experiments.** This project is under active development (![GitHub commit activity](https://img.shields.io/github/commit-activity/m/khonsulabs/bonsaidb)), but at the point of writing this README, the project is too early to be used.

If you're interested in chatting about this project or potentially wanting to contribute, come chat with us on Discord: [![Discord](https://img.shields.io/discord/578968877866811403)](https://discord.khonsulabs.com/).

## Example

To get an idea of how it works, this is a simple schema:

```rust,ignore
$../examples/basic-local/examples/view-examples.rs:snippet-a$
```

After you have your collection(s) defined, you can open up a database and insert documents:

```rust,ignore
$../examples/basic-local/examples/view-examples.rs:snippet-b$
```

And query data using the Map-Reduce-powered view:

```rust,ignore
$../examples/basic-local/examples/view-examples.rs:snippet-c$
```

See the [examples README](https://github.com/khonsulabs/bonsaidb/blob/main/examples/README.md) for a list of all available examples.

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
