![BonsaiDb is considered alpha](https://img.shields.io/badge/status-alpha-orange)
[![crate version](https://img.shields.io/crates/v/bonsaidb.svg)](https://crates.io/crates/bonsaidb)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/bonsaidb/Tests/$ref-name$)](https://github.com/khonsulabs/bonsaidb/actions?query=workflow:Tests)
[![HTML Coverage Report for `$ref-name$`]($pages-base$/coverage/badge.svg)]($pages-base$/coverage/)
[![Documentation for `$ref-name$`](https://img.shields.io/badge/docs-$ref-name$-informational)]($bonsaidb-docs$)

BonsaiDb is a developer-friendly document database for
[Rust](https://rust-lang.org) that grows with you. It offers many features out
of the box that many developers need:

- ACID-compliant, transactional storage of [Collections][collection]
- [Atomic Key-Value storage][key-value] with configurable delayed persistence (similar to Redis)
- At-rest Encryption
- Backup/Restore
- Role-Based Access Control (RBAC)
- Local-only access, networked access via QUIC, or networked access via WebSockets
- And [much more](https://bonsaidb.io/about).

[collection]: $pages-base$/guide/about/concepts/collection.html
[key-value]: $pages-base$/guide/traits/key-value.html

## ⚠️ Status of this project

BonsaiDb is considered alpha software. It is under active development (![GitHub
commit
activity](https://img.shields.io/github/commit-activity/m/khonsulabs/bonsaidb)).
There may still be bugs that result in data loss. All users should regularly
back up their data and test that restoring from backup works correctly.

## Example

To get an idea of how it works, let's review the [`view-examples` example][view-examples].
See the [examples README][examples-readme] for a list of all available examples.

The [`view-examples` example][view-examples] shows how to define a simple schema containing a single collection (`Shape`), a view to query the `Shape`s by their `number_of_sides` (`ShapesByNumberOfSides`), and demonstrates multiple ways to query that view.

First, here's how the schema is defined:

```rust,ignore
$../examples/basic-local/examples/view-examples.rs:snippet-a$
```

After you have your collection(s) and view(s) defined, you can open up a database and insert documents:

```rust,ignore
$../examples/basic-local/examples/view-examples.rs:snippet-b$
```

And query data using the Map-Reduce-powered view:

```rust,ignore
$../examples/basic-local/examples/view-examples.rs:snippet-c$
```

You can review the [full example in the repository][view-examples], or see all available examples [in the examples README][examples-readme].

[view-examples]: https://github.com/khonsulabs/bonsaidb/blob/$ref-name$/examples/basic-local/examples/view-examples.rs
[examples-readme]: https://github.com/khonsulabs/bonsaidb/blob/$ref-name$/examples/README.md

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
- `password-hashing`: Enables the ability to use password authentication using
  Argon2 via `AnyConnection`.

### Local databases only

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "local-full" }
```

- `local-full`: Enables all the flags below
- `local`: Enables the [`local`]($bonsaidb-docs$/local/) module, which re-exports the crate
  `bonsaidb-local`.
- `local-cli`: Enables the `StructOpt` structures for embedding database
  management commands into your own command-line interface.
- `local-encryption`: Enables at-rest encryption.
- `local-instrument`: Enables instrumenting with `tracing`.
- `local-multiuser`: Enables multi-user support.
- `local-password-hashing`: Enables the ability to use password authentication
  using Argon2.

### `BonsaiDb` server

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "server-full" }
```

- `server-full`: Enables all the flags below,
- `server`: Enables the [`server`]($bonsaidb-docs$/server/) module, which re-exports the crate
  `bonsaidb-server`.
- `server-acme`: Enables automtic certificate acquisition through ACME/LetsEncrypt.
- `server-cli`: Enables the `cli` module.
- `server-encryption`: Enables at-rest encryption.
- `server-hyper`: Enables convenience functions for upgrading websockets using `hyper`.
- `server-instrument`: Enables instrumenting with `tracing`.
- `server-pem`: Enables the ability to install a certificate using the PEM format.
- `server-websockets`: Enables `WebSocket` support.
- `server-password-hashing`: Enables the ability to use password authentication
  using Argon2.

### Client for accessing a `BonsaiDb` server

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "client-full" }
```

- `client-full`: Enables `client`, `client-trusted-dns` and `client-websockets`
- `client`: Enables the [`client`]($bonsaidb-docs$/client/) module, which re-exports the crate
  `bonsaidb-client`.
- `client-trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `client-websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `client-password-hashing`: Enables the ability to use password authentication
  using Argon2.
