# BonsaiDb

![BonsaiDb forbids unsafe code](https://img.shields.io/badge/unsafe-forbid-success)
![BonsaiDb is considered alpha](https://img.shields.io/badge/status-alpha-orange)
[![crate version](https://img.shields.io/crates/v/bonsaidb.svg)](https://crates.io/crates/bonsaidb)
[![Live Build Status](https://img.shields.io/github/actions/workflow/status/khonsulabs/bonsaidb/tests.yml?branch=main)](https://github.com/khonsulabs/bonsaidb/actions?query=workflow:Tests)
[![HTML Coverage Report for `main`](https://dev.bonsaidb.io/main/coverage/badge.svg)](https://dev.bonsaidb.io/main/coverage/)
[![Documentation for `main`](https://img.shields.io/badge/docs-main-informational)](https://dev.bonsaidb.io/main/docs/bonsaidb)

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

[collection]: https://dev.bonsaidb.io/main/guide/about/concepts/collection.html
[key-value]: https://dev.bonsaidb.io/main/guide/traits/key-value.html

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
#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "shapes", views = [ShapesByNumberOfSides])]
struct Shape {
    pub sides: u32,
}

#[derive(Debug, Clone, View)]
#[view(collection = Shape, key = u32, value = usize, name = "by-number-of-sides")]
struct ShapesByNumberOfSides;

impl CollectionViewSchema for ShapesByNumberOfSides {
    type View = Self;

    fn map(&self, document: CollectionDocument<Shape>) -> ViewMapResult<Self::View> {
        document
            .header
            .emit_key_and_value(document.contents.sides, 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
```

After you have your collection(s) and view(s) defined, you can open up a database and insert documents:

```rust,ignore
let db = Database::open::<Shape>(StorageConfiguration::new("view-examples.bonsaidb"))?;

// Insert a new document into the Shape collection.
Shape { sides: 3 }.push_into(&db)?;
```

And query data using the Map-Reduce-powered view:

```rust,ignore
let triangles = ShapesByNumberOfSides::entries(&db).with_key(&3).query()?;
println!("Number of triangles: {}", triangles.len());
```

You can review the [full example in the repository][view-examples], or see all available examples [in the examples README][examples-readme].

[view-examples]: https://github.com/khonsulabs/bonsaidb/blob/main/examples/basic-local/examples/view-examples.rs
[examples-readme]: https://github.com/khonsulabs/bonsaidb/blob/main/examples/README.md

## User's Guide

Our user's guide is early in development, but is available at: <https://dev.bonsaidb.io/main/guide/>

## Minimum Supported Rust Version (MSRV)

While this project is alpha, we are actively adopting the current version of
Rust. The current minimum version is `1.60`.

## Feature Flags

No feature flags are enabled by default in the `bonsaidb` crate. This is
because in most Rust executables, you will only need a subset of the
functionality. If you'd prefer to enable everything, you can use the `full`
feature:

```toml
[dependencies]
bonsaidb = { version = "*", features = "full" }
```

- `full`: Enables the features below and `local-full`, `server-full`, and `client-full`.
- `cli`: Enables the `bonsaidb` executable.
- `files`: Enables file storage support with `bonsaidb-files`
- `password-hashing`: Enables the ability to use password authentication using
  Argon2 via `AnyConnection`.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.

All other feature flags, listed below, affect each crate individually, but can
be safely combined.

### Local databases only

```toml
[dependencies]
bonsaidb = { version = "*", features = "local-full" }
```

All Cargo features that affect local databases:

- `local-full`: Enables all the flags below
- `local`: Enables the [`local`](https://dev.bonsaidb.io/main/docs/bonsaidb/local/) module, which re-exports the crate
  `bonsaidb-local`.
- `async`: Enables async support with Tokio.
- `cli`: Enables the `clap` structures for embedding database
  management commands into your own command-line interface.
- `compression`: Enables support for compressed storage using lz4.
- `encryption`: Enables at-rest encryption.
- `instrument`: Enables instrumenting with `tracing`.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.

### BonsaiDb server

```toml
[dependencies]
bonsaidb = { version = "*", features = "server-full" }
```

All Cargo features that affect networked servers:

- `server-full`: Enables all the flags below,
- `server`: Enables the [`server`](https://dev.bonsaidb.io/main/docs/bonsaidb/server/) module, which re-exports the crate
  `bonsaidb-server`.
- `acme`: Enables automtic certificate acquisition through ACME/LetsEncrypt.
- `cli`: Enables the `cli` module.
- `compression`: Enables support for compressed storage using lz4.
- `encryption`: Enables at-rest encryption.
- `hyper`: Enables convenience functions for upgrading websockets using `hyper`.
- `instrument`: Enables instrumenting with `tracing`.
- `pem`: Enables the ability to install a certificate using the PEM format.
- `websockets`: Enables `WebSocket` support.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.

### Client for accessing a BonsaiDb server

```toml
[dependencies]
bonsaidb = { version = "*", features = "client-full" }
```

All Cargo features that affect networked clients:

- `client-full`: Enables all flags below.
- `client`: Enables the [`client`](https://dev.bonsaidb.io/main/docs/bonsaidb/client/) module, which re-exports the crate
  `bonsaidb-client`.
- `trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.

## Developing BonsaiDb

### Writing Unit Tests

Unless there is a good reason not to, every feature in BonsaiDb should have
thorough unit tests. Many tests are implemented in `bonsaidb_core::test_util`
via a macro that allows the suite to run using various methods of accessing
BonsaiDb.

Some features aren't able to be tested using the `Connection`,
`StorageConnection`, `KeyValue`, and `PubSub` traits only. If that's the case,
you should add tests to whichever crates makes the most sense to test the code.
For example, if it's a feature that only can be used in `bonsaidb-server`, the
test should be somewhere in the `bonsaidb-server` crate.

Tests that require both a client and server can be added to the `core-suite`
test file in the `bonsaidb` crate.

### Checking Syntax

We use `clippy` to give additional guidance on our code. Clippy should always return with no errors, regardless of feature flags being enabled:

```bash
cargo clippy --all-features
```

### Running all tests

Our CI processes require that some commands succeed without warnings or errors. These checks can be performed manually by running:

```bash
cargo xtask test --fail-on-warnings
```

Or, if you would like to run all these checks before each commit, you can install the check as a pre-commit hook:

```bash
cargo xtask install-pre-commit-hook
```

### Formatting Code

We have a custom rustfmt configuration that enables several options only available in nightly builds:

```bash
cargo +nightly fmt
```

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
