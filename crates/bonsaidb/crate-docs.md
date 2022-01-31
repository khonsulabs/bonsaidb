BonsaiDb is a developer-friendly document database for [Rust](https://rust-lang.org) that grows with you. Visit [BonsaiDb.io](https://bonsaidb.io/about) to learn more about the features of BonsaiDb.

![BonsaiDb is considered alpha](https://img.shields.io/badge/status-alpha-orange)
[![crate version](https://img.shields.io/crates/v/bonsaidb.svg)](https://crates.io/crates/bonsaidb)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/bonsaidb/Tests/main)](https://github.com/khonsulabs/bonsaidb/actions?query=workflow:Tests)
[![HTML Coverage Report for `main` branch](https://dev.bonsaidb.io/main/coverage/badge.svg)](https://dev.bonsaidb.io/main/coverage/)
[![Documentation for `main` branch](https://img.shields.io/badge/docs-main-informational)](https://dev.bonsaidb.io/main)

## ⚠️ Status of this project

BonsaiDb is considered alpha software. It is under active development (![GitHub commit activity](https://img.shields.io/github/commit-activity/m/khonsulabs/bonsaidb)). There may still be bugs that result in data loss. All users should regularly back up their data and test that restoring from backup works correctly.

## Example

To get an idea of how it works, this is a simple schema:

```rust,ignore
#[derive(Debug, Serialize, Deserialize)]
struct Shape {
    pub sides: u32,
}

impl Collection for Shape {
    fn collection_name() -> CollectionName {
        CollectionName::new("khonsulabs", "shapes")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ShapesByNumberOfSides)
    }
}

impl DefaultSerialization for Shape {}

#[derive(Debug, Clone)]
struct ShapesByNumberOfSides;

impl View for ShapesByNumberOfSides {
    type Collection = Shape;
    type Key = u32;
    type Value = usize;

    fn name(&self) -> Name {
        Name::new("by-number-of-sides")
    }
}

impl CollectionViewSchema for ShapesByNumberOfSides {
    type View = Self;

    fn map(&self, document: CollectionDocument<Shape>) -> ViewMapResult<Self::View> {
        Ok(document.emit_key_and_value(document.contents.sides, 1))
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}

impl DefaultViewSerialization for ShapesByNumberOfSides {}
```

After you have your collection(s) defined, you can open up a database and insert documents:

```rust,ignore
let db = Database::open::<Shape>(StorageConfiguration::new("view-examples.bonsaidb")).await?;

// Insert a new document into the Shape collection.
Shape::new(3).push_into(&db).await?;
```

And query data using the Map-Reduce-powered view:

```rust,ignore
let triangles = db
    .view::<ShapesByNumberOfSides>()
    .with_key(3)
    .query()
    .await?;
println!("Number of triangles: {}", triangles.len());
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
- `password-hashing`: Enables the ability to use password authentication using
  Argon2 via `AnyConnection`.

### Local databases only

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "local-full" }
```

- `local-full`: Enables all the flags below
- `local`: Enables the [`local`](https://dev.bonsaidb.io/main/local/) module, which re-exports the crate
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
- `server`: Enables the [`server`](https://dev.bonsaidb.io/main/server/) module, which re-exports the crate
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
- `client`: Enables the [`client`](https://dev.bonsaidb.io/main/client/) module, which re-exports the crate
  `bonsaidb-client`.
- `client-trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `client-websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `client-password-hashing`: Enables the ability to use password authentication
  using Argon2.

## Developing BonsaiDb

### Pre-commit hook

Our CI processes require that some commands succeed without warnings or errors. These checks can be performed manually by running:

```bash
cargo xtask test --fail-on-warnings
```

Or, if you would like to run all these checks before each commit, you can install the check as a pre-commit hook:

```bash
cargo xtask install-pre-commit-hook
```

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are
open-source. This repository is available under the [MIT License](./LICENSE-MIT)
or the [Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
