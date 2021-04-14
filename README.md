# PliantDB

![PliantDB is considered experimental and unsupported](https://img.shields.io/badge/status-experimental-blueviolet)
[![crate version](https://img.shields.io/crates/v/pliantdb.svg)](https://crates.io/crates/pliantdb)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/pliantdb/Tests/main)](https://github.com/khonsulabs/pliantdb/actions?query=workflow:Tests)
[![codecov](https://codecov.io/gh/khonsulabs/pliantdb/branch/main/graph/badge.svg)](https://codecov.io/gh/khonsulabs/pliantdb)
[![Documentation for `main` branch](https://img.shields.io/badge/docs-main-informational)](https://khonsulabs.github.io/pliantdb/main/pliantdb/)
[![HTML Coverage Report for `main` branch](https://img.shields.io/badge/coverage-report-informational)](https://khonsulabs.github.io/pliantdb/coverage/)

PliantDB aims to be a [Rust](https://rust-lang.org)-written, ACID-compliant, document-database inspired by [CouchDB](https://couchdb.apache.org/). While it is inspired by CouchDB, this project will not aim to be compatible with existing CouchDB servers, and it will be implementing its own replication, clustering, and sharding strategies.

## Project Goals

The high-level goals for this project are:

- ☑️ Be able to build a document-based database's schema using Rust types.
- ☑️ Run within your Rust binary, simplifying basic deployments.
- ☑️ Run as a local-only file-based database with no networking involved.
- ☑️🚧 Run in a multi-database, networked server mode with TLS enabled by default
- Easily set up read-replicas between multiple servers.
- Easily run a highly-available quorum-based cluster across at least 3 servers
- Expose a Publish/Subscribe eventing system
- Expose a Job queue and scheduling system -- a la [Sidekiq](https://sidekiq.org/) or [SQS](https://aws.amazon.com/sqs/)

## ⚠️ Status of this project

**You should not attempt to use this software in anything except for experiments.** This project is under active development (![GitHub commit activity](https://img.shields.io/github/commit-activity/m/khonsulabs/pliantdb)), but at the point of writing this README, the project is too early to be used.

If you're interested in chatting about this project or potentially wanting to contribute, come chat with us on Discord: [![Discord](https://img.shields.io/discord/578968877866811403)](https://discord.khonsulabs.com/).

## Example

Check out [./pliantdb/examples](./pliantdb/examples) for examples. To get an idea of how it works, this is a simple schema:

```rust
// From pliantdb/examples/view-examples.rs
#[derive(Debug, Serialize, Deserialize)]
struct Shape {
    pub sides: u32,
}

impl Collection for Shape {
    fn id() -> collection::Id {
        collection::Id::from("shapes")
    }

    fn define_views(schema: &mut Schema) {
        schema.define_view(ShapesByNumberOfSides);
    }
}

#[derive(Debug)]
struct ShapesByNumberOfSides;

impl View for ShapesByNumberOfSides {
    type Collection = Shape;

    type Key = u32;

    type Value = ();

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Cow<'static, str> {
        Cow::from("by-sides")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key> {
        let shape = document.contents::<Shape>()?;
        Ok(Some(document.emit_key(shape.sides as u32)))
    }
}
```

After you have your collection(s) defined, you can open up a database and query it:

```rust
let db =
    Storage::<Shape>::open_local("view-examples.pliantdb", &Configuration::default()).await?;

// Insert a new document into the Shape collection.
db.collection::<Shape>()?.push(&Shape::new(3)).await?;

// Query the Number of Sides View to see how many documents have 3 sides.
println!(
    "Number of triangles: {}",
    db.view::<ShapesByNumberOfSides>()
        .with_key(3)
        .query()
        .await?
        .len()
);
```

## Why write another database?

- Deploying highly-available databases is hard (and often expensive). It doesn't need to be.
- We are passionate Rustaceans and are striving for an ideal of supporting a 100% Rust-based deployment ecosystem for newly written software.
- Specifically for the founding author [@ecton](https://github.com/ecton), the idea for this design dates back to thoughts of fun side-projects while running my last business which was built atop CouchDB. Working on this project is fulfilling a long-time desire of his.

## Backups

### Exporting and restoring databases with direct access

If you have a local `PliantDB` database, you can use the `local-backup` command to save and load backups:

```sh
pliantdb local-backup <database-path> save
```

```sh
pliantdb local-backup <destination-database-path> load <backup-path>
```

The format of this export should be easy to work with if you're either transitioning from PliantDB to another solution or needing to do complicated disaster recovery work. It is [described here](https://khonsulabs.github.io/pliantdb/main/pliantdb/local/backup/enum.Command.html#variant.Save).

## Developing PliantDB

### Pre-commit hook

Our CI processes require that some commands succeed without warnings or errors. To ensure that code you submit passes the basic checks, install the included [pre-commit](./git-pre-commit-hook.sh) hook:

```bash
./git-pre-commit-hook.sh install
```

Once done, tools including `cargo fmt`, `cargo doc`, and `cargo test` will all be checked before `git commit` will execute.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are open-source. This repository is available under the [MIT License](./LICENSE-MIT) or the [Apache License 2.0](./LICENSE-APACHE).
