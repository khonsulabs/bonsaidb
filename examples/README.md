# BonsaiDb Examples

This folder contains crates that are full of examples that demonstrate various usages of BonsaiDb. The examples are grouped into crates by two things:

- The feature flags they enable in the `bonsaidb` crate
- Shared list of dependencies

This approach to organizing examples will keep individual example build times as low as possible, as only the dependencies and features needed for each example will be built.

## Single offline database

Source code: [`basic-local/examples/basic-local.rs`](./basic-local/examples/basic-local.rs)

```sh
cargo run --example basic-local
```

## Offline storage with multiple databases

Source code: [`basic-local/examples/basic-local-multidb.rs`](./basic-local/examples/basic-local-multidb.rs)

```sh
cargo run --example basic-local-multidb
```

## Introduction to Views

Source code: [`basic-local/examples/view-examples.rs`](./basic-local/examples/view-examples.rs)

```sh
cargo run --example view-examples
```

## Using a View for Keyword Search

Source code: [`basic-local/examples/keyword-search.rs`](./basic-local/examples/keyword-search.rs)

```sh
cargo run --example keyword-search
```

## Using Views with `hdrhistogram`

Source code: [`view-histogram/examples/view-histogram.rs`](./view-histogram/examples/view-histogram.rs)

```sh
cargo run --example view-histogram
```

## PubSub: Publish messages to subscribable topics

Source code: [`basic-local/examples/pubsub.rs`](./basic-local/examples/pubsub.rs)

```sh
cargo run --example pubsub
```

## Key-Value Storage

Source code: [`basic-local/examples/key-value-store.rs`](./basic-local/examples/key-value-store.rs)

```sh
cargo run --example key-value-store
```

## Basic BonsaiDb Server + Network Access

Source code: [`basic-server/examples/basic-server.rs`](./basic-server/examples/basic-server.rs)

```sh
cargo run --example basic-server
```

## Users and Permissions

Source code: [`basic-server/examples/users.rs`](./basic-server/examples/users.rs)

```sh
cargo run --example users
```

## Building a command-line interface

Source code: [`basic-server/examples/cli.rs`](./basic-server/examples/cli.rs)

```sh
cargo run --example cli
```

## Automatically requesting an ACME certificate

Source code: [`acme/examples/acme.rs`](./acme/examples/acme.rs)

```sh
cargo run --example acme
```

## Running a webserver with BonsaiDb websockets

Source code: [`axum/examples/axum.rs`](./axum/examples/axum.rs)

```sh
cargo run --example axum
```
