# BonsaiDb Examples

## Single offline database

Source code: [`basic-local.rs`](./basic-local.rs)

```sh
cargo run --example basic-local --features local
```

## Offline storage with multiple databases

Source code: [`basic-local-multidb.rs`](./basic-local-multidb.rs)

```sh
cargo run --example basic-local-multidb --features local
```

## Introduction to Views

Source code: [`view-examples.rs`](./view-examples.rs)

```sh
cargo run --example view-examples --features local
```

## Using Views with `hdrhistogram`

Source code: [`view-histogram.rs`](./view-histogram.rs)

```sh
cargo run --example view-histogram --features local
```

## PubSub: Publish messages to subscribable topics

Source code: [`pubsub.rs`](./pubsub.rs)

```sh
cargo run --example pubsub --features local
```

## Key-Value Storage

Source code: [`key-value-store.rs`](./key-value-store.rs)

```sh
cargo run --example key-value-store --features local
```

## Basic BonsaiDb Server + Network Access

Source code: [`server.rs`](./server.rs)

```sh
cargo run --example server --features server,client
```

## Users and Permissions

Source code: [`users.rs`](./users.rs)

```sh
cargo run --example users --features server,client
```

## Automatically requesting an ACME certificate

Source code: [`acme.rs`](./acme.rs)

```sh
cargo run --example acme --features server,server-acme,client
```

## Running a webserver with BonsaiDb websockets

Source code: [`axum.rs`](./axum.rs)

```sh
cargo run --example axum --features server,server-hyper,websockets
```