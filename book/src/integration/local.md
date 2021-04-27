# Integrating PliantDb Locally

`PliantDb` supports multiple [databases](../about/concepts/database.md) and multiple [schemas](../about/concepts/schema.md). However, for many applications, you only need a single database.

If you're only wanting a single database, the setup is straightforward: (from [`pliantdb/examples/basic-local.rs`](https://github.com/khonsulabs/pliantdb/blob/main/pliantdb/examples/basic-local.rs))

```rust,noplayground,no_run
let db = Database::<Message>::open_local(
    "basic.pliantdb", 
    &Configuration::default()
).await?;
```

Under the hood, `PliantDb` is creating a multi-database [`Storage`](https://pliantdb.dev/main/pliantdb/local/struct.Storage.html) with a local [`Database`](https://pliantdb.dev/main/pliantdb/local/struct.Database.html) named `default` for you. If you need to switch to a multi-database model, you can open the storage and access the `default` database: (adapted from [`pliantdb/examples/basic-local.rs`](https://github.com/khonsulabs/pliantdb/blob/main/pliantdb/examples/basic-local-multidb.rs))

```rust,noplayground,no_run
let storage = Storage::open_local(
    "basic.pliantdb",
    &Configuration::default()
).await?;
storage.register_schema::<Message>().await?;
let db = storage.database::<Message>("default").await?;
```

You can register multiple schemas so that databases can be purpose-built.

## Common Traits

To help your code transition between different modes of accessing `PliantDb`, you can use these common traits to make your methods accept any style of `PliantDb` access.

* [`Database`](https://pliantdb.dev/main/pliantdb/local/struct.Database.html) implements [`Connection`](../traits/connection.md), [`Kv`](../traits/kv.md), and [`PubSub`](../traits/kv.md).
* [`Storage`](https://pliantdb.dev/main/pliantdb/local/struct.Storage.html) implements [`ServerConnection`](../traits/server_connection.md).

For example, [`pliantdb/examples/basic-local.rs`](https://github.com/khonsulabs/pliantdb/blob/main/pliantdb/examples/basic-local-multidb.rs) uses this helper method to insert a record:

```rust,noplayground,no_run
{{#include ../../../pliantdb/examples/basic-local-multidb.rs:reusable-code}}
```
